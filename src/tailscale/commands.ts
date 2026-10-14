import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import type { MeshClient } from "../client/client.js";
import { DEFAULT_TAILSCALE_PORT, isMeshId, type MeshConfig, type TailscaleConfig } from "../shared/config.js";
import { configPath } from "../shared/paths.js";
import { clearTailnetDiscoveryCache, listTailnetServices, readTailnetStatus, resolveConnection } from "./adapter.js";
import { diagnoseConnection } from "./diagnostics.js";

export interface TailnetSelection {
  meshId: string;
  service?: string;
  host?: string;
  tag?: string;
  port?: number;
}

export interface TailnetCommand {
  action: "status" | "discover" | "join" | "doctor";
  selection?: TailnetSelection;
}

export interface TailnetCommandResult {
  ok: boolean;
  text: string;
  details: Record<string, unknown>;
  /** Internal result for a live extension; never serialized to CLI stdout. */
  config?: MeshConfig;
}

function selectionConfig(config: MeshConfig, selection: TailnetSelection, env: NodeJS.ProcessEnv): MeshConfig {
  if (config.brokerUrl !== undefined) {
    throw new Error("tailscale_selection_overridden: explicit MESH_BROKER_URL/brokerUrl takes precedence; remove it before selecting a tailnet mesh");
  }
  if (!isMeshId(selection.meshId)) throw new Error("tailscale_config_invalid: select a valid mesh identifier");
  const mode = selection.host !== undefined || selection.tag !== undefined ? "node" : "service";
  if (mode === "node" && selection.service !== undefined) {
    throw new Error("tailscale_config_invalid: choose a Service or a node selector, not both");
  }
  const port = selection.port ?? config.tailscale?.port ?? DEFAULT_TAILSCALE_PORT;
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error("tailscale_config_invalid: port must be an integer between 1 and 65535");
  }
  const tailscale: TailscaleConfig = { mode, port, execPath: config.tailscale?.execPath };
  if (selection.service !== undefined) {
    if (!/^svc:[a-z0-9][a-z0-9-]*$/.test(selection.service)) throw new Error("tailscale_config_invalid: service must be svc:<service-name>");
    tailscale.service = selection.service;
  }
  if (selection.host !== undefined) tailscale.host = selection.host;
  if (selection.tag !== undefined) {
    if (!/^tag:[a-z0-9][a-z0-9-]*$/.test(selection.tag)) throw new Error("tailscale_config_invalid: tag must be tag:<tag-name>");
    tailscale.tag = selection.tag;
  }
  const expected: Record<string, string | undefined> = {
    MESH_ID: selection.meshId,
    MESH_TAILSCALE_MODE: tailscale.mode,
    MESH_TAILSCALE_SERVICE: tailscale.service,
    MESH_TAILSCALE_HOST: tailscale.host,
    MESH_TAILSCALE_TAG: tailscale.tag,
    MESH_TAILSCALE_PORT: String(tailscale.port),
  };
  for (const [key, value] of Object.entries(expected)) {
    if (env[key] !== undefined && env[key] !== value) {
      throw new Error(`tailscale_selection_overridden: ${key} overrides the requested selection; unset it first`);
    }
  }
  return { ...config, meshId: selection.meshId, tailscale };
}

/** Preserve unrelated configuration and secrets; publish only a complete replacement. */
async function persistSelection(stateDirectory: string, config: MeshConfig, isCurrent: () => boolean): Promise<void> {
  const target = configPath(stateDirectory);
  let current: Record<string, unknown> = {};
  try {
    const value: unknown = JSON.parse(await fs.readFile(target, "utf8"));
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw new Error("tailscale_config_invalid: existing config.json must be an object");
    }
    current = value as Record<string, unknown>;
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
  }
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.${randomUUID()}.tmp`;
  try {
    if (!isCurrent()) throw new Error("selection_cancelled: the requesting session or selection is no longer current");
    await fs.writeFile(temporary, JSON.stringify({ ...current, meshId: config.meshId, tailscale: config.tailscale }, null, 2) + "\n", { flag: "wx", mode: 0o600 });
    if (!isCurrent()) throw new Error("selection_cancelled: the requesting session or selection is no longer current");
    await fs.rename(temporary, target);
  } finally {
    await fs.rm(temporary, { force: true });
  }
}

export async function executeTailnetCommand(
  command: TailnetCommand,
  config: MeshConfig,
  stateDirectory: string,
  runtimeDirectory: string,
  client?: MeshClient,
  env: NodeJS.ProcessEnv = process.env,
  isCurrent: () => boolean = () => true,
): Promise<TailnetCommandResult> {
  const details: Record<string, unknown> = { schema: "mesh.tailnet.v1", action: command.action };
  try {
    if (command.action === "doctor") {
      const diagnostic = await diagnoseConnection(config, runtimeDirectory);
      details.diagnostic = diagnostic;
      const lines = [
        `source: ${diagnostic.target?.source ?? "unresolved"}`,
        `endpoint: ${diagnostic.target?.url ?? (diagnostic.resolved ? "local socket/pipe" : "unresolved")}`,
        `discovery: ${diagnostic.resolved ? "resolved" : "failed"}`,
        `tcp/socket: ${diagnostic.reachable ? "reachable" : "unreachable"}`,
        `mesh: ${diagnostic.authenticated ? "authenticated" : "not authenticated"}`,
      ];
      if (diagnostic.broker !== undefined) {
        lines.push(`meshId: ${diagnostic.broker.meshId}`, `brokerId: ${diagnostic.broker.brokerId}`, `bootId: ${diagnostic.broker.bootId}`);
      }
      if (diagnostic.authenticated) lines.push(`agents: ${diagnostic.peers.map((peer) => peer.alias).join(", ") || "none"}`);
      if (diagnostic.error !== undefined) lines.push(`error: ${diagnostic.error}`);
      return { ok: diagnostic.authenticated, text: lines.join("\n"), details };
    }
    clearTailnetDiscoveryCache();
    if (command.action === "join") {
      if (command.selection === undefined) throw new Error("tailscale_config_invalid: join requires a mesh identifier");
      if (!isCurrent()) throw new Error("selection_cancelled: the requesting session or selection is no longer current");
      const selected = selectionConfig(config, command.selection, env);
      const diagnostic = await diagnoseConnection(selected, runtimeDirectory);
      details.diagnostic = diagnostic;
      if (!diagnostic.authenticated) {
        return { ok: false, text: `mesh selection not saved: ${diagnostic.error ?? "broker not authenticated"}`, details };
      }
      if (!isCurrent()) throw new Error("selection_cancelled: the requesting session or selection is no longer current");
      await persistSelection(stateDirectory, selected, isCurrent);
      details.selected = { meshId: selected.meshId, ...selected.tailscale, execPath: undefined };
      details.saved = true;
      return { ok: true, text: `mesh ${selected.meshId} selected at ${diagnostic.target?.url}; configuration saved for subsequent sessions`, details, config: selected };
    }
    const status = await readTailnetStatus(config.tailscale);
    details.tailnet = status;
    details.mode = config.tailscale?.mode ?? "off";
    details.meshId = config.meshId;
    const lines = [
      `tailnet: ${status.backendState}`,
      `MagicDNS: ${status.magicDnsEnabled ? "enabled" : "disabled"}`,
      `discovery mode: ${config.tailscale?.mode ?? "off"}`,
      `visible devices: ${status.peers.length + (status.self === null ? 0 : 1)} (device visibility is not broker readiness)`,
    ];
    if (status.backendState !== "Running") {
      details.error = "tailscale_down";
      lines.push("run tailscale up before connecting");
      return { ok: false, text: lines.join("\n"), details };
    }
    if (command.action === "discover" && config.tailscale?.mode !== "node") {
      const services = (await listTailnetServices(config.tailscale)).filter((service) =>
        service.name.startsWith("svc:pi-mesh-") || service.name === config.tailscale?.service,
      );
      details.services = services;
      lines.push(`mesh Services: ${services.length} (advertisements; not authenticated brokers)`);
      for (const service of services) lines.push(`${service.name} ${service.hostname} TCP=${service.ports.join(",")}`);
    }
    const target = client?.connectionTarget ?? await resolveConnection(config);
    details.target = target;
    details.connection = { online: client?.isOnline() ?? false, broker: client?.brokerInfo, error: client?.connectionError };
    lines.push(`selected source: ${target.source}`, `selected endpoint: ${target.url ?? "local socket/pipe"}`);
    if (target.node !== undefined) lines.push(`selected node: ${target.node.hostname}; device online=${target.node.online}`);
    lines.push(`mesh: ${client?.isOnline() === true ? "authenticated live connection" : "not verified by this command; use tailscale doctor"}`);
    return { ok: true, text: lines.join("\n"), details };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    details.error = message;
    return { ok: false, text: message, details };
  }
}
