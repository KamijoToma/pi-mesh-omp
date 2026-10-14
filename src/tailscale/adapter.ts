import childProcess from "node:child_process";
import { isIP } from "node:net";
import {
  parseEndpoint,
  TAILSCALE_COMMAND_TIMEOUT_MS,
  TAILSCALE_DISCOVERY_CACHE_MS,
  TAILSCALE_MAX_OUTPUT_BYTES,
  TAILSCALE_MAX_SERVICE_PORTS,
  type MeshConfig,
  type MeshEndpoint,
  type TailscaleConfig,
} from "../shared/config.js";

export interface TailnetNode {
  id: string;
  hostname: string;
  dnsName: string;
  addresses: string[];
  online: boolean;
  tags: string[];
}

export interface TailnetStatus {
  backendState: string;
  self: TailnetNode | null;
  peers: TailnetNode[];
  magicDnsEnabled: boolean;
  dnsSuffix: string;
}

export interface TailnetService {
  name: string;
  hostname: string;
  addresses: string[];
  ports: number[];
}

export interface ResolvedConnection {
  endpoint: MeshEndpoint | null;
  url: string | null;
  source: "local" | "explicit" | "tailscale-service" | "tailscale-node";
  meshId?: string;
  node?: TailnetNode;
  service?: TailnetService;
}

interface CacheEntry<T> {
  promise: Promise<T>;
  expiresAt: number;
  pending: boolean;
}

const statusCache = new Map<string, CacheEntry<TailnetStatus>>();
const serviceCache = new Map<string, CacheEntry<TailnetService[]>>();
const MACOS_CLI = "/Applications/Tailscale.app/Contents/MacOS/Tailscale";

function cacheKey(config?: TailscaleConfig): string {
  return JSON.stringify([config?.execPath, process.platform, process.env.PATH]);
}

function cached<T>(
  cache: Map<string, CacheEntry<T>>,
  config: TailscaleConfig | undefined,
  refresh: boolean,
  load: () => Promise<T>,
): Promise<T> {
  const key = cacheKey(config);
  const previous = cache.get(key);
  if (previous !== undefined && (previous.pending || (!refresh && previous.expiresAt > Date.now()))) {
    return previous.promise;
  }
  const entry: CacheEntry<T> = { promise: load(), expiresAt: 0, pending: true };
  cache.set(key, entry);
  entry.promise = entry.promise.then(
    (value) => {
      entry.pending = false;
      entry.expiresAt = Date.now() + TAILSCALE_DISCOVERY_CACHE_MS;
      return value;
    },
    (error: unknown) => {
      if (cache.get(key) === entry) cache.delete(key);
      throw error;
    },
  );
  return entry.promise;
}

export function clearTailnetDiscoveryCache(): void {
  statusCache.clear();
  serviceCache.clear();
}

function runCommand(executable: string, args: readonly string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = childProcess.spawn(executable, [...args], {
      stdio: ["ignore", "pipe", "pipe"],
      windowsHide: true,
      env: process.platform === "darwin" ? { ...process.env, TAILSCALE_BE_CLI: "1" } : process.env,
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let bytes = 0;
    let settled = false;
    const fail = (error: Error): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.kill("SIGKILL");
      reject(error);
    };
    const timer = setTimeout(() => {
      fail(new Error(`tailscale_timeout: ${executable} ${args.join(" ")} exceeded ${TAILSCALE_COMMAND_TIMEOUT_MS}ms; check the Tailscale daemon`));
    }, TAILSCALE_COMMAND_TIMEOUT_MS);
    const capture = (chunks: Buffer[], chunk: Buffer): void => {
      if (settled) return;
      bytes += chunk.length;
      if (bytes > TAILSCALE_MAX_OUTPUT_BYTES) {
        fail(new Error(`tailscale_output_limit: CLI output exceeded ${TAILSCALE_MAX_OUTPUT_BYTES} bytes`));
        return;
      }
      chunks.push(chunk);
    };
    child.stdout.on("data", (chunk: Buffer) => capture(stdout, chunk));
    child.stderr.on("data", (chunk: Buffer) => capture(stderr, chunk));
    child.on("error", (error: NodeJS.ErrnoException) => {
      fail(error.code === "ENOENT"
        ? Object.assign(new Error(`tailscale_command_missing: ${executable} not found; install Tailscale or set MESH_TAILSCALE_EXEC_PATH`), { code: "ENOENT" })
        : new Error(`tailscale_command_failed: cannot run ${executable}: ${error.message}`));
    });
    child.on("close", (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (code !== 0) {
        const detail = Buffer.concat(stderr).toString("utf8").trim();
        const daemon = /tailscaled|daemon|failed to connect|connection refused|socket/i.test(detail);
        reject(new Error(`${daemon ? "tailscale_daemon_unavailable" : "tailscale_command_failed"}: ${detail || `CLI exited with code ${code}`}; ${daemon ? "start the Tailscale daemon and run tailscale up" : "check CLI permissions/version (service list --json must be supported)"}`));
        return;
      }
      resolve(Buffer.concat(stdout).toString("utf8"));
    });
  });
}

async function cliJson(args: readonly string[], config?: TailscaleConfig): Promise<unknown> {
  let text: string;
  try {
    text = await runCommand(config?.execPath ?? (process.platform === "win32" ? "tailscale.exe" : "tailscale"), args);
  } catch (error) {
    if (config?.execPath !== undefined || process.platform !== "darwin" || !(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
    text = await runCommand(MACOS_CLI, args);
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    throw new Error(`tailscale_invalid_json: ${args.join(" ")} did not return JSON; update the official Tailscale CLI`);
  }
}

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function strings(value: unknown, field: string): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || !value.every((item: unknown) => typeof item === "string")) {
    throw new Error(`tailscale_invalid_json: ${field} must be a string array`);
  }
  return value as string[];
}

function string(value: unknown, field: string): string {
  if (typeof value !== "string") throw new Error(`tailscale_invalid_json: ${field} must be a string`);
  return value;
}

function addresses(value: unknown, field: string): string[] {
  const result = strings(value, field);
  if (result.some((address) => isIP(address) === 0)) throw new Error(`tailscale_invalid_json: ${field} contains an invalid IP address`);
  return result;
}

function node(value: unknown): TailnetNode {
  if (!record(value) || typeof value.Online !== "boolean") throw new Error("tailscale_invalid_json: invalid status node");
  return {
    id: string(value.ID, "node.ID"),
    hostname: string(value.HostName, "node.HostName"),
    dnsName: string(value.DNSName, "node.DNSName"),
    addresses: addresses(value.TailscaleIPs, "node.TailscaleIPs"),
    online: value.Online,
    tags: strings(value.Tags, "node.Tags"),
  };
}

async function loadStatus(config?: TailscaleConfig): Promise<TailnetStatus> {
  const value = await cliJson(["status", "--json"], config);
  if (!record(value)) throw new Error("tailscale_invalid_json: status must be an object");
  const backendState = string(value.BackendState, "BackendState");
  if (value.Peer !== undefined && value.Peer !== null && !record(value.Peer)) throw new Error("tailscale_invalid_json: Peer must be an object");
  const tailnet = value.CurrentTailnet;
  if (tailnet !== undefined && tailnet !== null && !record(tailnet)) throw new Error("tailscale_invalid_json: CurrentTailnet must be an object");
  return {
    backendState,
    self: value.Self === undefined || value.Self === null ? null : node(value.Self),
    peers: record(value.Peer) ? Object.values(value.Peer).map(node) : [],
    magicDnsEnabled: record(tailnet) && tailnet.MagicDNSEnabled === true,
    dnsSuffix: record(tailnet) ? string(tailnet.MagicDNSSuffix, "CurrentTailnet.MagicDNSSuffix") : "",
  };
}

export function readTailnetStatus(config?: TailscaleConfig): Promise<TailnetStatus> {
  return cached(statusCache, config, false, () => loadStatus(config));
}

// Official schema pinned at tailscale/tailscale fd1cc09139ba046dccd7d2e3799909f3fa070df2:
// cmd/tailscale/cli/service.go serviceListEntry embeds ServiceDetails plus Hostname;
// tailcfg/proto_port_range.go MarshalText encodes Ports as e.g. "tcp:8712".
function tcpPorts(value: unknown, remainingPorts: number): number[] {
  const intervals: [number, number][] = [];
  for (const entry of strings(value, "service.Ports")) {
    const match = /^(?:(tcp|6|\*):)?(\d+)(?:-(\d+))?$/i.exec(entry);
    if (match === null) continue; // UDP, wildcards and non-concrete declarations are not broker endpoints.
    const first = Number(match[2]);
    const last = match[3] === undefined ? first : Number(match[3]);
    if (!Number.isInteger(first) || !Number.isInteger(last) || first < 1 || last > 65_535 || first > last) {
      throw new Error("tailscale_invalid_json: invalid service TCP port range");
    }
    intervals.push([first, last]);
  }
  intervals.sort((a, b) => a[0] - b[0] || a[1] - b[1]);
  let merged = 0;
  for (const interval of intervals) {
    const previous = merged === 0 ? undefined : intervals[merged - 1];
    if (previous !== undefined && interval[0] <= previous[1] + 1) {
      previous[1] = Math.max(previous[1], interval[1]);
    } else {
      intervals[merged++] = interval;
    }
  }
  let count = 0;
  for (let index = 0; index < merged; index += 1) {
    const [first, last] = intervals[index]!;
    count += last - first + 1;
  }
  if (count > remainingPorts) {
    throw new Error(`tailscale_service_port_limit: service list exceeds ${TAILSCALE_MAX_SERVICE_PORTS} normalized TCP ports; narrow advertised Service port ranges`);
  }
  const ports = new Array<number>(count);
  let index = 0;
  for (let interval = 0; interval < merged; interval += 1) {
    const [first, last] = intervals[interval]!;
    for (let port = first; port <= last; port += 1) ports[index++] = port;
  }
  return ports;
}

async function loadServices(config?: TailscaleConfig): Promise<TailnetService[]> {
  const value = await cliJson(["service", "list", "--json"], config);
  if (!Array.isArray(value)) throw new Error("tailscale_invalid_json: service list must be an array");
  let remainingPorts = TAILSCALE_MAX_SERVICE_PORTS;
  return value.map((entry: unknown) => {
    if (!record(entry)) throw new Error("tailscale_invalid_json: service entry must be an object");
    const ports = tcpPorts(entry.Ports, remainingPorts);
    remainingPorts -= ports.length;
    return {
      name: string(entry.Name, "service.Name"),
      hostname: string(entry.Hostname, "service.Hostname"),
      addresses: addresses(entry.Addrs, "service.Addrs"),
      ports,
    };
  });
}

export function listTailnetServices(config?: TailscaleConfig): Promise<TailnetService[]> {
  return cached(serviceCache, config, false, () => loadServices(config));
}

function normalizedName(name: string): string {
  return name.toLowerCase().replace(/\.$/, "");
}

function tcpTarget(host: string, port: number): { endpoint: MeshEndpoint; url: string } {
  const url = `tcp://${isIP(host) === 6 ? `[${host}]` : host}:${port}`;
  const endpoint = parseEndpoint(url);
  if (endpoint === null) throw new Error(`tailscale_endpoint_invalid: invalid advertised endpoint ${url}`);
  return { endpoint, url };
}

export async function resolveConnection(config: MeshConfig, options: { refresh?: boolean } = {}): Promise<ResolvedConnection> {
  if (config.brokerUrl !== undefined) {
    const endpoint = parseEndpoint(config.brokerUrl);
    if (endpoint === null) throw new Error(`broker_endpoint_invalid: ${config.brokerUrl}`);
    return { endpoint, url: config.brokerUrl, source: "explicit", meshId: config.meshId };
  }
  const discovery = config.tailscale;
  if (discovery === undefined || discovery.mode === "off") return { endpoint: null, url: null, source: "local", meshId: config.meshId };
  const meshId = config.meshId ?? "default";
  const status = await cached(statusCache, discovery, options.refresh === true, () => loadStatus(discovery));
  if (status.backendState !== "Running") throw new Error(`tailscale_down: backend is ${status.backendState}; run tailscale up and select the correct tailnet`);
  if (discovery.mode === "service") {
    const name = discovery.service ?? `svc:pi-mesh-${meshId}`;
    const services = await cached(serviceCache, discovery, options.refresh === true, () => loadServices(discovery));
    const matches = services.filter((service) => service.name === name);
    if (matches.length === 0) throw new Error(`tailscale_service_not_found: ${name}; advertise and approve this Service in the selected tailnet`);
    if (matches.length !== 1) throw new Error(`tailscale_service_ambiguous: ${name} has multiple advertisements`);
    const service = matches[0]!;
    if (!service.ports.includes(discovery.port)) throw new Error(`tailscale_service_port_missing: ${name} does not declare TCP port ${discovery.port}`);
    const host = service.addresses[0] ?? (status.magicDnsEnabled ? service.hostname : "");
    if (!host) throw new Error(`tailscale_service_address_missing: ${name} has no advertised address (enable MagicDNS or publish Service IPs)`);
    return { ...tcpTarget(host, discovery.port), source: "tailscale-service", meshId, service };
  }
  if (discovery.host === undefined && discovery.tag === undefined) throw new Error("tailscale_node_selector_missing: configure an exact hostname, FQDN, IP, or tag");
  const nodes = status.self === null ? status.peers : [status.self, ...status.peers.filter((peer) => peer.id !== status.self?.id)];
  const hostName = discovery.host === undefined ? undefined : normalizedName(discovery.host);
  const matches = nodes.filter((candidate) =>
    (hostName === undefined || normalizedName(candidate.hostname) === hostName || normalizedName(candidate.dnsName) === hostName || candidate.addresses.includes(discovery.host!)) &&
    (discovery.tag === undefined || candidate.tags.includes(discovery.tag)),
  );
  if (matches.length === 0) throw new Error(`tailscale_node_not_found: no node matches ${discovery.host ?? discovery.tag}; check the exact selector and tailnet ACLs`);
  if (matches.length !== 1) throw new Error(`tailscale_node_ambiguous: ${matches.length} nodes match ${discovery.host ?? discovery.tag}; configure an exact FQDN/IP or a unique tag`);
  const selected = matches[0]!;
  const host = discovery.host !== undefined && selected.addresses.includes(discovery.host)
    ? discovery.host : selected.addresses[0] ?? (status.magicDnsEnabled ? normalizedName(selected.dnsName) : "");
  if (!host) throw new Error("tailscale_node_address_missing: selected node has no Tailscale IP (enable MagicDNS or check node status)");
  // Online describes daemon visibility, not broker readiness: only the token-authenticated handshake proves readiness.
  return { ...tcpTarget(host, discovery.port), source: "tailscale-node", meshId, node: selected };
}
