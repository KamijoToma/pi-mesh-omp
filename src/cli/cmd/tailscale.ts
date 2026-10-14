import { parseArgs, type ArgSpec } from "../args.js";
import { EXIT_MESH_FAILURE, EXIT_OK, EXIT_USAGE } from "../codes.js";
import { jsonLine, say, sayErr } from "../out.js";
import { isMeshId, loadConfig } from "../../shared/config.js";
import { runtimeDir, stateDir } from "../../shared/paths.js";
import { executeTailnetCommand, type TailnetCommand, type TailnetSelection } from "../../tailscale/commands.js";

export const TAILSCALE_USAGE = "usage: mesh tailscale status|discover|doctor [--json]\n       mesh tailscale join <mesh-id> [--service svc:NAME | --host HOST | --tag tag:NAME] [--port PORT] [--json]";

const SPECS: ArgSpec[] = [
  { name: "service", kind: "value", meta: "svc:NAME" },
  { name: "host", kind: "value", meta: "HOST" },
  { name: "tag", kind: "value", meta: "tag:NAME" },
  { name: "port", kind: "value", meta: "PORT" },
  { name: "json", kind: "flag" },
];

export type ParsedTailnetArgs =
  | { ok: true; help: boolean; json: boolean; command: TailnetCommand }
  | { ok: false; error: string };

export function parseTailnetArgs(argv: string[]): ParsedTailnetArgs {
  const first = argv[0];
  const action = first === undefined || first.startsWith("-") ? "status" : first;
  if (action !== "status" && action !== "discover" && action !== "join" && action !== "doctor") {
    return { ok: false, error: `unknown tailscale action: ${action}` };
  }
  const parsed = parseArgs(first === undefined || first.startsWith("-") ? argv : argv.slice(1), SPECS);
  if (!parsed.ok) return parsed;
  if (parsed.help) return { ok: true, help: true, json: false, command: { action } };
  const args = parsed.parsed;
  if (action !== "join") {
    if (args.positionals.length !== 0 || args.values.size !== 0) {
      return { ok: false, error: `${action} takes only --json` };
    }
    return { ok: true, help: false, json: args.flags.has("json"), command: { action } };
  }
  const meshId = args.positionals[0];
  if (args.positionals.length !== 1 || !isMeshId(meshId)) {
    return { ok: false, error: "join requires one valid mesh identifier" };
  }
  const selection: TailnetSelection = { meshId };
  for (const key of ["service", "host", "tag"] as const) {
    const value = args.values.get(key);
    if (value !== undefined) selection[key] = value;
  }
  const rawPort = args.values.get("port");
  if (rawPort !== undefined) {
    const port = Number(rawPort);
    if (!Number.isInteger(port) || port < 1 || port > 65_535) return { ok: false, error: "port must be an integer between 1 and 65535" };
    selection.port = port;
  }
  if (selection.service !== undefined && (selection.host !== undefined || selection.tag !== undefined)) {
    return { ok: false, error: "choose a Service or node selector, not both" };
  }
  return { ok: true, help: false, json: args.flags.has("json"), command: { action, selection } };
}

export async function cmdTailscale(argv: string[]): Promise<number> {
  const parsed = parseTailnetArgs(argv);
  if (!parsed.ok) {
    sayErr(`${parsed.error}\n${TAILSCALE_USAGE}`);
    return EXIT_USAGE;
  }
  if (parsed.help) {
    say(TAILSCALE_USAGE);
    return EXIT_OK;
  }
  try {
    const result = await executeTailnetCommand(parsed.command, loadConfig(stateDir()), stateDir(), runtimeDir());
    if (parsed.json) jsonLine({ ...result.details, ok: result.ok });
    else say(result.text);
    return result.ok ? EXIT_OK : EXIT_MESH_FAILURE;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (parsed.json) jsonLine({ schema: "mesh.tailnet.v1", action: parsed.command.action, error: message, ok: false });
    else sayErr(message);
    return EXIT_MESH_FAILURE;
  }
}
