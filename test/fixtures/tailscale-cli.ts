import * as fs from "node:fs";
import * as path from "node:path";
const root = process.env.MESH_TEST_TAILSCALE_ROOT;
if (!root) throw new Error("MESH_TEST_TAILSCALE_ROOT is required");
const args = process.argv.slice(2);
fs.appendFileSync(path.join(root, "calls.jsonl"), JSON.stringify(args) + "\n");
const state = JSON.parse(fs.readFileSync(path.join(root, "cli-state.json"), "utf8")) as {
  status: unknown;
  services: unknown;
  stderr?: string;
  exitCode?: number;
  outputBytes?: number;
  raw?: string;
  startedFile?: string;
  delayMs?: number;
};
const command = args.join(" ");
if (command !== "status --json" && command !== "service list --json") {
  process.stderr.write("unexpected arguments");
  process.exit(2);
}
if (state.startedFile) fs.writeFileSync(state.startedFile, command);
function respond() {
  if (state.stderr) process.stderr.write(state.stderr);
  if (state.exitCode) process.exit(state.exitCode);
  if (state.outputBytes) process.stdout.write("x".repeat(state.outputBytes));
  else if (state.raw !== undefined) process.stdout.write(state.raw);
  else process.stdout.write(JSON.stringify(command === "status --json" ? state.status : state.services));
}
setTimeout(respond, state.delayMs || 0);
