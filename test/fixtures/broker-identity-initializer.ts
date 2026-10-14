import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { loadOrCreateBrokerId } from "../../src/shared/broker-identity.js";

const [stateDir, mode] = process.argv.slice(2);
if (!stateDir || (mode !== "interrupt" && mode !== "concurrent") || !process.send) {
  throw new Error("identity initializer requires a state directory, mode, and IPC channel");
}

process.once("message", (message: unknown) => {
  if (message !== "start") throw new Error("unexpected initializer message");
  if (mode === "interrupt") {
    // The identity loader opens a real file, then the process dies before writing it.
    // No filesystem results are faked; the next broker must recover from the actual disk state.
    fs.writeFileSync = () => process.exit(73);
    syncBuiltinESMExports();
  }
  const id = loadOrCreateBrokerId(stateDir);
  process.stdout.write(`${id}\n`);
  process.disconnect();
});
process.send("ready");
