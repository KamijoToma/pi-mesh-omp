// Broker runtime selection is independent of the host application and SDK.
import * as fs from "node:fs";
import * as path from "node:path";
function currentRuntime() {
    const bun = globalThis.Bun;
    return { execPath: process.execPath, execArgv: process.execArgv, bun };
}
function sameExecutable(left, right) {
    try {
        return fs.realpathSync(left) === fs.realpathSync(right);
    }
    catch {
        return path.resolve(left) === path.resolve(right);
    }
}
/** Select a JS runtime, never a compiled Bun application's entrypoint. */
export function brokerLaunch(entry, env = process.env, runtime = currentRuntime()) {
    const main = runtime.bun?.main?.replaceAll("\\", "/") ?? "";
    const compiled = runtime.bun?.isStandaloneExecutable === true ||
        main.includes("/$bunfs/") || main.includes("/~BUN/");
    const override = env.MESH_BROKER_EXEC_PATH;
    if (override !== undefined && override.trim() !== "") {
        const resolved = runtime.bun?.which?.(override) ?? override;
        if (compiled && sameExecutable(resolved, runtime.execPath)) {
            throw new Error("MESH_BROKER_EXEC_PATH points to the compiled host, not a JavaScript runtime; select a real node or bun executable");
        }
        return { executable: resolved, args: [entry] };
    }
    if (!compiled) {
        // Preserve Node loader/runtime flags only when reusing the actual Node host.
        return {
            executable: runtime.execPath,
            args: runtime.bun === undefined ? [...runtime.execArgv, entry] : [entry],
        };
    }
    for (const name of ["node", "bun"]) {
        const executable = runtime.bun?.which?.(name);
        if (executable !== undefined && executable !== null &&
            !sameExecutable(executable, runtime.execPath)) {
            return { executable, args: [entry] };
        }
    }
    throw new Error("compiled Bun host cannot launch the broker with its own executable; install node or bun on PATH, or set MESH_BROKER_EXEC_PATH to a real JavaScript runtime (MESH_BROKER_ENTRY selects the script separately)");
}
