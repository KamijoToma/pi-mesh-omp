import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { acquireLock } from "../src/broker/broker.js";
import { connectProbe } from "../src/client/reconnect.js";
import { brokerLaunch } from "../src/shared/broker-launch.js";
import { brokerLockPath, socketPathForDir } from "../src/shared/paths.js";
import { waitFor } from "./helpers.js";

const reconnectUrl = new URL("../src/client/reconnect.js", import.meta.url).href;
const commandUrl = new URL("../src/cli/cmd/broker.js", import.meta.url).href;
const brokerEntry = fileURLToPath(new URL("../src/broker/broker.js", import.meta.url));

// Each startup runs in a separate host process: no global env/module mutation.
async function runHost(
  root: string,
  mode: "auto" | "cli" | "stop",
  extra: NodeJS.ProcessEnv = {},
  replacementPid?: number,
): Promise<{
  code: number | null;
  out: string;
  err: string;
}> {
  const driver = path.join(fs.mkdtempSync(path.join(root, "host-")), "host.mjs");
  fs.writeFileSync(driver, `
import * as fs from "node:fs";
import { ensureBroker } from ${JSON.stringify(reconnectUrl)};
import { cmdBroker } from ${JSON.stringify(commandUrl)};
// Deterministically replace the lock at the process-death boundary. The signal
// and liveness checks still use the real OS process, on both POSIX and Windows.
const replacementPid = ${JSON.stringify(replacementPid ?? null)};
if (replacementPid !== null) {
  const kill = process.kill.bind(process);
  let stoppedPid;
  process.kill = (pid, signal) => {
    try {
      const result = kill(pid, signal);
      if (signal === "SIGTERM") stoppedPid = pid;
      return result;
    } catch (error) {
      if (signal === 0 && pid === stoppedPid && error.code === "ESRCH") {
        fs.writeFileSync(${JSON.stringify(brokerLockPath(path.join(root, "runtime")))}, String(replacementPid));
        stoppedPid = undefined;
      }
      throw error;
    }
  };
}
try {
  if (process.argv[2] !== "auto") {
    process.exitCode = await cmdBroker(process.argv[2] === "stop" ? "stop" : "start");
  } else {
    const sockets = await Promise.all(Array.from({ length: 8 }, () => ensureBroker(process.env.MESH_RUNTIME_DIR)));
    process.stdout.write(JSON.stringify(sockets) + "\\n");
  }
} catch (error) {
  process.stderr.write(error instanceof Error ? error.message : String(error));
  process.exitCode = 3;
}
`);
  const env: NodeJS.ProcessEnv = { ...process.env };
  for (const key of Object.keys(env)) {
    if (key.startsWith("MESH_")) delete env[key];
  }
  Object.assign(env, {
    MESH_RUNTIME_DIR: path.join(root, "runtime"),
    MESH_STATE_DIR: path.join(root, "state"),
    ...extra,
  });
  const child = spawn(process.execPath, [driver, mode], { env, stdio: ["ignore", "pipe", "pipe"] });
  let out = "";
  let err = "";
  child.stdout.on("data", (chunk: Buffer) => { out += chunk.toString("utf8"); });
  child.stderr.on("data", (chunk: Buffer) => { err += chunk.toString("utf8"); });
  const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
  try {
    const [code, signal] = await once(child, "close");
    assert.equal(signal, null, `host did not finish: ${err}`);
    return { code: typeof code === "number" ? code : null, out, err };
  } finally {
    clearTimeout(timer);
  }
}

async function cleanup(root: string): Promise<void> {
  const lock = brokerLockPath(path.join(root, "runtime"));
  if (fs.existsSync(lock)) {
    const pid = Number(fs.readFileSync(lock, "utf8").trim());
    if (Number.isInteger(pid) && pid > 0 && pid !== process.pid) {
      const ready = await connectProbe(socketPathForDir(path.join(root, "runtime")), 500);
      const result = await runHost(root, "stop");
      if (result.code !== 0) {
        // Failed startup has no ready broker for CLI stop. Still reap a stray
        // child on failure, without depending on JS signal handlers to run.
        try {
          process.kill(pid, "SIGKILL");
        } catch (error) {
          if (!(error instanceof Error) || !("code" in error) || error.code !== "ESRCH") throw error;
        }
        await waitFor(() => {
          try {
            process.kill(pid, 0);
            return false;
          } catch (error) {
            if (!(error instanceof Error) || !("code" in error) || error.code !== "ESRCH") throw error;
            return true;
          }
        });
      }
      if (ready) {
        assert.equal(result.code, 0, result.err);
        assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
        assert.equal(fs.existsSync(lock), false, "CLI stop left the stopped broker's lock");
      }
    }
  }
  fs.rmSync(root, { recursive: true, force: true });
}

test("compiled Bun metadata selects an external runtime without host flags", () => {
  for (const bun of [
    { isStandaloneExecutable: true },
    { main: "/$bunfs/root/custom-app" },
    { main: "B:\\~BUN\\root\\custom-app.exe" },
  ]) {
    const launch = brokerLaunch("broker.js", {}, {
      execPath: "/host/custom-app",
      execArgv: ["--host-flag"],
      bun: { ...bun, which: () => process.execPath },
    });
    assert.equal(launch.executable, process.execPath);
    assert.deepEqual(launch.args, ["broker.js"]);
  }
});

test("compiled runtime lookup skips the host and falls back from node to bun", () => {
  const launch = brokerLaunch("broker.js", {}, {
    execPath: "/host/app",
    execArgv: ["--not-a-runtime-option"],
    bun: {
      isStandaloneExecutable: true,
      which: (name) => name === "node" ? "/host/app" : process.execPath,
    },
  });
  assert.equal(launch.executable, process.execPath);
  assert.deepEqual(launch.args, ["broker.js"]);
  assert.throws(() => brokerLaunch("broker.js", {}, {
    execPath: "/host/app", execArgv: [], bun: { isStandaloneExecutable: true, which: () => null },
  }), /MESH_BROKER_EXEC_PATH/);
  assert.throws(() => brokerLaunch("broker.js", { MESH_BROKER_EXEC_PATH: "/host/app" }, {
    execPath: "/host/app", execArgv: [], bun: { isStandaloneExecutable: true },
  }), /MESH_BROKER_EXEC_PATH/);
});

test("source Node preserves loader flags only when genuinely reusing its runtime", () => {
  const runtime = { execPath: process.execPath, execArgv: ["--import", "loader.mjs"] };
  assert.deepEqual(brokerLaunch("broker.js", {}, runtime).args, ["--import", "loader.mjs", "broker.js"]);
  assert.deepEqual(brokerLaunch("broker.js", { MESH_BROKER_EXEC_PATH: process.execPath }, runtime).args, ["broker.js"]);
  assert.deepEqual(brokerLaunch("broker.js", {}, {
    ...runtime, bun: { main: "/source/app.ts" },
  }).args, ["broker.js"]);
});

for (const mode of ["auto", "cli"] as const) {
  test(`${mode} starts a real broker with executable and script overrides and waits for readiness`, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-"));
    try {
      const result = await runHost(root, mode, {
        MESH_BROKER_EXEC_PATH: process.execPath,
        MESH_BROKER_ENTRY: brokerEntry,
      });
      assert.equal(result.code, 0, result.err);
      const runtimeDir = path.join(root, "runtime");
      const pid = Number(fs.readFileSync(brokerLockPath(runtimeDir), "utf8").trim());
      assert.ok(Number.isInteger(pid) && pid > 0);
      process.kill(pid, 0);
      if (mode === "auto") {
        assert.deepEqual(JSON.parse(result.out), Array(8).fill(socketPathForDir(runtimeDir)));
      }
      // A separate source host must reuse the same live broker, not spawn another.
      const second = await runHost(root, mode);
      assert.equal(second.code, 0, second.err);
      assert.equal(Number(fs.readFileSync(brokerLockPath(runtimeDir), "utf8").trim()), pid);
    } finally {
      await cleanup(root);
    }
  });

  test(`${mode} reports spawn errors and leaves no launch lock`, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-invalid-"));
    const executable = path.join(root, "missing-runtime");
    try {
      const result = await runHost(root, mode, {
        MESH_BROKER_EXEC_PATH: executable,
        MESH_BROKER_ENTRY: brokerEntry,
      });
      assert.notEqual(result.code, 0);
      assert.ok(result.err.includes(executable), result.err);
      assert.equal(result.out, "");
      assert.equal(fs.existsSync(brokerLockPath(path.join(root, "runtime"))), false);
    } finally {
      await cleanup(root);
    }
  });

  test(`${mode} reports invalid entry exits and bounded startup diagnostics without a live lock`, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-exit-"));
    const entry = path.join(root, "failed-entry.mjs");
    fs.writeFileSync(entry, 'process.stderr.write("diagnostic-marker\\n" + "x".repeat(20_000)); process.exit(17);\n');
    try {
      const result = await runHost(root, mode, {
        MESH_BROKER_EXEC_PATH: process.execPath,
        MESH_BROKER_ENTRY: entry,
      });
      assert.notEqual(result.code, 0);
      assert.ok(result.err.includes(entry), result.err);
      assert.ok(result.err.includes("diagnostic-marker"));
      assert.ok(result.err.length < 6000);
      assert.equal(result.out, "");
      assert.equal(fs.existsSync(brokerLockPath(path.join(root, "runtime"))), false);
    } finally {
      await cleanup(root);
    }
  });
}

test("source host cold-starts without executable or entry overrides", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-source-"));
  try {
    const result = await runHost(root, "auto");
    assert.equal(result.code, 0, result.err);
    assert.deepEqual(JSON.parse(result.out), Array(8).fill(socketPathForDir(path.join(root, "runtime"))));
  } finally {
    await cleanup(root);
  }
});

test("a child PID without a listening socket is not startup success and is terminated", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-not-ready-"));
  const entry = path.join(root, "not-ready.mjs");
  const pidFile = path.join(root, "child.pid");
  fs.writeFileSync(entry, `
import * as fs from "node:fs";
fs.writeFileSync(${JSON.stringify(pidFile)}, String(process.pid));
setInterval(() => {}, 1000);
`);
  try {
    const result = await runHost(root, "cli", {
      MESH_BROKER_EXEC_PATH: process.execPath,
      MESH_BROKER_ENTRY: entry,
    });
    assert.notEqual(result.code, 0);
    assert.ok(result.err.includes(entry), result.err);
    assert.equal(result.out, "");
    assert.equal(fs.existsSync(brokerLockPath(path.join(root, "runtime"))), false);
    const pid = Number(fs.readFileSync(pidFile, "utf8").trim());
    assert.throws(() => process.kill(pid, 0));
  } finally {
    await cleanup(root);
  }
});

test("a missing broker script is an actionable startup failure, not a silent timeout", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-missing-entry-"));
  const entry = path.join(root, "missing-entry.mjs");
  try {
    const result = await runHost(root, "auto", {
      MESH_BROKER_EXEC_PATH: process.execPath,
      MESH_BROKER_ENTRY: entry,
    });
    assert.notEqual(result.code, 0);
    assert.ok(result.err.includes(entry), result.err);
    assert.ok(result.err.includes("MESH_BROKER_ENTRY"), result.err);
    assert.equal(result.out, "");
    assert.equal(fs.existsSync(brokerLockPath(path.join(root, "runtime"))), false);
  } finally {
    await cleanup(root);
  }
});

test("concurrent source hosts coordinate one cold-start through the PID lock", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-race-"));
  try {
    const results = await Promise.all([runHost(root, "auto"), runHost(root, "cli")]);
    for (const result of results) assert.equal(result.code, 0, result.err);
    const runtimeDir = path.join(root, "runtime");
    const pid = Number(fs.readFileSync(brokerLockPath(runtimeDir), "utf8").trim());
    process.kill(pid, 0);
    assert.deepEqual(JSON.parse(results[0]!.out), Array(8).fill(socketPathForDir(runtimeDir)));
  } finally {
    await cleanup(root);
  }
});

test("broker accepts only its matching parent starter handoff and preserves standalone locking", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-handoff-"));
  const lock = brokerLockPath(root);
  try {
    fs.writeFileSync(lock, String(process.ppid));
    assert.equal(acquireLock(root), false);
    assert.equal(fs.readFileSync(lock, "utf8"), String(process.ppid));
    assert.equal(acquireLock(root, process.ppid), true);
    assert.equal(fs.readFileSync(lock, "utf8"), String(process.pid));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("an unrelated live process cannot be taken over by naming it as starter", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-spoof-"));
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
  const closed = once(child, "close");
  try {
    assert.ok(child.pid !== undefined);
    const lock = brokerLockPath(root);
    fs.writeFileSync(lock, String(child.pid));
    assert.equal(acquireLock(root, child.pid), false);
    assert.equal(fs.readFileSync(lock, "utf8"), String(child.pid));
  } finally {
    child.kill("SIGKILL");
    await closed;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("CLI stop does not signal a live startup owner before the broker socket is ready", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-stop-"));
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
  const closed = once(child, "close");
  try {
    assert.ok(child.pid !== undefined);
    const runtimeDir = path.join(root, "runtime");
    fs.mkdirSync(runtimeDir);
    fs.writeFileSync(brokerLockPath(runtimeDir), String(child.pid));
    const result = await runHost(root, "stop");
    assert.notEqual(result.code, 0);
    process.kill(child.pid, 0);
    assert.equal(fs.readFileSync(brokerLockPath(runtimeDir), "utf8"), String(child.pid));
  } finally {
    child.kill("SIGKILL");
    await closed;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("CLI stop confirms process death and preserves a replacement owner's lock", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-launch-stop-replacement-"));
  let pid: number | undefined;
  try {
    const started = await runHost(root, "cli");
    assert.equal(started.code, 0, started.err);
    const lock = brokerLockPath(path.join(root, "runtime"));
    const brokerPid = Number(fs.readFileSync(lock, "utf8").trim());
    pid = brokerPid;
    assert.ok(Number.isInteger(brokerPid) && brokerPid > 0);
    process.kill(brokerPid, 0);
    const stopped = await runHost(root, "stop", {}, process.pid);
    assert.equal(stopped.code, 0, stopped.err);
    assert.throws(() => process.kill(brokerPid, 0), { code: "ESRCH" });
    assert.equal(fs.readFileSync(lock, "utf8").trim(), String(process.pid));
  } finally {
    if (pid !== undefined && Number.isInteger(pid) && pid > 0) {
      try {
        process.kill(pid, "SIGKILL");
      } catch (error) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "ESRCH") throw error;
      }
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
});
