// client/reconnect.ts — ensureBroker auto-spawn, connect probe, backoff.
import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { once } from "node:events";
import { setTimeout as sleep } from "node:timers/promises";
import * as fs from "node:fs";
import net from "node:net";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BROKER_STARTUP_DIAGNOSTIC_MAX_BYTES,
  ENSURE_BROKER_MAX_POLLS,
  ENSURE_BROKER_POLL_MS,
  LOCK_RETRY_MAX,
  RECONNECT_BASE_MS,
  RECONNECT_MAX_MS,
} from "../shared/config.js";
import { brokerLockPath, brokerSocketPath } from "../shared/paths.js";
import { brokerLaunch } from "../shared/broker-launch.js";

const BROKER_ENTRY_REL = "../broker/broker.js";
const ENV_BROKER_ENTRY = "MESH_BROKER_ENTRY";

/** Walk up from `startDir` to the nearest directory containing package.json. */
function findPackageRoot(startDir: string): string | undefined {
  let dir = path.resolve(startDir);
  for (;;) {
    if (fs.existsSync(path.join(dir, "package.json"))) return dir;
    const parent = path.dirname(dir);
    if (parent === dir) return undefined;
    dir = parent;
  }
}

/**
 * Resolve the broker entry, in order
 * 1. $MESH_BROKER_ENTRY override (highest priority, returned as-is).
 * 2. Sibling candidate: <moduleDir>/../broker/broker.js — only if it exists on disk.
 * 3. Repo-root fallback: nearest package.json walking up from moduleDir, then
 *  <root>/dist/src/broker/broker.js — only if it exists (covers jiti-loaded
 *  TS sources, where the sibling is broker.ts, not broker.js).
 * 4. Otherwise throw a descriptive error listing the tried paths.
 * Never returns a path that does not exist on disk (except the explicit override).
 */
export function brokerEntryPath(
  env: NodeJS.ProcessEnv = process.env,
  moduleDir: string = path.dirname(fileURLToPath(import.meta.url)),
): string {
  const override = env[ENV_BROKER_ENTRY];
  if (override && override.trim() !== "") return override;
  const sibling = path.resolve(moduleDir, BROKER_ENTRY_REL);
  if (fs.existsSync(sibling)) return sibling;
  const root = findPackageRoot(moduleDir);
  const distFallback =
    root === undefined ? undefined : path.join(root, "dist", "src", "broker", "broker.js");
  if (distFallback !== undefined && fs.existsSync(distFallback)) return distFallback;
  const tried = [sibling, ...(distFallback === undefined ? [] : [distFallback])];
  throw new Error(
    `broker entry not found: run \`npm run build\` or set ${ENV_BROKER_ENTRY} (tried: ${tried.join(", ")})`,
  );
}

/** Try to connect to the broker socket. */
export function connectProbe(socketPath: string, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.createConnection(socketPath);
    let settled = false;
    const finish = (ok: boolean): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      resolve(ok);
    };
    const timer = setTimeout(() => finish(false), timeoutMs);
    timer.unref();
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
}

/** Try to connect to a remote tcp/tls broker endpoint (host/port probe). */
export function connectProbeTcp(host: string, port: number, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port });
    let settled = false;
    const finish = (ok: boolean): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      resolve(ok);
    };
    const timer = setTimeout(() => finish(false), timeoutMs);
    timer.unref();
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
}


function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Remove only our launch lock, never one belonging to a concurrent broker. */
function removeLaunchLock(lockPath: string, pid: number): void {
  try {
    const currentPid = Number(fs.readFileSync(lockPath, "utf8").trim());
    if (currentPid === pid || currentPid === process.pid) fs.unlinkSync(lockPath);
  } catch {
    // Already removed or replaced.
  }
}

async function startBroker(runtimeDir: string, lockPath: string, sockPath: string): Promise<void> {
  let ownerPid = process.pid;
  let child: ChildProcess | undefined;
  let startupFailure: string | undefined;
  let diagnostics = Buffer.alloc(0);
  let captureDiagnostics = true;
  let entry: string | undefined;
  let executable: string | undefined;
  let childClosed: Promise<void> | undefined;
  try {
    entry = brokerEntryPath();
    const launch = brokerLaunch(entry);
    executable = launch.executable;
    child = spawn(launch.executable, launch.args, {
      detached: true,
      stdio: ["ignore", "ignore", "pipe"],
      env: {
        ...process.env,
        MESH_RUNTIME_DIR: runtimeDir,
        MESH_BROKER_STARTER_PID: String(process.pid),
      },
    });
    childClosed = once(child, "close").then(
      () => {},
      (error: unknown) => {
        startupFailure = error instanceof Error ? error.message : String(error);
      },
    );
    child.on("error", (error: Error) => {
      startupFailure = error.message;
    });
    child.once("close", (code, signal) => {
      startupFailure ??= `process exited ${signal === null ? `with code ${code}` : `on ${signal}`}`;
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      if (captureDiagnostics && diagnostics.length < BROKER_STARTUP_DIAGNOSTIC_MAX_BYTES) {
        diagnostics = Buffer.concat([diagnostics, chunk.subarray(0, BROKER_STARTUP_DIAGNOSTIC_MAX_BYTES - diagnostics.length)]);
      }
    });
    child.stderr?.on("error", (error: Error) => {
      startupFailure ??= error.message;
    });
    if (child.pid !== undefined) {
      ownerPid = child.pid;
    }
    for (let i = 0; i < ENSURE_BROKER_MAX_POLLS; i += 1) {
      await sleep(ENSURE_BROKER_POLL_MS);
      if (await connectProbe(sockPath, ENSURE_BROKER_POLL_MS)) {
        captureDiagnostics = false;
        diagnostics = Buffer.alloc(0);
        child.unref();
        // Keep draining stderr without retaining output or holding the host open.
        const stderr = child.stderr as typeof child.stderr & { unref?: () => void };
        stderr?.unref?.();
        return;
      }
      if (startupFailure !== undefined) throw new Error(startupFailure);
    }
    throw new Error("socket did not become ready within the startup window");
  } catch (error) {
    if (child !== undefined) {
      if (child.pid !== undefined && child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
      }
      await childClosed;
    }
    const detail = error instanceof Error ? error.message : String(error);
    const stderr = diagnostics.length === 0 ? "" : `; startup stderr: ${JSON.stringify(diagnostics.toString("utf8"))}`;
    throw new Error(
      `broker_unavailable: ${detail}${executable === undefined ? "" : ` (executable: ${executable})`}${entry === undefined ? "" : ` (entry: ${entry})`}${stderr}; check MESH_BROKER_EXEC_PATH and MESH_BROKER_ENTRY`,
    );
  } finally {
    if (captureDiagnostics) removeLaunchLock(lockPath, ownerPid);
  }
}

const brokerStarts = new Map<string, Promise<string>>();

/** Share in-flight startup within a host; the PID lock coordinates other hosts. */
export function ensureBroker(runtimeDir: string): Promise<string> {
  const key = path.resolve(runtimeDir);
  const pending = brokerStarts.get(key);
  if (pending !== undefined) return pending;
  const starting = ensureBrokerReady(runtimeDir).finally(() => {
    brokerStarts.delete(key);
  });
  brokerStarts.set(key, starting);
  return starting;
}

async function ensureBrokerReady(runtimeDir: string): Promise<string> {
  const sockPath = brokerSocketPath(runtimeDir);
  const lockPath = brokerLockPath(runtimeDir);
  fs.mkdirSync(runtimeDir, { recursive: true });

  for (let attempt = 0; attempt < LOCK_RETRY_MAX; attempt += 1) {
    if (await connectProbe(sockPath, ENSURE_BROKER_POLL_MS * ENSURE_BROKER_MAX_POLLS)) {
      return sockPath;
    }
    let lockCreated = false;
    try {
      // Keep a positive live owner for compatibility with concurrent older clients.
      // The child alone replaces this lock through its explicit starter handshake.
      fs.writeFileSync(lockPath, String(process.pid), { flag: "wx" });
      lockCreated = true;
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") {
        throw new Error(`broker_unavailable: cannot acquire ${lockPath}: ${String(error)}`);
      }
    }
    if (lockCreated) {
      await startBroker(runtimeDir, lockPath, sockPath);
      return sockPath;
    }
    let pid = Number.NaN;
    try {
      pid = Number(fs.readFileSync(lockPath, "utf8").trim());
    } catch {
      // A concurrent startup may already have removed its lock.
    }
    if (!(Number.isInteger(pid) && pid > 0 && pidAlive(pid))) {
      try {
        fs.unlinkSync(lockPath);
      } catch {
        // Already gone.
      }
      continue;
    }
    for (let i = 0; i < ENSURE_BROKER_MAX_POLLS; i += 1) {
      await sleep(ENSURE_BROKER_POLL_MS);
      if (await connectProbe(sockPath, ENSURE_BROKER_POLL_MS)) return sockPath;
    }
  }
  throw new Error(`broker_unavailable: socket ${sockPath} is not ready; inspect ${lockPath} and MESH_BROKER_EXEC_PATH / MESH_BROKER_ENTRY`);
}

/** Reconnect backoff: 250 ms × 2^n, capped at 5 s. */
export function backoffMs(attempt: number): number {
  const exp = RECONNECT_BASE_MS * 2 ** Math.max(0, attempt);
  return Math.min(RECONNECT_MAX_MS, exp);
}
