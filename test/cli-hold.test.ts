// test/cli-hold.test.ts — Phase 4: reserve dry-run conflicts (exit 4),
// --hold keeps the claim alive and Ctrl-C releases it, release one-shot is
// an honest no-op.
import test from "node:test";
import assert from "node:assert/strict";
import { MeshClient } from "../src/client/client.js";
import { makeTempDirs, startTestBroker, sleep, spawnCli, type CliProcess } from "./helpers.js";

const SCRUB_ENV_KEYS = [
  "MESH_BROKER_URL",
  "MESH_BROKER_TOKEN",
  "MESH_LISTEN",
  "MESH_TLS_CERT",
  "MESH_TLS_KEY",
  "MESH_TLS_CA",
  "MESH_TLS_INSECURE",
  "MESH_POLICY",
  "MESH_ALIAS",
  "MESH_ROOMS",
];

function hermeticEnv(extra: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const base: NodeJS.ProcessEnv = { ...process.env };
  for (const key of SCRUB_ENV_KEYS) delete base[key];
  return { ...base, ...extra };
}

/** Await an (async) predicate — helpers.waitFor is sync-only: an async
 * predicate returns a Promise, which is truthy and would pass instantly. */
async function until(fn: () => Promise<boolean> | boolean, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await fn()) return;
    if (Date.now() > deadline) throw new Error("until: timeout");
    await sleep(100);
  }
}

test("reserve dry-run reports peer conflicts and exits 4 (honest partial)", async () => {
  const dirs = makeTempDirs("cli-hold1-");
  const broker = await startTestBroker(dirs.runtimeDir);
  const holder = new MeshClient({ alias: "holder", runtimeDir: dirs.runtimeDir });
  await holder.connect();
  await holder.reserve(["src/shared.ts"], "mine");
  await sleep(300); // let the reserve broadcast settle
  let proc: CliProcess | undefined;
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    proc = spawnCli(["reserve", "src/shared.ts", "--reason", "mine-too"], hermeticEnv(env));
    const code = await proc.done;
    assert.equal(code, 4, `out: ${proc.out}\nerr: ${proc.err}`);
    assert.match(proc.out, /reserved src\/shared\.ts/);
    assert.match(proc.err, /conflict: src\/shared\.ts held by @holder/);
    assert.match(proc.out, /released on exit/);
  } finally {
    await proc?.cleanup();
    await holder.close();
    await broker.close();
    dirs.cleanup();
  }
});

test("reserve --hold keeps the claim; SIGINT releases it cleanly (exit 0)", async () => {
  const dirs = makeTempDirs("cli-hold2-");
  const broker = await startTestBroker(dirs.runtimeDir);
  const observer = new MeshClient({ alias: "observer", runtimeDir: dirs.runtimeDir });
  await observer.connect();
  let proc: CliProcess | undefined;
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    proc = spawnCli(["reserve", "src/held.ts", "--reason", "holding", "--hold", "60000"], hermeticEnv(env));

    // the claim becomes visible to peers…
    await until(async () => {
      const snap = await observer.status();
      return snap.peers.some((p) => (p.reservations ?? []).some((r) => r.pattern === "src/held.ts"));
    });
    assert.match(proc.out, /holding for 60s/);

    // Ctrl-C → release + clean exit 0
    proc.interrupt();
    const code = await proc.done;
    assert.equal(code, 0, `out: ${proc.out}\nerr: ${proc.err}`);
    assert.match(proc.out, /releasing/);

    // …and the claim is gone
    await until(async () => {
      const snap = await observer.status();
      return !snap.peers.some((p) => (p.reservations ?? []).some((r) => r.pattern === "src/held.ts"));
    });
  } finally {
    await proc?.cleanup();
    await observer.close();
    await broker.close();
    dirs.cleanup();
  }
});

test("reserve --hold out of bounds exits 2 before any claim", async () => {
  const dirs = makeTempDirs("cli-hold3-");
  let proc: CliProcess | undefined;
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    proc = spawnCli(["reserve", "src/x.ts", "--hold", "99999999999"], hermeticEnv(env));
    const code = await proc.done;
    assert.equal(code, 2);
    assert.match(proc.err, /invalid --hold/);
  } finally {
    await proc?.cleanup();
    dirs.cleanup();
  }
});

test("release one-shot: honest no-op (a fresh process holds nothing)", async () => {
  const dirs = makeTempDirs("cli-hold4-");
  const broker = await startTestBroker(dirs.runtimeDir);
  let proc: CliProcess | undefined;
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    proc = spawnCli(["release", "src/whatever.ts"], hermeticEnv(env));
    const code = await proc.done;
    assert.equal(code, 0, `err: ${proc.err}`);
    assert.match(proc.out, /released nothing/);
    assert.match(proc.out, /connection-scoped/);
  } finally {
    await proc?.cleanup();
    await broker.close();
    dirs.cleanup();
  }
});
