// test/cli-observe.test.ts — Phase 3: ledger filters, tail -f (real follow),
// status --reservations, config show token masking. All hermetic.
import * as fs from "node:fs";
import * as path from "node:path";
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
  // scrub the INHERITED env only, then merge the test's explicit values —
  // an explicit extra (e.g. MESH_BROKER_TOKEN for the masking test) must
  // survive the scrub of the ambient one.
  const base: NodeJS.ProcessEnv = { ...process.env };
  for (const key of SCRUB_ENV_KEYS) delete base[key];
  return { ...base, ...extra };
}

async function runCli(
  args: string[],
  env: NodeJS.ProcessEnv,
  timeoutMs = 15_000,
): Promise<{ code: number; out: string; err: string }> {
  const proc = spawnCli(args, hermeticEnv(env), timeoutMs);
  try {
    const code = await proc.done;
    return { code, out: proc.out, err: proc.err };
  } finally {
    await proc.cleanup();
  }
}


test("mesh ledger: filters, limit, rotations oldest-first", async () => {
  const dirs = makeTempDirs("cli-ledger-");
  fs.mkdirSync(dirs.stateDir, { recursive: true });
  const rec = (event: string, from: string, to: string, n: number): string =>
    JSON.stringify({ schema: "mesh.ledger.v1", event, from, to, room: "default", id: `m_x${n}`, ts: new Date(2026, 0, 1, 0, 0, n).toISOString(), bodyStored: false });
  // rotation (older) + current (newer)
  fs.writeFileSync(path.join(dirs.stateDir, "ledger-2026-01-01.jsonl.1"), [rec("sent", "alice", "bob", 1), rec("delivered", "bob", "alice", 2)].join("\n") + "\n");
  fs.writeFileSync(path.join(dirs.stateDir, "ledger.jsonl"), [rec("sent", "carol", "bob", 3), rec("reply", "bob", "carol", 4), rec("sent", "alice", "carol", 5)].join("\n") + "\n");
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };

    const all = await runCli(["ledger"], env);
    assert.equal(all.code, 0);
    assert.equal(all.out.trim().split("\n").length, 5); // rotations + current, oldest first

    const fromAlice = await runCli(["ledger", "--from", "alice"], env);
    const aliceLines = fromAlice.out.trim().split("\n");
    assert.equal(aliceLines.length, 2);
    assert.ok(aliceLines.every((l) => l.includes('"from":"alice"')));

    const replies = await runCli(["ledger", "--event", "reply"], env);
    assert.equal(replies.out.trim().split("\n").length, 1);
    assert.match(replies.out, /"event":"reply"/);

    const limited = await runCli(["ledger", "--limit", "2"], env);
    assert.equal(limited.out.trim().split("\n").length, 2);
    // the LAST records, not the first
    assert.match(limited.out, /m_x5/);
    assert.doesNotMatch(limited.out, /m_x1/);

    const bad = await runCli(["ledger", "--limit", "999"], env);
    assert.equal(bad.code, 2);
    assert.match(bad.err, /invalid --limit/);
  } finally {
    dirs.cleanup();
  }
});

test("mesh tail -f: backlog then streams appended records", async () => {
  const dirs = makeTempDirs("cli-tailf-");
  fs.mkdirSync(dirs.stateDir, { recursive: true });
  const ledger = path.join(dirs.stateDir, "ledger.jsonl");
  fs.writeFileSync(ledger, JSON.stringify({ event: "sent", from: "alice", ts: "2026-01-01T00:00:00Z" }) + "\n");
  let proc: CliProcess | undefined;
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    proc = spawnCli(["tail", "-f"], hermeticEnv(env));
    // after the backlog had time to print, append two records
    await sleep(700);
    fs.appendFileSync(ledger, JSON.stringify({ event: "reply", from: "bob", ts: "2026-01-01T00:00:01Z" }) + "\n");
    fs.appendFileSync(ledger, JSON.stringify({ event: "blocked", from: "carol", ts: "2026-01-01T00:00:02Z" }) + "\n");
    await sleep(1200);
    proc.interrupt();
    const code = await proc.done;
    assert.equal(code, 0, "SIGINT exits 0 (agreed outcome)");
    assert.match(proc.out, /"event":"sent"/); // backlog
    assert.match(proc.out, /"event":"reply"/); // streamed
    assert.match(proc.out, /"event":"blocked"/); // streamed
  } finally {
    await proc?.cleanup();
    dirs.cleanup();
  }
});

test("mesh status --reservations shows peer claims with age; stale = alias", async () => {
  const dirs = makeTempDirs("cli-status-");
  const broker = await startTestBroker(dirs.runtimeDir);
  const holder = new MeshClient({ alias: "holder", runtimeDir: dirs.runtimeDir });
  await holder.connect();
  await holder.reserve(["src/owned.ts"], "working on it");
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    const res = await runCli(["status", "--reservations"], env);
    assert.equal(res.code, 0, `stderr: ${res.err}`);
    assert.match(res.out, /holder/);
    assert.match(res.out, /src\/owned\.ts/);
    assert.match(res.out, /working on it/);

    const stale = await runCli(["stale"], env);
    assert.equal(stale.code, 0);
    assert.match(stale.out, /src\/owned\.ts/);

    const json = await runCli(["status", "--json"], env);
    assert.equal(json.code, 0);
    const parsed = JSON.parse(json.out.trim().split("\n")[0] ?? "{}") as { peers: { alias: string; reservations: { pattern: string }[] }[] };
    const h = parsed.peers.find((p) => p.alias === "holder");
    assert.ok(h !== undefined);
    assert.equal(h.reservations.length, 1);
    assert.equal(h.reservations[0]?.pattern, "src/owned.ts");
  } finally {
    await holder.close();
    await broker.close();
    dirs.cleanup();
  }
});

test("mesh tail -f: torn line is carried, not lost", async () => {
  const dirs = makeTempDirs("cli-tailf2-");
  fs.mkdirSync(dirs.stateDir, { recursive: true });
  const ledger = path.join(dirs.stateDir, "ledger.jsonl");
  let proc: CliProcess | undefined;
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    proc = spawnCli(["tail", "-f"], hermeticEnv(env));
    await sleep(700);
    // first write: HALF a line (no newline) — the second completes it
    fs.appendFileSync(ledger, '{"event":"sent","from":"alice","ts":"2026');
    await sleep(400);
    fs.appendFileSync(ledger, '-01-01T00:00:03Z"}\n');
    await sleep(900);
    proc.interrupt();
    const code = await proc.done;
    assert.equal(code, 0, "SIGINT exits 0 (agreed outcome)");
    // the torn record MUST appear complete exactly once
    const matches = proc.out.match(/"event":"sent","from":"alice"/g) ?? [];
    assert.equal(matches.length, 1, `out: ${JSON.stringify(proc.out)}`);
  } finally {
    await proc?.cleanup();
    dirs.cleanup();
  }
});

test("mesh tail -f --limit invalid exits 2 (validation parity)", async () => {
  const dirs = makeTempDirs("cli-tailf3-");
  fs.mkdirSync(dirs.stateDir, { recursive: true });
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir };
    const res = await runCli(["tail", "-f", "--limit", "abc"], env);
    assert.equal(res.code, 2);
    assert.match(res.err, /invalid --limit/);
  } finally {
    dirs.cleanup();
  }
});

test("mesh config show masks the token, honors stateDir config", async () => {
  const dirs = makeTempDirs("cli-config-");
  fs.mkdirSync(dirs.stateDir, { recursive: true });
  fs.writeFileSync(
    path.join(dirs.stateDir, "config.json"),
    JSON.stringify({ rooms: ["ops"], mailboxCap: 42 }),
  );
  try {
    const env = { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir, MESH_BROKER_TOKEN: "super-secret-token" };
    const res = await runCli(["config", "show"], env);
    assert.equal(res.code, 0, `stderr: ${res.err}`);
    assert.match(res.out, /rooms: ops/);
    assert.match(res.out, /cap=42/);
    assert.doesNotMatch(res.out, /super-secret-token/);
    assert.match(res.out, /token: set \(sha256:[0-9a-f]{8}\)/);
  } finally {
    dirs.cleanup();
  }
});
