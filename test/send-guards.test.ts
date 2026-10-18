// test/send-guards.test.ts — M1/M2 mesh_send target guards + busy warnings.
// Pure-function level: the impossible-target errors, the unseen-alias soft
// warning, and the awaitReply-vs-busy timeout mismatch that expired 6/6
// missions in cs-room.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { busyTargetWarning, BUSY_WARN_MIN_MS, checkSendTarget } from "../src/extension/tools.js";

describe("checkSendTarget (M1)", () => {
  it('to:"*" is impossible → local error with broadcast hint', () => {
    const c = checkSendTarget("*", () => false, 3);
    assert.equal(c.error, "invalid_target");
    assert.match(c.hint ?? "", /broadcast: true/);
  });

  it('to:"cs-room-broadcast" pseudo-target → local error (observed in the ledger)', () => {
    const c = checkSendTarget("cs-room-broadcast", () => false, 5);
    assert.equal(c.error, "invalid_target");
    assert.match(c.hint ?? "", /broadcast: true/);
  });

  it("a REAL alias that happens to end in -broadcast is not rejected when online", () => {
    const c = checkSendTarget("ops-broadcast", (a) => a === "ops-broadcast", 1);
    assert.equal(c.error, undefined);
    assert.equal(c.warning, undefined);
  });

  it("unseen alias (presence cache non-empty) → soft warning only, never an error", () => {
    const c = checkSendTarget("ghost", () => false, 4);
    assert.equal(c.error, undefined);
    assert.match(c.warning ?? "", /@ghost not in the latest presence/);
  });

  it("empty presence cache (fresh session) → no warning (nothing to compare)", () => {
    const c = checkSendTarget("anyone", () => false, 0);
    assert.deepEqual(c, {});
  });

  it("known alias → completely clean", () => {
    const c = checkSendTarget("bob", (a) => a === "bob", 2);
    assert.deepEqual(c, {});
  });
});

describe("busyTargetWarning (M2)", () => {
  const to = "agent-1";

  it("busy 8 min + timeout 120 s → warn with the burst pattern advice", () => {
    const w = busyTargetWarning(to, 8 * 60_000, 120_000);
    assert.ok(w !== undefined);
    assert.match(w, /@agent-1 has been busy for 8m/);
    assert.match(w, /mesh_wait_all/);
  });

  it("busy 30 s (a bash turn) → silent, no nag", () => {
    assert.equal(busyTargetWarning(to, 30_000, 120_000), undefined);
  });

  it("busy 4 min (below the 5 min floor) → silent", () => {
    assert.equal(busyTargetWarning(to, BUSY_WARN_MIN_MS - 1_000, 60_000), undefined);
  });

  it("busy 10 min + timeout 30 min → silent (the timeout outlives the busy span)", () => {
    assert.equal(busyTargetWarning(to, 10 * 60_000, 30 * 60_000), undefined);
  });

  it("not busy / unknown → silent", () => {
    assert.equal(busyTargetWarning(to, undefined, 60_000), undefined);
  });
});

// ---- integration through the registered mesh_send tool ----
import { MeshLedger, type LedgerRecord } from "../src/extension/ledger.js";
import { registerTools, type MeshRuntime } from "../src/extension/tools.js";
import type { ExtensionAPI, SessionContext, ToolDefinition } from "../src/extension/pi-types.js";
import { makeTempDirs } from "./helpers.js";
import { readFileSync } from "node:fs";

function makeRt(stateDir: string, over: Record<string, unknown>): MeshRuntime {
  const rt = {
    client: {
      alias: "alice",
      rooms: ["default"],
      roomRoles: new Map([["default", "member"]]),
      isOnline: () => true,
      send: () => Promise.resolve({ status: "delivered", msgId: "m_t_00000001" }),
      connect: () => Promise.reject(new Error("no connect in tests")),
      knowsPeer: (a: string) => a === "bob",
      knownPeerList: ["bob"],
      busyForMs: (a: string) => (a === "busyguy" ? 8 * 60_000 : undefined),
      ...over,
    },
    ledger: new MeshLedger(stateDir),
    transcript: { isEnabled: () => false, record: () => {} },
    guards: { checkSend: () => ({ ok: true, warnings: [] }) },
    ctx: null,
    stateDir,
    runtimeDir: stateDir,
    startedAt: Date.now(),
    ledgerFailures: 0,
    transcriptFailures: 0,
    injectionFailures: 0,
  } as unknown as MeshRuntime;
  return rt;
}

function toolOf(rt: MeshRuntime): ToolDefinition {
  const tools = new Map<string, ToolDefinition>();
  registerTools({ registerTool: (d: ToolDefinition) => tools.set(d.name, d) } as unknown as ExtensionAPI, () => rt);
  const t = tools.get("mesh_send");
  if (t === undefined) throw new Error("mesh_send not registered");
  return t;
}

async function run(tool: ToolDefinition, params: Record<string, unknown>) {
  return tool.execute("tc", params, undefined, undefined, {} as unknown as SessionContext);
}

describe("mesh_send guard integration", () => {
  it('to:"*" is refused LOCALLY — no broker frame, blocked ledger record', async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    let sent = 0;
    const rt = makeRt(dirs.stateDir, { send: () => { sent += 1; return Promise.resolve({ status: "delivered", msgId: "m" }); } });
    const res = await run(toolOf(rt), { to: "*", message: "hello room" });
    assert.match(res.content[0]!.text, /blocked: invalid_target/);
    assert.match(res.content[0]!.text, /broadcast: true/);
    assert.equal(sent, 0, "no client.send call for an impossible target");
    const recs: LedgerRecord[] = readFileSync(rt.ledger.path, "utf8").trim().split("\n").map((l) => JSON.parse(l));
    assert.equal(recs[0]!.event, "blocked");
    assert.equal(recs[0]!.code, "invalid_target");
  });

  it("unseen alias still sends — warning appended to text AND details", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const rt = makeRt(dirs.stateDir, {});
    const res = await run(toolOf(rt), { to: "ghost", message: "ping?" });
    assert.match(res.content[0]!.text, /^delivered m_t_00000001/);
    assert.match(res.content[0]!.text, /@ghost not in the latest presence/);
    assert.match(String(res.details?.warning), /not in the latest presence/);
  });

  it("awaitReply toward a long-busy peer with a short timeout carries the busy warning", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const rt = makeRt(dirs.stateDir, {});
    const res = await run(toolOf(rt), { to: "busyguy", message: "job?", awaitReply: true, timeoutMs: 120_000 });
    assert.match(res.content[0]!.text, /busy for 8m/);
    assert.match(res.content[0]!.text, /mesh_wait_all/);
  });

  it("a known, idle, plain send keeps its exact legacy result text", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const rt = makeRt(dirs.stateDir, {});
    const res = await run(toolOf(rt), { to: "bob", message: "hi" });
    assert.equal(res.content[0]!.text, "delivered m_t_00000001");
  });
});
