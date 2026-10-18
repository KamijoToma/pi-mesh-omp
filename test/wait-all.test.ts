// test/wait-all.test.ts — D42: mesh_wait_all (group await with honest summary).
import { after, before, describe, it, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { rmSync } from "node:fs";
import type { RunningBroker } from "../src/broker/broker.js";
import { MeshClient } from "../src/client/client.js";
import type { MeshFrame } from "../src/protocol/envelope.js";
import { makeTempDirs, startTestBroker, waitFor, type TempDirs } from "./helpers.js";

describe("waitAll (D42)", () => {
  let dirs: TempDirs;
  let broker: RunningBroker;
  let lead: MeshClient;
  let bob: MeshClient;
  let carol: MeshClient;

  before(async () => {
    dirs = makeTempDirs("mesh-waitall-");
    broker = await startTestBroker(dirs.runtimeDir);
    lead = new MeshClient({ alias: "lead", runtimeDir: dirs.runtimeDir });
    bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir });
    carol = new MeshClient({ alias: "carol", runtimeDir: dirs.runtimeDir });
    await Promise.all([lead.connect(), bob.connect(), carol.connect()]);
  });

  after(async () => {
    await lead.close().catch(() => {});
    await bob.close().catch(() => {});
    await carol.close().catch(() => {});
    await broker.close();
    rmSync(dirs.root, { recursive: true, force: true });
  });

  it("waitAll with no awaited missions completes immediately with 0/0", async () => {
    lead.cancelAllAwaited(); // clear leftover pendings from previous tests
    const res = await lead.waitAll(1000);
    assert.equal(res.status, "complete");
    assert.equal(res.total, 0);
  });

  it("resolves 'complete' with every answer once all missions are answered", async () => {
    // bob + carol answer automatically on inbound msg
    bob.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") void bob.reply(f.id, `bob:${f.body ?? ""}`);
    });
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") void carol.reply(f.id, `carol:${f.body ?? ""}`);
    });
    // fire the missions WITHOUT awaiting them (waitAll does the waiting)
    const p1 = lead.send({ to: "bob", message: "M1", awaitReply: true, timeoutMs: 30_000 });
    const p2 = lead.send({ to: "carol", message: "M2", awaitReply: true, timeoutMs: 30_000 });
    void p1;
    void p2;
    const res = await lead.waitAll(8000);
    assert.equal(res.status, "complete");
    assert.equal(res.total, 2);
    assert.equal(res.answered, 2);
    assert.deepEqual(res.missing, []);
    const bodies = res.answers.map((a) => a.response);
    assert.ok(bodies.some((b) => b === "bob:M1"));
    assert.ok(bodies.some((b) => b === "carol:M2"));
  });

  it("times out and reports exactly who is missing", async () => {
    // clean the auto-answer handlers from the previous test: bob stays silent
    bob.removeAllListeners("inbound");
    carol.removeAllListeners("inbound");
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") void carol.reply(f.id, "carol done");
    });
    const p1 = lead.send({ to: "bob", message: "silent", awaitReply: true, timeoutMs: 30_000 });
    const p2 = lead.send({ to: "carol", message: "talk", awaitReply: true, timeoutMs: 30_000 });
    void p1;
    void p2;
    const res = await lead.waitAll(1500);
    assert.equal(res.status, "timeout");
    assert.equal(res.total, 2);
    assert.equal(res.answered, 1);
    assert.equal(res.missing.length, 1);
    assert.equal(res.missing[0]?.to, "bob");
    assert.ok(res.answers.some((a) => a.response === "carol done"));
  });

  it("missionStatus exposes the per-mission answer state", async () => {
    const status = lead.missionStatus();
    assert.ok(status.length >= 3);
    const silent = status.find((m) => m.to === "bob" && !m.answered);
    const done = status.find((m) => m.to === "carol" && m.answered);
    assert.ok(silent !== undefined, "bob mission tracked as waiting");
    assert.ok(done !== undefined, "carol mission tracked as answered");
  });
  it("launch mode (block:false) returns immediately and wait_all reports the verdict", async () => {
    lead.cancelAllAwaited();
    bob.removeAllListeners("inbound");
    carol.removeAllListeners("inbound");
    let original: MeshFrame | undefined;
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") original = f;
    });
    const t0 = Date.now();
    const r1 = await lead.send({ to: "carol", message: "fast", awaitReply: true, block: false, timeoutMs: 30_000 });
    const r2 = await lead.send({ to: "bob", message: "slow", awaitReply: true, block: false, timeoutMs: 30_000 });
    assert.equal(r1.status, "delivered");
    assert.equal(r2.status, "delivered");
    assert.ok(Date.now() - t0 < 2000, "launch must NOT block for the replies");
    const verdict = lead.waitAll(2500);
    await waitFor(() => original !== undefined);
    await carol.reply(original!.id, "carol launch done");
    const res = await verdict;
    assert.equal(res.status, "timeout", "bob stays silent");
    assert.ok(res.answers.some((a) => a.to === "carol" && a.response === "carol launch done"), "fast answer in the verdict");
    assert.ok(res.missing.some((m) => m.to === "bob"), "slow mission reported missing");
  });

  it("a mission answered BEFORE wait_all still appears in the verdict", async () => {
    lead.cancelAllAwaited();
    bob.removeAllListeners("inbound");
    carol.removeAllListeners("inbound");
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") void carol.reply(f.id, "early done");
    });
    const r = await lead.send({ to: "carol", message: "early", awaitReply: true, block: false, timeoutMs: 30_000 });
    assert.equal(r.status, "delivered");
    // wait until the answer actually arrived (background resolution)
    await waitFor(() => {
      const m = lead.missionStatus().find((x) => x.msgId === r.msgId);
      return m !== undefined && m.answered ? true : undefined;
    });
    const res = await lead.waitAll(500);
    assert.equal(res.status, "complete");
    assert.equal(res.total, 1, "answered-before-call mission is still in the verdict");
    assert.equal(res.answered, 1);
  });

  it("block:false without awaitReply is refused", async () => {
    const r = await lead.send({ to: "carol", message: "x", block: false });
    assert.equal(r.status, "error");
    assert.match(r.reason ?? "", /block_requires_awaitReply/);
  });

  it("missions blocked at ack are 'failed', never 'waiting' forever", async () => {
    const res = await lead.send({ to: "ghost-peer", message: "nobody home", awaitReply: true, timeoutMs: 500 });
    assert.equal(res.status, "blocked");
    const m = lead.missionStatus().find((x) => x.msgId === res.msgId);
    assert.ok(m, "mission tracked");
    assert.equal(m!.status, "failed");
    assert.equal(m!.answered, false);
  });

  it("missions that expire are 'expired', never 'waiting' forever", async () => {
    const res = await lead.send({ to: "bob", message: "silent mission", awaitReply: true, timeoutMs: 300 });
    assert.equal(res.status, "expired");
    const m = lead.missionStatus().find((x) => x.msgId === res.msgId);
    assert.ok(m, "mission tracked");
    assert.equal(m!.status, "expired");
  });

  it("reports a launch mission that expired before the first verdict", async () => {
    lead.cancelAllAwaited();
    bob.removeAllListeners("inbound");
    const sent = await lead.send({
      to: "bob", message: "expire before wait", awaitReply: true, block: false, timeoutMs: 100,
    });
    assert.equal(sent.status, "delivered");
    await waitFor(() => lead.missionStatus().some((m) => m.msgId === sent.msgId && m.status === "expired"));
    const verdict = await lead.waitAll(50);
    assert.equal(verdict.status, "timeout");
    assert.equal(verdict.total, 1);
    assert.equal(verdict.answered, 0);
    assert.deepEqual(verdict.missing, [{ msgId: sent.msgId, to: "bob" }]);
    assert.deepEqual((await lead.waitAll(50)).missing, verdict.missing);
  });

  it("keeps mailbox drops and ACK failures in the missing verdict until the batch is cleared", async () => {
    lead.cancelAllAwaited();
    await bob.close();
    await waitFor(() => !broker.state.peers.has("bob"));
    try {
      const queued = await lead.send({
        to: "bob", message: "drop before wait", awaitReply: true, block: false, timeoutMs: 30_000,
      });
      assert.equal(queued.status, "queued_offline");
      broker.purgeMailboxExpired(Date.now() + 3_600_001);
      await waitFor(() => lead.missionStatus().some((m) => m.msgId === queued.msgId && m.status === "failed"));
      const blocked = await lead.send({
        to: "unknown-recipient", message: "no such peer", awaitReply: true, block: false,
      });
      assert.equal(blocked.status, "blocked");
      const verdict = await lead.waitAll(50);
      assert.equal(verdict.status, "error");
      assert.equal(verdict.total, 2);
      assert.deepEqual(verdict.missing, [
        { msgId: queued.msgId, to: "bob", reason: "dropped_offline" },
        { msgId: blocked.msgId, to: "unknown-recipient", reason: "peer_not_found" },
      ]);
      lead.cancelAllAwaited();
      assert.deepEqual(lead.missionStatus(), []);
      assert.equal((await lead.waitAll(50)).total, 0);
    } finally {
      await bob.connect();
    }
  });

  it("ESC (AbortSignal) cancels a pending wait_all early, reporting nothing", async () => {
    lead.cancelAllAwaited();
    // bob never answers — wait_all would block until the timeout.
    const res = await lead.send({ to: "bob", message: "esc me", awaitReply: true, block: false, timeoutMs: 30_000 });
    assert.equal(res.status, "delivered");
    const ac = new AbortController();
    const p = lead.waitAll(30_000, ac.signal);
    setTimeout(() => ac.abort(), 300).unref();
    const verdict = await p;
    assert.equal(verdict.status, "cancelled");
    assert.equal(verdict.reason, "aborted");
    assert.equal(verdict.total, 1);
    assert.equal(verdict.answered, 0);
    assert.equal(verdict.missing.length, 1);
  // a cancelled verdict reports NOTHING — the mission stays reportable
  // so a later wait_all (after the interrupt) still lists it.
    const again = await lead.waitAll(500);
    assert.equal(again.status, "timeout");
    assert.equal(again.total, 1);
    assert.equal(again.missing.length, 1);
  });

});


async function isolatedWaitFixture(t: TestContext) {
  const dirs = makeTempDirs("mesh-wait-lifecycle-");
  const broker = await startTestBroker(dirs.runtimeDir);
  const lead = new MeshClient({ alias: "lead", runtimeDir: dirs.runtimeDir, noReconnect: true });
  const bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir, noReconnect: true });
  const carol = new MeshClient({ alias: "carol", runtimeDir: dirs.runtimeDir, noReconnect: true });
  t.after(async () => {
    await Promise.all([lead.close(), bob.close(), carol.close()]);
    await broker.close();
    dirs.cleanup();
  });
  await lead.connect();
  await bob.connect();
  await carol.connect();
  return { lead, bob, carol, broker };
}

describe("waitAll: non-destructive lifecycle exits", () => {
  for (const timing of ["before", "during"] as const) {
    it(`reports a previously-online target going offline ${timing} the wait and permits re-wait`, { timeout: 5000 }, async (t) => {
      const { lead, bob } = await isolatedWaitFixture(t);
      const sent = await lead.send({ to: "bob", message: "keep working", awaitReply: true, block: false });
      assert.ok(sent.msgId);
      const waiting = timing === "during" ? lead.waitAll(2000) : undefined;
      await bob.close();
      await waitFor(() => lead.missionStatus().some((mission) => mission.reason === "target_offline"));
      const result = await (waiting ?? lead.waitAll(2000));
      assert.equal(result.status, "error");
      assert.equal(result.reason, "target_offline");
      assert.deepEqual(result.missing, [{ msgId: sent.msgId, to: "bob", reason: "target_offline" }]);
      assert.equal(lead.pendingCount, 1, "presence loss must not cancel the mission");
      await bob.connect();
      await waitFor(() => lead.knowsPeer("bob"));
      const again = lead.waitAll(2000);
      assert.equal((await bob.reply(sent.msgId, "back with the answer")).status, "delivered");
      const completed = await again;
      assert.equal(completed.status, "complete");
      assert.deepEqual(completed.answers, [{ msgId: sent.msgId, to: "bob", response: "back with the answer" }]);
    });
  }

  it("does not confuse leaving an unrelated room with losing the mission target", { timeout: 5000 }, async (t) => {
    const { lead, bob } = await isolatedWaitFixture(t);
    await lead.join("unrelated");
    await bob.join("unrelated");
    const presence: MeshFrame[] = [];
    lead.on("presence", (frame: MeshFrame) => presence.push(frame));
    const sent = await lead.send({ to: "bob", room: "default", message: "stay here", awaitReply: true, block: false });
    assert.ok(sent.msgId);
    const waiting = lead.waitAll(2000);
    await bob.leave("unrelated");
    await waitFor(() => presence.some((frame) => frame.from === "bob" && frame.room === "unrelated" && frame.status === "offline"));
    assert.equal(lead.knowsPeer("bob"), true);
    assert.equal((await bob.reply(sent.msgId, "still in default")).status, "delivered");
    assert.equal((await waiting).status, "complete");
  });

  it("keeps an initially queued-offline mission waiting until mailbox reconnect and reply", { timeout: 5000 }, async (t) => {
    const { lead, bob } = await isolatedWaitFixture(t);
    await bob.close();
    await waitFor(() => !lead.knowsPeer("bob"));
    const sent = await lead.send({ to: "bob", message: "intentional mailbox", awaitReply: true, block: false });
    assert.equal(sent.status, "queued_offline");
    assert.ok(sent.msgId);
    const waiting = lead.waitAll(2000);
    await bob.connect();
    await waitFor(() => bob.peekInbox(sent.msgId!) !== undefined);
    assert.equal((await bob.reply(sent.msgId, "mailbox completed")).status, "delivered");
    assert.equal((await waiting).status, "complete");
  });

  it("interrupts active waits only, preserving partial answers and pending work for a new wait", { timeout: 5000 }, async (t) => {
    const { lead, bob, carol } = await isolatedWaitFixture(t);
    const first = await lead.send({ to: "bob", message: "first", awaitReply: true, block: false });
    const second = await lead.send({ to: "carol", message: "second", awaitReply: true, block: false });
    assert.ok(first.msgId && second.msgId);
    const inbound: MeshFrame[] = [];
    lead.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    const waiting = lead.waitAll(2000);
    await bob.reply(first.msgId, "partial answer");
    await waitFor(() => lead.missionStatus().some((mission) => mission.msgId === first.msgId && mission.answered));
    assert.equal(lead.interruptWaits("incoming_message", "m_incoming_12345678"), true);
    assert.equal(lead.interruptWaits("nothing_active"), false);
    const interrupted = await waiting;
    assert.equal(interrupted.status, "interrupted");
    assert.equal(interrupted.reason, "incoming_message");
    assert.equal(interrupted.interruptedBy, "m_incoming_12345678");
    assert.equal(interrupted.total, 2);
    assert.deepEqual(interrupted.answers, [{ msgId: first.msgId, to: "bob", response: "partial answer" }]);
    assert.deepEqual(interrupted.missing, [{ msgId: second.msgId, to: "carol" }]);
    assert.equal(lead.pendingCount, 1);
    assert.deepEqual(inbound, [], "matched replies belong only to the active aggregate");
    const again = lead.waitAll(2000);
    await carol.reply(second.msgId, "final answer");
    const complete = await again;
    assert.equal(complete.status, "complete");
    assert.equal(complete.answered, 2);
    assert.equal((await lead.waitAll(50)).total, 0, "normal answer-once semantics are unchanged");
  });

  it("snapshots an explicitly cancelled batch before clearing all mission maps", { timeout: 5000 }, async (t) => {
    const { lead, bob } = await isolatedWaitFixture(t);
    const answered = await lead.send({ to: "bob", message: "answer", awaitReply: true, block: false });
    const missing = await lead.send({ to: "carol", message: "missing", awaitReply: true, block: false });
    assert.ok(answered.msgId && missing.msgId);
    await bob.reply(answered.msgId, "already answered");
    await waitFor(() => lead.missionStatus().some((mission) => mission.msgId === answered.msgId && mission.answered));
    const waiting = lead.waitAll(2000);
    lead.cancelAllAwaited();
    const result = await waiting;
    assert.equal(result.status, "cancelled");
    assert.equal(result.reason, "missions_cancelled");
    assert.equal(result.total, 2);
    assert.equal(result.answered, 1);
    assert.deepEqual(result.missing, [{ msgId: missing.msgId, to: "carol" }]);
    assert.equal(lead.pendingCount, 0);
    assert.deepEqual(lead.missionStatus(), []);
    assert.equal((await lead.waitAll(50)).total, 0);
  });

  it("synchronizes both launch and blocking mission failures on rename", { timeout: 5000 }, async (t) => {
    const { lead, bob } = await isolatedWaitFixture(t);
    const launched = await lead.send({ to: "bob", message: "launched", awaitReply: true, block: false });
    const blocking = lead.send({ to: "bob", message: "blocking", awaitReply: true });
    await waitFor(() => lead.pendingCount === 2 && bob.transcript.filter((frame) => frame.type === "msg").length === 2);
    const waiting = lead.waitAll(2000);
    assert.equal((await lead.rename("new-lead")).ok, true);
    const cancelled = await waiting;
    assert.equal(cancelled.status, "cancelled");
    assert.equal(cancelled.reason, "renamed");
    assert.equal(cancelled.total, 2);
    const blockResult = await blocking;
    assert.equal(blockResult.status, "error");
    if (blockResult.status === "error") assert.equal(blockResult.reason, "renamed");
    assert.equal(lead.pendingCount, 0);
    assert.ok(lead.missionStatus().every((mission) => mission.status === "failed" && mission.reason === "renamed"));
    const result = await lead.waitAll(2000);
    assert.equal(result.status, "error");
    assert.equal(result.reason, "renamed");
    assert.equal(result.missing.length, 2);
    assert.ok(result.missing.some((mission) => mission.msgId === launched.msgId));
  });

  it("reports own transport loss as connection loss without cancelling remote work", { timeout: 5000 }, async (t) => {
    const { lead, broker } = await isolatedWaitFixture(t);
    await lead.send({ to: "bob", message: "remote work remains", awaitReply: true, block: false });
    const waiting = lead.waitAll(2000);
    broker.state.peers.get("lead")!.socket.destroy();
    const result = await waiting;
    assert.equal(result.status, "error");
    assert.equal(result.reason, "connection_lost");
    assert.equal(result.total, 1);
    assert.equal(result.missing.length, 1);
    assert.equal(lead.pendingCount, 1);
  });
});

it("waitAll scopes a target's departure and recovery to the mission room", { timeout: 5000 }, async (t) => {
  const { lead, bob } = await isolatedWaitFixture(t);
  await lead.join("mission-room");
  await bob.join("mission-room");
  const sent = await lead.send({
    to: "bob", room: "mission-room", message: "room-scoped work", awaitReply: true, block: false,
  });
  assert.ok(sent.msgId);
  await bob.leave("mission-room");
  await waitFor(() => lead.missionStatus().some((mission) => mission.reason === "target_offline"));
  assert.equal(lead.knowsPeer("bob"), true, "bob remains online in default");
  const unavailable = await lead.waitAll(2000);
  assert.equal(unavailable.status, "error");
  assert.equal(unavailable.reason, "target_offline");
  assert.equal(lead.pendingCount, 1);
  await bob.join("mission-room");
  await waitFor(() => lead.missionStatus().every((mission) => mission.reason === undefined));
  const waiting = lead.waitAll(2000);
  await bob.reply(sent.msgId, "room work resumed");
  assert.equal((await waiting).status, "complete");
});

it("waitAll intentional close settles immediately with an honest intact snapshot", { timeout: 5000 }, async (t) => {
  const { lead } = await isolatedWaitFixture(t);
  const sent = await lead.send({ to: "bob", message: "closing", awaitReply: true, block: false });
  const waiting = lead.waitAll(2000);
  await lead.close();
  const closed = await waiting;
  assert.equal(closed.status, "cancelled");
  assert.equal(closed.reason, "closed");
  assert.equal(closed.total, 1);
  assert.deepEqual(closed.missing, [{ msgId: sent.msgId, to: "bob" }]);
  assert.equal(lead.pendingCount, 0);
});

it("a group owns only its snapshot, not a subsequently launched answer", { timeout: 5000 }, async (t) => {
  const { lead, bob, carol } = await isolatedWaitFixture(t);
  const first = await lead.send({ to: "bob", message: "group member", awaitReply: true, block: false });
  const waiting = lead.waitAll(2000);
  const outside = await lead.send({ to: "carol", message: "outside snapshot", awaitReply: true, block: false });
  assert.ok(first.msgId && outside.msgId);
  const inbound: MeshFrame[] = [];
  lead.on("inbound", (frame: MeshFrame) => inbound.push(frame));
  await carol.reply(outside.msgId, "outside answer");
  const interrupted = await waiting;
  assert.equal(interrupted.status, "interrupted");
  assert.equal(interrupted.total, 1);
  assert.deepEqual(interrupted.missing, [{ msgId: first.msgId, to: "bob" }]);
  await waitFor(() => inbound.length === 1);
  assert.equal(inbound[0]!.body, "outside answer");
  const again = lead.waitAll(2000);
  await bob.reply(first.msgId, "group answer");
  const complete = await again;
  assert.equal(complete.status, "complete");
  assert.equal(complete.answered, 2);
  assert.equal(inbound.length, 1, "answers carried by the new snapshot are not injected");
});

it("a matched answer releases unrelated sends while its owning group aggregates normally", { timeout: 5000 }, async (t) => {
  const { lead, bob, carol } = await isolatedWaitFixture(t);
  const first = lead.send({ to: "bob", message: "first blocking", awaitReply: true });
  const second = lead.send({ to: "carol", message: "second blocking", awaitReply: true });
  await waitFor(() => lead.pendingCount === 2 && bob.transcript.some((frame) => frame.body === "first blocking"));
  const firstId = lead.missionStatus().find((mission) => mission.to === "bob")!.msgId;
  const secondId = lead.missionStatus().find((mission) => mission.to === "carol")!.msgId;
  const inbound: MeshFrame[] = [];
  lead.on("inbound", (frame: MeshFrame) => inbound.push(frame));
  const aggregate = lead.waitAll(2000);
  await bob.reply(firstId, "first result");
  const own = await first;
  assert.equal(own.status, "reply");
  const other = await second;
  assert.equal(other.status, "interrupted");
  if (other.status === "interrupted") {
    assert.equal(other.msgId, secondId);
    assert.equal(other.tracked, true);
  }
  await waitFor(() => carol.peekInbox(secondId) !== undefined);
  await carol.reply(secondId, "second result");
  const complete = await aggregate;
  assert.equal(complete.status, "complete");
  assert.equal(complete.answered, 2);
  assert.deepEqual(inbound, [], "the owning send/group results carry both answers");
});

it("single send connection loss yields promptly and its remote answer survives reconnect", { timeout: 5000 }, async (t) => {
  const { lead, bob, broker } = await isolatedWaitFixture(t);
  let acknowledgedId: string | undefined;
  lead.on("frame", (frame: MeshFrame) => {
    if (frame.type === "ack" && frame.status === "delivered") acknowledgedId = frame.id;
  });
  const sending = lead.send({ to: "bob", message: "survive connection loss", awaitReply: true });
  await waitFor(() => bob.transcript.some((frame) => frame.body === "survive connection loss"));
  const id = lead.missionStatus()[0]!.msgId;
  await waitFor(() => acknowledgedId === id);
  broker.state.peers.get("lead")!.socket.destroy();
  const result = await sending;
  assert.equal(result.status, "error");
  if (result.status === "error") {
    assert.equal(result.reason, "connection_lost");
    assert.equal(result.delivery, "delivered");
    assert.equal(result.tracked, true);
  }
  assert.equal(lead.pendingCount, 1);
  await lead.connect();
  const aggregate = lead.waitAll(2000);
  await bob.reply(id, "remote work completed");
  const complete = await aggregate;
  assert.equal(complete.status, "complete");
  assert.deepEqual(complete.answers, [{ msgId: id, to: "bob", response: "remote work completed" }]);
});
