// test/wait-all.test.ts — D42: mesh_wait_all (group await with honest summary).
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { rmSync } from "node:fs";
import { MeshClient } from "../src/client/client.js";
import type { MeshFrame } from "../src/protocol/envelope.js";
import { makeTempDirs, startTestBroker, waitFor, type TempDirs } from "./helpers.js";

describe("waitAll (D42)", () => {
  let dirs: TempDirs;
  let broker: Awaited<ReturnType<typeof startTestBroker>>;
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
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") void carol.reply(f.id, "carol launch done");
    });
    const t0 = Date.now();
    const r1 = await lead.send({ to: "carol", message: "fast", awaitReply: true, block: false, timeoutMs: 30_000 });
    const r2 = await lead.send({ to: "bob", message: "slow", awaitReply: true, block: false, timeoutMs: 30_000 });
    assert.equal(r1.status, "delivered");
    assert.equal(r2.status, "delivered");
    assert.ok(Date.now() - t0 < 2000, "launch must NOT block for the replies");
    const res = await lead.waitAll(2500);
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
      assert.equal(verdict.status, "timeout");
      assert.equal(verdict.total, 2);
      assert.deepEqual(verdict.missing, [
        { msgId: queued.msgId, to: "bob" },
        { msgId: blocked.msgId, to: "unknown-recipient" },
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


