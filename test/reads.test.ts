// test/reads.test.ts — D34: read receipts (honest delivered ≠ read).
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { readFileSync, rmSync } from "node:fs";
import type { RunningBroker } from "../src/broker/broker.js";
import { MeshClient, type SendOpts } from "../src/client/client.js";
import { attachClientListeners } from "../src/extension/attach.js";
import { MeshGuards } from "../src/extension/guards.js";
import { MeshLedger } from "../src/extension/ledger.js";
import type { ExtensionAPI, InboundMessage, SendMessageOptions, SessionContext } from "../src/extension/pi-types.js";
import type { MeshRuntime } from "../src/extension/tools.js";
import { MeshTranscript } from "../src/extension/transcript.js";
import type { MeshConfig } from "../src/shared/config.js";
import { MeshIdentity } from "../src/shared/identity-store.js";
import type { MeshFrame } from "../src/protocol/envelope.js";
import { makeTempDirs, startTestBroker, waitFor, sleep, type TempDirs } from "./helpers.js";

describe("read receipts (D34)", () => {
  let dirs: TempDirs;
  let broker: RunningBroker;
  let lead: MeshClient;
  let bob: MeshClient;

  before(async () => {
    dirs = makeTempDirs("mesh-reads-");
    broker = await startTestBroker(dirs.runtimeDir);
    lead = new MeshClient({ alias: "lead", runtimeDir: dirs.runtimeDir });
    bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir });
    await Promise.all([lead.connect(), bob.connect()]);
  });

  after(async () => {
    await lead.close().catch(() => {});
    await bob.close().catch(() => {});
    await broker.close();
    rmSync(dirs.root, { recursive: true, force: true });
  });

  it("the recipient's read receipt reaches the original sender", async () => {
    const msgP = new Promise<MeshFrame>((resolve) => {
      bob.once("inbound", (f: MeshFrame) => resolve(f));
    });
    const sent = await lead.send({ to: "bob", message: "mission X" });
    assert.equal(sent.status, "delivered");
    const msg = await msgP;

    // bob acknowledges reading it
    bob.sendRead(msg.id, "lead");
    await waitFor(() => (lead.readsOf(msg.id).length > 0 ? true : undefined));
    const reads = lead.readsOf(msg.id);
    assert.equal(reads[0]?.alias, "bob");
    assert.ok(reads[0]?.at);
  });

  it("reads are tracked per msgId and exposed via readReceipts()", async () => {
    const msgP = new Promise<MeshFrame>((resolve) => {
      bob.once("inbound", (f: MeshFrame) => resolve(f));
    });
    const sent = await lead.send({ to: "bob", message: "mission Y" });
    assert.ok(sent.msgId);
    const msg = await msgP;
    bob.sendRead(msg.id, "lead");
    await waitFor(() => (lead.readsOf(msg.id).length > 0 ? true : undefined));
    const receipts = lead.readReceipts(10);
    assert.ok(receipts.some((r) => r.msgId === msg.id && r.alias === "bob"));
  });

  it("sendRead when offline is a no-op (no crash)", async () => {
    await bob.close(); // bob offline now
    bob.sendRead("m_ghost", "lead"); // no-op, never throws
    assert.equal(lead.readsOf("m_ghost").length, 0);
  });
});

async function attachedFixture(config: Partial<MeshConfig> = {}) {
  const dirs = makeTempDirs("mesh-attached-reads-");
  const broker = await startTestBroker(dirs.runtimeDir, {
    policy: {
      forceAllowedFrom: ["lead"],
      rateLimits: { msgPerMin: 100, urgentPerMin: 100, forcePerMin: 100 },
    },
  });
  const lead = new MeshClient({ alias: "lead", runtimeDir: dirs.runtimeDir });
  const bob = new MeshClient({
    alias: "bob", runtimeDir: dirs.runtimeDir,
    config: { inboundBatchMs: 30, inboundBatchMaxHoldMs: 5_000, ...config },
  });
  await Promise.all([lead.connect(), bob.connect()]);
  const control = { idle: false, current: true, failing: false, attempts: 0, aborts: 0 };
  const sent: { message: InboundMessage; options?: SendMessageOptions }[] = [];
  const received: MeshFrame[] = [];
  const reads: MeshFrame[] = [];
  lead.on("frame", (frame: MeshFrame) => {
    if (frame.type === "read") reads.push(frame);
  });
  const ctx: SessionContext = {
    cwd: dirs.root,
    ui: { notify: () => {}, setWidget: () => {}, setStatus: () => {} },
    isIdle: () => control.idle,
    abort: () => { control.aborts += 1; },
  };
  const rt: MeshRuntime = {
    client: bob, ctx, stateDir: dirs.stateDir, runtimeDir: dirs.runtimeDir,
    sessionId: "attached", identity: new MeshIdentity(dirs.stateDir),
    guards: new MeshGuards(), ledger: new MeshLedger(dirs.stateDir),
    transcript: new MeshTranscript(dirs.stateDir, false),
    startedAt: Date.now(), ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0,
  };
  const pi: ExtensionAPI = {
    on: () => {}, registerTool: () => {}, registerCommand: () => {}, appendEntry: () => {},
    sendMessage: (message, options) => {
      control.attempts += 1;
      if (control.failing) throw new Error("host cannot accept the message");
      sent.push({ message, options });
    },
  };
  attachClientListeners(pi, rt, () => null, ctx, bob, () => {}, undefined, () => control.current);
  bob.on("inbound", (frame: MeshFrame) => { received.push(frame); });
  return {
    dirs, lead, bob, control, sent, received, reads, rt, pi, ctx,
    receive: async (message: string, options: Partial<SendOpts> = {}) => {
      const result = await lead.send({ to: options.broadcast === true ? undefined : "bob", message, ...options });
      assert.ok(result.status === "delivered");
      await waitFor(() => received.some((frame) => frame.id === result.msgId));
      return result.msgId;
    },
    close: async () => {
      rt.markDetached?.();
      await Promise.all([lead.close(), bob.close()]);
      await broker.close();
      dirs.cleanup();
    },
  };
}

describe("read receipts through the attached host", { timeout: 10_000 }, () => {
  it("a busy batch and a provider-held message are read only after host injection, with hash-only history", async () => {
    const host = await attachedFixture();
    try {
      const first = await host.receive("first accepted batch payload");
      const second = await host.receive("second accepted batch payload");
      await sleep(60); // The batching window elapsed, but the host remains busy.
      assert.equal(host.sent.length, 0);
      assert.equal(host.reads.length, 0);
      assert.deepEqual(host.rt.ledger.read(), []);
      host.control.idle = true;
      host.rt.batcher?.flushNow();
      await waitFor(() => host.reads.length === 2);
      assert.equal(host.sent.length, 1);
      assert.match(host.sent[0]!.message.content, /first accepted batch payload/);
      assert.match(host.sent[0]!.message.content, /second accepted batch payload/);
      assert.deepEqual(host.reads.map((frame) => frame.reads), [first, second]);
      assert.equal(host.lead.readsOf(first)[0]?.alias, "bob");
      assert.equal(host.lead.readsOf(second)[0]?.alias, "bob");
      assert.deepEqual(host.rt.ledger.read().map((record) => record.id), [first, second]);
      const durable = readFileSync(host.rt.ledger.path, "utf8");
      assert.ok(!durable.includes("accepted batch payload"), "message bodies never enter the ledger");
      assert.ok(host.rt.ledger.read().every((record) => /^[a-f0-9]{64}$/.test(record.bodyHash ?? "")));

      host.rt.rateLimitedUntil = Date.now() + 60_000;
      const held = await host.receive("provider-held payload");
      await sleep(30);
      assert.equal(host.sent.length, 1);
      assert.deepEqual(host.lead.readsOf(held), []);
      host.rt.rateLimitedUntil = undefined;
      host.rt.flushHeld?.();
      host.rt.batcher?.flushNow();
      await waitFor(() => host.reads.length === 3);
      host.rt.flushHeld?.();
      host.rt.batcher?.flushNow();
      await sleep(30);
      assert.deepEqual(host.reads.map((frame) => frame.reads), [first, second, held]);
      assert.equal(host.sent.length, 2);
      assert.equal(host.rt.ledger.read().length, 3);

      host.control.idle = false;
      const failed = await host.receive("batch rejected by host");
      host.control.failing = true;
      host.rt.batcher?.flushNow();
      host.control.failing = false;
      const stale = await host.receive("batch abandoned by stale session");
      host.control.current = false;
      host.rt.batcher?.flushNow();
      await sleep(30);
      assert.equal(host.sent.length, 2);
      assert.deepEqual(host.reads.map((frame) => frame.reads), [first, second, held]);
      assert.equal(host.rt.ledger.read().length, 3);
      assert.deepEqual(host.lead.readsOf(failed), []);
      assert.deepEqual(host.lead.readsOf(stale), []);
    } finally {
      await host.close();
    }
  });

  it("deferred flush and cancelled prompt preflight produce one read per accepted message, never for a failed send", async () => {
    const host = await attachedFixture({ inboundBatchMs: 0, inboundBroadcasts: "deferred" });
    try {
      const first = await host.receive("unaddressed update one", { broadcast: true });
      const second = await host.receive("unaddressed update two", { broadcast: true });
      const inbox = host.rt.deferredInbox!;
      assert.equal(inbox.count, 2);
      assert.equal(host.reads.length, 0);
      assert.deepEqual(host.rt.ledger.read(), []);
      host.control.failing = true;
      assert.throws(() => inbox.flush(), /host cannot accept/);
      assert.equal(inbox.count, 2);
      await sleep(30);
      assert.equal(host.reads.length, 0);
      assert.deepEqual(host.rt.ledger.read(), []);
      host.control.failing = false;
      assert.equal(inbox.flush(), 2);
      await waitFor(() => host.reads.length === 2);
      assert.equal(host.sent.length, 1);
      assert.deepEqual(host.sent[0]!.options, { deliverAs: "followUp", triggerTurn: true });
      inbox.queueForPrompt();
      inbox.promptStarting();
      inbox.agentStarting();
      assert.equal(inbox.flush(), 0);

      const third = await host.receive("unaddressed next prompt update", { broadcast: true });
      host.control.current = false;
      assert.throws(() => inbox.queueForPrompt(), /no longer active/);
      assert.equal(inbox.count, 1);
      assert.deepEqual(host.lead.readsOf(third), []);
      host.control.current = true;
      host.control.failing = true;
      assert.throws(() => inbox.queueForPrompt(), /host cannot accept/);
      assert.equal(inbox.count, 1);
      assert.deepEqual(host.lead.readsOf(third), []);
      host.control.failing = false;
      inbox.queueForPrompt();
      await waitFor(() => host.reads.length === 3);
      assert.deepEqual(host.sent[1]!.options, { deliverAs: "nextTurn" });
      inbox.agentStarting(); // A cancelled preflight does not consume nextTurn.
      inbox.queueForPrompt();
      assert.equal(inbox.flush(), 0);
      assert.equal(inbox.count, 1);
      inbox.promptStarting();
      inbox.agentStarting();
      await sleep(30);
      assert.equal(inbox.count, 0);
      assert.equal(host.sent.length, 2);
      assert.deepEqual(host.reads.map((frame) => frame.reads), [first, second, third]);
      assert.deepEqual(host.rt.ledger.read().map((record) => record.id), [first, second, third]);
    } finally {
      await host.close();
    }
  });

  it("abandoning a graph discards busy, deferred and provider-held traffic without claiming reads", async () => {
    const host = await attachedFixture({ inboundBroadcasts: "deferred" });
    try {
      const batched = await host.receive("old graph busy batch");
      const deferred = await host.receive("old graph unaddressed update", { broadcast: true });
      host.rt.rateLimitedUntil = Date.now() + 60_000;
      const held = await host.receive("old graph provider hold");
      host.rt.clearInbound?.();
      host.rt.rateLimitedUntil = undefined;
      host.control.idle = true;
      host.rt.flushHeld?.();
      host.rt.batcher?.flushNow();
      host.rt.deferredInbox?.queueForPrompt();
      assert.equal(host.rt.deferredInbox?.flush(), 0);
      await sleep(60);
      assert.equal(host.sent.length, 0);
      assert.equal(host.reads.length, 0);
      assert.deepEqual(host.rt.ledger.read(), []);
      const current = await host.receive("current graph message");
      host.rt.batcher?.flushNow();
      await waitFor(() => host.reads.length === 1);
      assert.equal(host.sent.length, 1);
      assert.ok(host.sent[0]!.message.content.includes("current graph message"));
      assert.deepEqual(host.reads.map((frame) => frame.reads), [current]);
      for (const abandoned of [batched, deferred, held]) assert.deepEqual(host.lead.readsOf(abandoned), []);
    } finally {
      await host.close();
    }
  });

  it("failed, stale and detached hosts cannot claim delivery; retry and delayed force succeed exactly once", async () => {
    const host = await attachedFixture({ inboundBatchMs: 0 });
    try {
      host.control.failing = true;
      const retry = await host.receive("retry after failed host injection");
      assert.equal(host.sent.length, 0);
      assert.equal(host.reads.length, 0);
      assert.deepEqual(host.rt.ledger.read(), []);
      host.control.failing = false;
      await waitFor(() => host.reads.length === 1);
      assert.equal(host.sent.length, 1);
      assert.equal(host.rt.ledger.read().length, 1);

      host.control.current = false;
      const attempts = host.control.attempts;
      const stale = await host.receive("stale session must not receive this");
      assert.equal(host.control.attempts, attempts);
      host.control.current = true;
      const staleForce = await host.receive("session replaced during force wait", { priority: "force", reason: "host busy" });
      assert.equal(host.sent.length, 1);
      host.control.current = false;
      await sleep(100);
      assert.equal(host.sent.length, 1);
      assert.deepEqual(host.lead.readsOf(staleForce), []);

      host.control.current = true;
      const force = await host.receive("force after host settles", { priority: "force", reason: "host busy" });
      assert.deepEqual(host.lead.readsOf(force), []);
      assert.equal(host.sent.length, 1);
      host.control.idle = true;
      await waitFor(() => host.reads.length === 2);
      assert.equal(host.sent.length, 2);
      assert.deepEqual(host.sent[1]!.options, { triggerTurn: true, deliverAs: "steer" });

      host.control.idle = false;
      const interrupt = await host.receive("interrupt after host settles", { priority: "force", interrupt: true, reason: "host busy" });
      assert.deepEqual(host.lead.readsOf(interrupt), []);
      host.control.idle = true;
      await waitFor(() => host.reads.some((frame) => frame.reads === interrupt));
      assert.equal(host.rt.interruptCount, 1);
      assert.equal(host.rt.lastInterruptOutcome, "aborted");

      host.control.idle = false;
      const detached = await host.receive("detached force must never be read", { priority: "force", interrupt: true, reason: "host busy" });
      host.rt.markDetached?.();
      await sleep(100);
      assert.equal(host.sent.length, 3);
      assert.deepEqual(host.reads.map((incoming) => incoming.reads), [retry, force, interrupt]);
      assert.deepEqual(host.rt.ledger.read().map((record) => record.id), [retry, force, interrupt]);
      assert.deepEqual(host.lead.readsOf(stale), []);
      assert.deepEqual(host.lead.readsOf(detached), []);
      assert.equal(host.rt.interruptCount, 1, "a detached send must not issue a delivered interrupt report");
    } finally {
      await host.close();
    }
  });
});
