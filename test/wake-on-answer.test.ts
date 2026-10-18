// test/wake-on-answer.test.ts — LAUNCH answers are delivered to the session
// (inbox + inbound event, waking an idle session); a wait_all in flight
// carries them instead (no double delivery); a blocking send is ESC-cancellable
// and a late reply to a cancelled mission takes the orphan path.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { rmSync } from "node:fs";
import type { Socket } from "node:net";
import { MeshClient } from "../src/client/client.js";
import type { MeshFrame } from "../src/protocol/envelope.js";
import { registerTools, type MeshRuntime } from "../src/extension/tools.js";
import { attachClientListeners } from "../src/extension/attach.js";
import { MeshLedger } from "../src/extension/ledger.js";
import { MeshTranscript } from "../src/extension/transcript.js";
import { MeshGuards } from "../src/extension/guards.js";
import { MeshIdentity } from "../src/shared/identity-store.js";
import type { ExtensionAPI, InboundMessage, SendMessageOptions, SessionContext, ToolDefinition, ToolResult } from "../src/extension/pi-types.js";
import { makeTempDirs, sleep, startTestBroker, waitFor, type TempDirs } from "./helpers.js";

describe("wake-on-answer (LAUNCH missions)", () => {
  let dirs: TempDirs;
  let broker: Awaited<ReturnType<typeof startTestBroker>>;
  let lead: MeshClient;
  let carol: MeshClient;
  let bob: MeshClient;

  before(async () => {
    dirs = makeTempDirs("mesh-wake-");
    broker = await startTestBroker(dirs.runtimeDir);
    lead = new MeshClient({ alias: "lead", runtimeDir: dirs.runtimeDir });
    carol = new MeshClient({ alias: "carol", runtimeDir: dirs.runtimeDir });
    bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir });
    await Promise.all([lead.connect(), carol.connect(), bob.connect()]);
  });

  after(async () => {
    await lead.close().catch(() => {});
    await carol.close().catch(() => {});
    await bob.close().catch(() => {});
    await broker.close();
    rmSync(dirs.root, { recursive: true, force: true });
  });

  function resetPeers(): void {
    carol.removeAllListeners("inbound");
    bob.removeAllListeners("inbound");
  }

  it("a LAUNCH answer is stored in the inbox and emitted as inbound (wakes the session)", async () => {
    lead.cancelAllAwaited();
    resetPeers();
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") void carol.reply(f.id, "carol answer");
    });
    const sent = await lead.send({ to: "carol", message: "wake me", awaitReply: true, block: false, timeoutMs: 30_000 });
    assert.equal(sent.status, "delivered");

    let matchedReply = false;
    const injected = await new Promise<MeshFrame>((resolve) => {
      lead.once("inbound", (frame: MeshFrame, meta?: { matchedReply?: boolean }) => {
        matchedReply = meta?.matchedReply === true;
        resolve(frame);
      });
    });
    assert.equal(matchedReply, true, "LAUNCH wake metadata bypasses broadcast deferral");
    assert.equal(injected.type, "reply");
    assert.equal(injected.replyTo, sent.msgId); // correlates the mission
    assert.equal(injected.body, "carol answer");
    // the answer frame itself is replyable (inbox, not just an event)
    assert.ok(lead.peekInbox(injected.id ?? "") !== undefined);
    // and the mission is answered as before
    const m = lead.missionStatus().find((x) => x.msgId === sent.msgId);
    assert.ok(m !== undefined && m.answered);
    lead.removeAllListeners("inbound");
  });

  it("a BLOCKING answer resolves the send promise and does NOT emit inbound", async () => {
    lead.cancelAllAwaited();
    resetPeers();
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") void carol.reply(f.id, "blocking answer");
    });
    let inboundCount = 0;
    lead.on("inbound", () => { inboundCount += 1; });
    const res = await lead.send({ to: "carol", message: "block me", awaitReply: true, timeoutMs: 30_000 });
    assert.equal(res.status, "reply");
    assert.equal(res.response, "blocking answer");
    assert.equal(inboundCount, 0, "blocking answers stay in the tool result");
    lead.removeAllListeners("inbound");
  });

  it("while wait_all is in flight the verdict carries the batch — no inbound; after it, injection resumes", async () => {
    lead.cancelAllAwaited();
    resetPeers();
    let carolGot: MeshFrame | undefined;
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") carolGot = f;
    });
    let inboundCount = 0;
    lead.on("inbound", () => { inboundCount += 1; });

    // batch 1: answer arrives WHILE wait_all is running
    const sent = await lead.send({ to: "carol", message: "during wait", awaitReply: true, block: false, timeoutMs: 30_000 });
    const verdictP = lead.waitAll(10_000);
    await waitFor(() => (carolGot !== undefined ? true : undefined));
    const during = carolGot as MeshFrame | undefined;
    assert.ok(during !== undefined);
    await carol.reply(during.id, "answered mid-wait");
    const verdict = await verdictP;
    assert.equal(verdict.status, "complete");
    assert.ok(verdict.answers.some((a) => a.response === "answered mid-wait"));
    assert.equal(inboundCount, 0, "no double delivery while wait_all is active");

    // batch 2: after the verdict, injection resumes
    carolGot = undefined;
    const sent2 = await lead.send({ to: "carol", message: "after wait", awaitReply: true, block: false, timeoutMs: 30_000 });
    assert.notEqual(sent2.msgId, sent.msgId);
    await waitFor(() => (carolGot !== undefined ? true : undefined));
    const after = carolGot as MeshFrame | undefined;
    assert.ok(after !== undefined);
    await carol.reply(after.id, "answered after wait");
    await waitFor(() => (inboundCount > 0 ? true : undefined));
    assert.equal(inboundCount, 1, "post-verdict answers are injected again");
    lead.removeAllListeners("inbound");
  });

  it("wait_all returns the complete multiline answer to the model without a duplicate inbound", async () => {
    lead.cancelAllAwaited();
    resetPeers();
    let original: MeshFrame | undefined;
    carol.on("inbound", (frame: MeshFrame) => {
      if (frame.type === "msg") original = frame;
    });
    let inboundCount = 0;
    const onInbound = (): void => { inboundCount += 1; };
    lead.on("inbound", onInbound);
    try {
      const tools = new Map<string, ToolDefinition>();
      registerTools({
        registerTool: (tool: ToolDefinition) => tools.set(tool.name, tool),
      } as unknown as ExtensionAPI, () => ({ client: lead }) as MeshRuntime);
      await lead.send({
        to: "carol", message: "complete multiline report", awaitReply: true, block: false, timeoutMs: 30_000,
      });
      await waitFor(() => original !== undefined);
      const waiting = tools.get("mesh_wait_all")!.execute(
        "complete-report", { timeoutMs: 2000 }, undefined, undefined, {} as SessionContext,
      );
      const body = `${"findings ".repeat(40)}\n\`\`\`ts\nconst answer = 42;\n\`\`\`\nFINAL_CRITICAL_RESULT`;
      assert.equal((await carol.reply(original!.id, body)).status, "delivered");
      const result = await waiting;
      assert.ok(result.content.some((part) => part.text.includes(body)));
      assert.deepEqual(result.details?.answers, [{
        msgId: original!.id, to: "carol", response: body,
      }]);
      assert.equal(inboundCount, 0);
      assert.equal((await lead.waitAll(50)).total, 0);
    } finally {
      lead.removeListener("inbound", onInbound);
    }
  });

  it("an identical re-sent answer after wake injection is deduped; a different body still passes", async () => {
    lead.cancelAllAwaited();
    resetPeers();
    let carolGot: MeshFrame | undefined;
    carol.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") carolGot = f;
    });
    let inboundCount = 0;
    lead.on("inbound", () => { inboundCount += 1; });

    const sent = await lead.send({ to: "carol", message: "dedup me", awaitReply: true, block: false, timeoutMs: 30_000 });
    await waitFor(() => (carolGot !== undefined ? true : undefined));
    const msg = carolGot as MeshFrame | undefined;
    assert.ok(msg !== undefined);
    await carol.reply(msg.id, "the answer");
    await waitFor(() => (inboundCount === 1 ? true : undefined));

    // identical re-send (e.g. the peer re-answering on a remind) → dropped
    await carol.reply(msg.id, "the answer");
    await sleep(300);
    assert.equal(inboundCount, 1, "exact duplicate of an injected answer is dropped");

    // a DIFFERENT answer to the same msgId (ack then final report) still passes
    await carol.reply(msg.id, "correction: actually 42");
    await waitFor(() => (inboundCount === 2 ? true : undefined));
    lead.removeAllListeners("inbound");
  });

  it("ESC (AbortSignal) cancels a blocking send; the mission is dropped and a late reply is injected", async () => {
    lead.cancelAllAwaited();
    resetPeers();
    // bob answers LATE — after the abort
    bob.on("inbound", (f: MeshFrame) => {
      if (f.type === "msg") setTimeout(() => void bob.reply(f.id, "late bob"), 600).unref();
    });
    const ac = new AbortController();
    const t0 = Date.now();
    const p = lead.send({ to: "bob", message: "esc me", awaitReply: true, timeoutMs: 30_000, signal: ac.signal });
    setTimeout(() => ac.abort(), 250).unref();
    const res = await p;
    assert.equal(res.status, "error");
    assert.equal(res.reason, "aborted");
    assert.ok(Date.now() - t0 < 5000, "settles at the abort, not the 30-min timeout");
    assert.equal(lead.pendingCount, 0, "pending cancelled");
    const m = lead.missionStatus().find((x) => x.msgId === res.msgId);
    assert.equal(m, undefined, "cancelled mission is dropped, never 'waiting'");

    // the late reply arrives → orphan path → injected
    const injected = await new Promise<MeshFrame>((resolve) => {
      lead.once("inbound", resolve);
    });
    assert.equal(injected.type, "reply");
    assert.equal(injected.replyTo, res.msgId);
    assert.equal(injected.body, "late bob");
    lead.removeAllListeners("inbound");
  });
});

describe("actionable inbound messages during passive waits", () => {
  async function fixture(options: { rejectAttempts?: number; rejectType?: string; batchMs?: number; allowForce?: boolean } = {}) {
    const dirs = makeTempDirs("mesh-wait-inbound-");
    const broker = await startTestBroker(dirs.runtimeDir, options.allowForce ? { policy: { forceAllowedFrom: ["bob"] } } : {});
    const lead = new MeshClient({
      alias: "lead", runtimeDir: dirs.runtimeDir, noReconnect: true,
      config: options.batchMs === undefined
        ? { inboundBroadcasts: "deferred" }
        : { inboundBroadcasts: "deferred", inboundBatchMs: options.batchMs },
    });
    const carol = new MeshClient({ alias: "carol", runtimeDir: dirs.runtimeDir, noReconnect: true });
    const bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir, noReconnect: true });
    await Promise.all([lead.connect(), carol.connect(), bob.connect()]);
    const tools = new Map<string, ToolDefinition>();
    const injected: { message: InboundMessage; options?: SendMessageOptions }[] = [];
    let aborts = 0;
    let injectionAttempts = 0;
    let rejections = options.rejectAttempts ?? 0;
    const ctx = {
      cwd: dirs.root, isIdle: () => false, abort: () => { aborts += 1; },
      ui: { notify: () => {}, setStatus: () => {}, setWidget: () => {} },
    } as unknown as SessionContext;
    const rt = {
      client: lead, ctx, sessionId: "wait-inbound",
      ledger: new MeshLedger(dirs.stateDir),
      transcript: new MeshTranscript(dirs.stateDir, false),
      guards: new MeshGuards(), identity: new MeshIdentity(dirs.stateDir),
      stateDir: dirs.stateDir, runtimeDir: dirs.runtimeDir, startedAt: Date.now(),
      ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0,
    } as MeshRuntime;
    const pi = {
      registerTool: (tool: ToolDefinition) => tools.set(tool.name, tool),
      sendMessage: (message: InboundMessage, deliveryOptions?: SendMessageOptions) => {
        if (message.customType === (options.rejectType ?? "mesh-inbound")) {
          injectionAttempts += 1;
          if (rejections > 0) {
            rejections -= 1;
            throw new Error("transient host transition");
          }
        }
        injected.push({ message, options: deliveryOptions });
      },
      appendEntry: () => {},
    } as unknown as ExtensionAPI;
    registerTools(pi, () => rt);
    attachClientListeners(pi, rt, () => null, ctx, lead, () => {});
    return {
      lead, carol, bob, rt, tools, injected, ctx, broker, dirs,
      abortCount: () => aborts,
      injectionAttempts: () => injectionAttempts,
      async cleanup() {
        rt.markDetached?.();
        await Promise.all([lead.close(), carol.close(), bob.close()]);
        await broker.close();
        dirs.cleanup();
      },
    };
  }

  for (const priority of ["normal", "urgent"] as const) {
    it(`${priority} input yields the wait without aborting or consuming partial answers`, { timeout: 10_000 }, async () => {
      const f = await fixture();
      try {
        const first = await f.lead.send({ to: "carol", message: "first task", awaitReply: true, block: false, timeoutMs: 5000 });
        const second = await f.lead.send({ to: "bob", message: "second task", awaitReply: true, block: false, timeoutMs: 5000 });
        assert.equal(first.status, "delivered");
        assert.equal(second.status, "delivered");
        await waitFor(() => f.carol.peekInbox(first.msgId!) !== undefined && f.bob.peekInbox(second.msgId!) !== undefined);
        let result: ToolResult | undefined;
        const waiting = f.tools.get("mesh_wait_all")!.execute("wait", { timeoutMs: 5000 }, undefined, undefined, f.ctx)
          .then((value) => { result = value; return value; });
        await f.carol.reply(first.msgId!, "partial report");
        await waitFor(() => f.lead.missionStatus().some((mission) => mission.msgId === first.msgId && mission.answered));
        const incoming = await f.carol.send({ to: "lead", message: "new actionable input", priority });
        await waitFor(() => result !== undefined, 2000);
        const interrupted = await waiting;
        assert.equal(interrupted.details?.status, "interrupted");
        assert.equal(interrupted.details?.reason, "inbound_message");
        assert.equal(interrupted.details?.interruptedBy, incoming.msgId);
        assert.deepEqual(interrupted.details?.answers, [{ msgId: first.msgId, to: "carol", response: "partial report" }]);
        const missing = interrupted.details?.missing as { msgId: string; to: string }[];
        assert.deepEqual(missing.map(({ msgId, to }) => ({ msgId, to })), [{ msgId: second.msgId, to: "bob" }]);
        const delivery = f.injected.find((item) => item.message.content.includes("new actionable input"));
        assert.equal(delivery?.options?.deliverAs, "steer", "input must be visible at the next model call, not after the turn");
        assert.equal(delivery?.options?.triggerTurn, true);
        assert.equal(f.abortCount(), 0);
        assert.equal(f.lead.pendingCount, 1);
        await f.bob.reply(second.msgId!, "remaining report");
        const resumed = await f.lead.waitAll(2000);
        assert.equal(resumed.status, "complete");
        assert.deepEqual(resumed.answers, [
          { msgId: first.msgId, to: "carol", response: "partial report" },
          { msgId: second.msgId, to: "bob", response: "remaining report" },
        ]);
      } finally {
        await f.cleanup();
      }
    });
  }

  for (const priority of ["normal", "urgent"] as const) {
    it(`${priority} input yields mesh_send and its later reply still wakes the session`, { timeout: 10_000 }, async () => {
      const f = await fixture();
      try {
        const oldSignal = new AbortController();
        let result: ToolResult | undefined;
        const sending = f.tools.get("mesh_send")!.execute("single-wait", {
          to: "bob", message: "single pending task", awaitReply: true, timeoutMs: 5000,
        }, oldSignal.signal, undefined, f.ctx).then((value) => { result = value; return value; });
        let original: MeshFrame | undefined;
        await waitFor(() => {
          original = f.bob.transcript.find((frame) => frame.type === "msg" && frame.body === "single pending task");
          return original !== undefined;
        });
        const incoming = await f.carol.send({ to: "lead", message: "handle new directive", priority });
        await waitFor(() => result !== undefined, 2000);
        const yielded = await sending;
        assert.equal(yielded.details?.status, "interrupted");
        assert.equal(yielded.details?.interruptedBy, incoming.msgId);
        assert.equal(yielded.details?.tracked, true);
        assert.equal(yielded.details?.msgId, original!.id);
        assert.equal(f.injected.find((item) => item.message.content.includes("handle new directive"))?.options?.deliverAs, "steer");
        oldSignal.abort();
        assert.equal(f.lead.pendingCount, 1);
        assert.equal(f.abortCount(), 0);
        f.ctx.isIdle = () => true;
        await f.bob.reply(original!.id, "answer after yielding");
        await waitFor(() => f.injected.some((item) => item.message.content.includes("answer after yielding")));
        assert.equal(f.injected.filter((item) => item.message.content.includes("answer after yielding")).length, 1);
        const resumed = await f.lead.waitAll(1000);
        assert.equal(resumed.status, "complete");
        assert.equal(resumed.answers[0]?.response, "answer after yielding");
      } finally {
        await f.cleanup();
      }
    });
  }

  for (const name of ["mesh_status", "mesh_reply", "mesh_reserve", "mesh_release"] as const) {
    it(`${name} yields a delayed control request and later delivers its actual result`, { timeout: 10_000 }, async () => {
      const f = await fixture();
      const socket = f.broker.state.peers.get("lead")!.socket;
      const received: MeshFrame[] = [];
      f.bob.on("inbound", (frame: MeshFrame) => received.push(frame));
      try {
        let params: Record<string, unknown> = {};
        if (name === "mesh_reply") {
          const original = await f.bob.send({ to: "lead", message: "reply to this" });
          await waitFor(() => f.lead.peekInbox(original.msgId!) !== undefined);
          params = { msgId: original.msgId, message: "reply completed after yielding" };
        } else if (name === "mesh_reserve") {
          params = { paths: ["src/yielded.ts"] };
        } else if (name === "mesh_release") {
          await f.lead.reserve(["src/yielded.ts"]);
          params = { paths: ["src/yielded.ts"] };
        }
        socket.pause();
        let result: ToolResult | undefined;
        const pending = f.tools.get(name)!.execute(name, params, undefined, undefined, f.ctx)
          .then((value) => { result = value; return value; });
        const incoming = await f.carol.send({ to: "lead", message: `input during ${name}` });
        await waitFor(() => result !== undefined, 2000);
        const yielded = await pending;
        assert.equal(yielded.details?.status, "interrupted");
        assert.equal(yielded.details?.interruptedBy, incoming.msgId);
        assert.equal(yielded.details?.pendingOperation, true);
        assert.equal(f.rt.pendingToolOperations?.size, 1);
        assert.equal(f.injected.find((item) => item.message.content.includes(`input during ${name}`))?.options?.deliverAs, "steer");
        assert.equal(f.abortCount(), 0);
        socket.resume();
        await waitFor(() => f.injected.some((item) =>
          item.message.customType === "mesh-operation-result" && item.message.details?.operationId === name));
        const completion = f.injected.find((item) => item.message.customType === "mesh-operation-result")!;
        const details = completion.message.details?.result as Record<string, unknown>;
        assert.equal(completion.options?.deliverAs, "steer");
        assert.equal(f.rt.pendingToolOperations?.size, 0);
        if (name === "mesh_status") {
          assert.ok((details.peers as { alias: string }[]).some((peer) => peer.alias === "bob"));
        } else {
          assert.equal(details.status, "delivered");
        }
        if (name === "mesh_reply") {
          await waitFor(() => received.some((frame) => frame.body === "reply completed after yielding"));
          assert.equal(received.filter((frame) => frame.body === "reply completed after yielding").length, 1);
        } else if (name === "mesh_reserve") {
          assert.ok(f.lead.reservations.some((reservation) => reservation.pattern === "src/yielded.ts"));
        } else if (name === "mesh_release") {
          assert.equal(f.lead.reservations.some((reservation) => reservation.pattern === "src/yielded.ts"), false);
        }
      } finally {
        socket.resume();
        await f.cleanup();
      }
    });
  }

  it("queued input during rename yields the alias tool without losing its final identity", { timeout: 10_000 }, async () => {
    const f = await fixture();
    const future = new MeshClient({ alias: "renamed", runtimeDir: f.dirs.runtimeDir, noReconnect: true });
    let heldSocket: Socket | undefined;
    try {
      await future.connect();
      await future.close();
      await waitFor(() => !f.carol.knowsPeer("renamed"));
      const queued = await f.carol.send({ to: "renamed", message: "input during alias handover" });
      assert.equal(queued.status, "queued_offline");
      await f.lead.join("readonly", "observer");
      // Pause reads after the new hello: mailbox delivery is independent of
      // packet coalescing while member-role restoration still awaits its ACK.
      f.broker.server.once("connection", (socket) => {
        heldSocket = socket;
        socket.prependOnceListener("data", () => socket.pause());
      });
      const result = await f.tools.get("mesh_alias")!.execute("rename-operation", { alias: "renamed" }, undefined, undefined, f.ctx);
      assert.equal(result.details?.status, "interrupted");
      assert.equal(f.rt.pendingToolOperations?.size, 1);
      heldSocket!.resume();
      await waitFor(() => f.injected.some((item) => item.message.customType === "mesh-operation-result"));
      const completion = f.injected.find((item) => item.message.customType === "mesh-operation-result")!;
      assert.equal((completion.message.details?.result as Record<string, unknown>).alias, "renamed");
      assert.equal(f.lead.alias, "renamed");
      assert.equal(f.injected.find((item) => item.message.content.includes("input during alias handover"))?.options?.deliverAs, "steer");
      assert.equal(f.abortCount(), 0);
    } finally {
      heldSocket?.resume();
      await future.close();
      await f.cleanup();
    }
  });

  it("a yielded control request never injects its completion into a detached session", { timeout: 10_000 }, async () => {
    const f = await fixture();
    const socket = f.broker.state.peers.get("lead")!.socket;
    try {
      socket.pause();
      const pending = f.tools.get("mesh_status")!.execute("detached-status", {}, undefined, undefined, f.ctx);
      await f.carol.send({ to: "lead", message: "yield then detach" });
      assert.equal((await pending).details?.status, "interrupted");
      f.rt.markDetached?.();
      socket.resume();
      await waitFor(() => f.rt.pendingToolOperations?.size === 0);
      assert.equal(f.injected.some((item) => item.message.customType === "mesh-operation-result"), false);
    } finally {
      socket.resume();
      await f.cleanup();
    }
  });

  for (const mode of ["single", "group"] as const) {
    it(`an unrelated launch answer reaches the model while a ${mode} wait is active`, { timeout: 10_000 }, async () => {
      const f = await fixture();
      try {
        let original: MeshFrame | undefined;
        f.bob.on("inbound", (frame: MeshFrame) => {
          if (frame.body === "different pending task") original = frame;
        });
        let waiting: Promise<ToolResult>;
        if (mode === "single") {
          waiting = f.tools.get("mesh_send")!.execute("different-send", {
            to: "bob", message: "different pending task", awaitReply: true, timeoutMs: 5000,
          }, undefined, undefined, f.ctx);
        } else {
          await f.lead.send({ to: "bob", message: "different pending task", awaitReply: true, block: false, timeoutMs: 5000 });
          waiting = f.tools.get("mesh_wait_all")!.execute("different-group", { timeoutMs: 5000 }, undefined, undefined, f.ctx);
        }
        await waitFor(() => original !== undefined);
        const other = await f.lead.send({ to: "carol", message: "outside the wait snapshot", awaitReply: true, block: false, timeoutMs: 5000 });
        await waitFor(() => f.carol.peekInbox(other.msgId!) !== undefined);
        await f.carol.reply(other.msgId!, "independent report is ready");
        const yielded = await waiting;
        assert.equal(yielded.details?.status, "interrupted");
        const report = f.injected.find((item) => item.message.content.includes("independent report is ready"));
        assert.equal(report?.options?.deliverAs, "steer", "a core-initiated yield must not leave its answer held by busy batching");
        assert.equal(f.lead.pendingCount, 1);
        const resumed = f.lead.waitAll(2000);
        await f.bob.reply(original!.id, "other task finished");
        assert.equal((await resumed).status, "complete");
        assert.equal(f.abortCount(), 0);
      } finally {
        await f.cleanup();
      }
    });
  }

  it("a late control result steers the current wait even if its original tool signal was aborted", { timeout: 10_000 }, async () => {
    const f = await fixture();
    const socket = f.broker.state.peers.get("lead")!.socket;
    try {
      socket.pause();
      const oldSignal = new AbortController();
      const request = f.tools.get("mesh_status")!.execute("old-status", {}, oldSignal.signal, undefined, f.ctx);
      await f.carol.send({ to: "lead", message: "yield the old status request" });
      assert.equal((await request).details?.status, "interrupted");
      oldSignal.abort();
      const current = f.lead.send({ to: "bob", message: "current passive wait", awaitReply: true, timeoutMs: 5000 });
      socket.resume();
      assert.equal((await current).status, "interrupted");
      await waitFor(() => f.injected.some((item) => item.message.customType === "mesh-operation-result"));
      const completion = f.injected.find((item) => item.message.customType === "mesh-operation-result")!;
      assert.equal(completion.options?.deliverAs, "steer");
      const result = completion.message.details?.result as Record<string, unknown>;
      assert.ok((result.peers as { alias: string }[]).some((peer) => peer.alias === "bob"));
      assert.equal(f.abortCount(), 0);
    } finally {
      socket.resume();
      await f.cleanup();
    }
  });

  it("retries a waking directive once instead of silently dropping it", { timeout: 10_000 }, async () => {
    const f = await fixture({ rejectAttempts: 1 });
    try {
      let original: MeshFrame | undefined;
      f.bob.on("inbound", (frame: MeshFrame) => { if (frame.body === "retry task") original = frame; });
      const sending = f.tools.get("mesh_send")!.execute("retry-send", {
        to: "bob", message: "retry task", awaitReply: true, timeoutMs: 5000,
      }, undefined, undefined, f.ctx);
      await waitFor(() => original !== undefined);
      const incoming = await f.carol.send({ to: "lead", message: "directive surviving host rejection" });
      assert.equal((await sending).details?.status, "interrupted");
      assert.equal(f.rt.isInboundReloadSafe?.(), false, "reload must not discard the scheduled retry");
      assert.equal(f.carol.readsOf(incoming.msgId!).length, 0);
      await waitFor(() => f.injected.some((item) => item.message.content.includes("directive surviving host rejection")));
      await waitFor(() => f.carol.readsOf(incoming.msgId!).length === 1);
      assert.equal(f.injectionAttempts(), 2);
      assert.equal(f.rt.injectionFailures, 0);
      assert.equal(f.rt.isInboundReloadSafe?.(), true);
      assert.equal(f.injected.filter((item) => item.message.content.includes("directive surviving host rejection")).length, 1);
    } finally {
      await f.cleanup();
    }
  });

  it("retries a rejected batch atomically and receipts follow actual acceptance", { timeout: 10_000 }, async () => {
    const f = await fixture({ rejectAttempts: 1, batchMs: 5000 });
    try {
      f.ctx.isIdle = () => true;
      const first = await f.bob.send({ to: "lead", message: "first queued directive" });
      const second = await f.bob.send({ to: "lead", message: "second queued directive" });
      await waitFor(() => f.lead.peekInbox(second.msgId!) !== undefined);
      f.rt.batcher!.flushNow();
      assert.equal(f.rt.isInboundReloadSafe?.(), false);
      assert.equal(f.bob.readsOf(first.msgId!).length, 0);
      assert.equal(f.bob.readsOf(second.msgId!).length, 0);
      await waitFor(() => f.injected.some((item) => item.message.content.includes("first queued directive")));
      await waitFor(() => f.bob.readsOf(first.msgId!).length === 1 && f.bob.readsOf(second.msgId!).length === 1);
      const batch = f.injected.find((item) => item.message.content.includes("first queued directive"))!;
      assert.ok(batch.message.content.indexOf("first queued directive") < batch.message.content.indexOf("second queued directive"));
      assert.equal(f.injectionAttempts(), 2);
      assert.equal(f.rt.injectionFailures, 0);
      assert.equal(f.rt.isInboundReloadSafe?.(), true);
    } finally {
      await f.cleanup();
    }
  });

  it("counts a final active-session injection failure and releases retry ownership", { timeout: 10_000 }, async () => {
    const f = await fixture({ rejectAttempts: 2, batchMs: 0 });
    try {
      await f.bob.send({ to: "lead", message: "unavailable host" });
      await waitFor(() => f.rt.injectionFailures === 1);
      assert.equal(f.injectionAttempts(), 2);
      assert.equal(f.rt.isInboundReloadSafe?.(), true);
      assert.equal(f.injected.some((item) => item.message.content.includes("unavailable host")), false);
    } finally {
      await f.cleanup();
    }
  });

  it("provider hold still yields a passive send and keeps the directive until recovery", { timeout: 10_000 }, async () => {
    const f = await fixture();
    try {
      let original: MeshFrame | undefined;
      f.bob.on("inbound", (frame: MeshFrame) => { if (frame.body === "held task") original = frame; });
      const sending = f.tools.get("mesh_send")!.execute("held-send", {
        to: "bob", message: "held task", awaitReply: true, timeoutMs: 5000,
      }, undefined, undefined, f.ctx);
      await waitFor(() => original !== undefined);
      f.rt.rateLimitedUntil = Date.now() + 1_800_000;
      const incoming = await f.carol.send({ to: "lead", message: "held directive" });
      assert.equal((await sending).details?.status, "interrupted");
      assert.equal(f.injected.some((item) => item.message.content.includes("held directive")), false);
      assert.equal(f.carol.readsOf(incoming.msgId!).length, 0);
      f.rt.rateLimitedUntil = undefined;
      f.rt.flushHeld?.();
      assert.equal(f.injected.find((item) => item.message.content.includes("held directive"))?.options?.deliverAs, "steer");
      assert.equal(f.abortCount(), 0);
      const resumed = f.lead.waitAll(2000);
      await f.bob.reply(original!.id, "held task completed");
      assert.equal((await resumed).status, "complete");
    } finally {
      await f.cleanup();
    }
  });

  it("authorized force is not delayed by provider hold", { timeout: 10_000 }, async () => {
    const f = await fixture({ allowForce: true });
    let aborts = 0;
    try {
      f.rt.rateLimitedUntil = Date.now() + 1_800_000;
      f.ctx.abort = () => { aborts += 1; f.ctx.isIdle = () => true; };
      const sent = await f.bob.send({ to: "lead", message: "authorized hold override", priority: "force", reason: "explicit operator override" });
      assert.equal(sent.status, "delivered");
      await waitFor(() => f.injected.some((item) => item.message.content.includes("authorized hold override")));
      assert.equal(aborts, 1);
      assert.ok(f.rt.rateLimitedUntil > Date.now(), "force must not clear the provider protection for ordinary input");
    } finally {
      await f.cleanup();
    }
  });

  it("a rejected late operation result is retried without rerunning the transaction", { timeout: 10_000 }, async () => {
    const f = await fixture({ rejectAttempts: 1, rejectType: "mesh-operation-result" });
    const socket = f.broker.state.peers.get("lead")!.socket;
    let responses = 0;
    f.lead.on("frame", (frame: MeshFrame) => { if (frame.type === "status_res") responses += 1; });
    try {
      socket.pause();
      const pending = f.tools.get("mesh_status")!.execute("retried-result", {}, undefined, undefined, f.ctx);
      await f.carol.send({ to: "lead", message: "yield control request" });
      assert.equal((await pending).details?.status, "interrupted");
      socket.resume();
      await waitFor(() => f.injectionAttempts() === 1);
      assert.equal(f.rt.pendingToolOperations?.size, 1);
      await waitFor(() => f.injected.some((item) => item.message.customType === "mesh-operation-result"));
      const completion = f.injected.find((item) => item.message.customType === "mesh-operation-result")!;
      assert.ok(((completion.message.details?.result as Record<string, unknown>).peers as { alias: string }[])
        .some((peer) => peer.alias === "bob"));
      assert.equal(responses, 1);
      assert.equal(f.injectionAttempts(), 2);
      assert.equal(f.rt.pendingToolOperations?.size, 0);
      assert.equal(f.rt.injectionFailures, 0);
    } finally {
      socket.resume();
      await f.cleanup();
    }
  });

  it("a mailbox drop notice survives a transient host rejection", { timeout: 10_000 }, async () => {
    const f = await fixture({ rejectAttempts: 1 });
    try {
      await f.bob.close();
      await waitFor(() => !f.lead.knowsPeer("bob"));
      const queued = await f.lead.send({ to: "bob", message: "expiring queue", awaitReply: true, block: false, timeoutMs: 5000 });
      assert.equal(queued.status, "queued_offline");
      f.broker.purgeMailboxExpired(Number.MAX_SAFE_INTEGER);
      await waitFor(() => f.injected.some((item) => item.message.details?.kind === "mesh-drop-notice"));
      const notice = f.injected.find((item) => item.message.details?.kind === "mesh-drop-notice")!;
      assert.equal(notice.message.details?.msgId, queued.msgId);
      assert.equal(f.injectionAttempts(), 2);
      assert.equal(f.rt.isInboundReloadSafe?.(), true);
      assert.equal(f.rt.injectionFailures, 0);
    } finally {
      await f.cleanup();
    }
  });

  it("deferred broadcasts and machine receipts do not wake a wait", { timeout: 10_000 }, async () => {
    const f = await fixture();
    try {
      const task = await f.lead.send({ to: "carol", message: "keep waiting", awaitReply: true, block: false, timeoutMs: 5000 });
      assert.equal(task.status, "delivered");
      let settled = false;
      const waiting = f.lead.waitAll(5000).then((result) => { settled = true; return result; });
      const observed: MeshFrame[] = [];
      f.lead.on("frame", (frame: MeshFrame) => observed.push(frame));
      await f.carol.send({ broadcast: true, room: "default", message: "deferred chatter" });
      await f.carol.reply(task.msgId!, "machine metadata", { receipt: true });
      await waitFor(() => observed.some((frame) => frame.receipt === true));
      assert.equal(f.rt.deferredInbox?.count, 1);
      assert.equal(settled, false);
      assert.equal(f.lead.pendingCount, 1);
      await f.carol.reply(task.msgId!, "actual answer");
      const completed = await waiting;
      assert.equal(completed.status, "complete");
      assert.deepEqual(completed.answers, [{ msgId: task.msgId, to: "carol", response: "actual answer" }]);
      assert.equal(f.abortCount(), 0);
    } finally {
      await f.cleanup();
    }
  });
});
