import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { MeshLoopController } from "../src/extension/mesh-loop.js";
import type { ExtensionAPI, InboundMessage, SendMessageOptions, SessionContext } from "../src/extension/pi-types.js";
import type { MeshRuntime } from "../src/extension/tools.js";

type ManagedTimer = Parameters<NonNullable<SessionContext["clearTimer"]>>[0];

interface ManagedEntry {
  callback: () => void;
  ms: number;
}


describe("mesh-loop controller", () => {
  it("waits for pending tool operations and mission failures before re-prompting", async () => {
    const messages: { message: InboundMessage; options: SendMessageOptions | undefined }[] = [];
    const timers = new Map<ManagedTimer, ManagedEntry>();
    const sessionId = "loop-test";
    let idle = true;
    let inboundPending = false;
    let pendingMeshReplies = 0;
    const pi = {
      pi: {},
      sendMessage: (message: InboundMessage, options?: SendMessageOptions) => messages.push({ message, options }),
    } as unknown as ExtensionAPI;
    const ctx = {
      cwd: process.cwd(),
      mode: "tui",
      getAsyncJobSnapshot: () => ({
        running: [],
        recent: [],
        delivery: { queued: 0, delivering: false, pendingJobIds: [] },
      }),
      sessionManager: { getSessionId: () => sessionId },
      ui: { notify: () => {}, setWidget: () => {}, setStatus: () => {} },
      isIdle: () => idle,
      hasPendingMessages: () => false,
      abort: () => {},
      setTimeout: (callback: () => void, ms = 0) => {
        const timer = {} as ManagedTimer;
        timers.set(timer, { callback, ms });
        return timer;
      },
      clearTimer: (timer: ManagedTimer) => { timers.delete(timer); },
    } as SessionContext;
    const runtime = {
      client: { get pendingCount() { return pendingMeshReplies; } },
      sessionId,
      ctx,
      pendingToolOperations: new Set<(reason: string, msgId?: string) => boolean>(),
      hasPendingInboundDelivery: () => inboundPending,
    } as unknown as MeshRuntime;
    const currentRuntime: MeshRuntime | null = runtime;
    const loop = new MeshLoopController(pi, () => currentRuntime);

    const started = loop.start(runtime, ctx, 3, "finish this task");
    assert.equal(started.ok, true);
    assert.equal(messages.length, 1);
    assert.equal(messages[0]?.message.display, false);
    assert.equal(messages[0]?.options?.triggerTurn, true);
    assert.equal(messages[0]?.options?.deliverAs, "followUp");

    idle = false;
    loop.onAgentStart(runtime, ctx);
    const operation = () => false;
    runtime.pendingToolOperations?.add(operation);
    idle = true;
    loop.onSettled(runtime, ctx);
    assert.equal(timers.size, 0);
    assert.equal(messages.length, 1);

    runtime.pendingToolOperations?.delete(operation);
    inboundPending = true;
    idle = false;
    loop.onAgentStart(runtime, ctx);
    idle = true;
    loop.onSettled(runtime, ctx);
    assert.equal(timers.size, 0);
    assert.equal(messages.length, 1);

    inboundPending = false;
    loop.onInboundInjected(runtime);
    const firstContinuation = [...timers.entries()].find(([, entry]) => entry.ms === 800);
    assert.ok(firstContinuation);
    timers.delete(firstContinuation[0]);
    firstContinuation[1].callback();
    assert.equal(messages.length, 2);

    idle = false;
    loop.onAgentStart(runtime, ctx);
    pendingMeshReplies = 1;
    idle = true;
    loop.onSettled(runtime, ctx);
    assert.equal(timers.size, 0);

    pendingMeshReplies = 0; // terminal failure settles without injecting a reply
    loop.onMissionsChanged(runtime);
    await Promise.resolve();
    const finalContinuation = [...timers.entries()].find(([, entry]) => entry.ms === 800);
    assert.ok(finalContinuation);
    timers.delete(finalContinuation[0]);
    finalContinuation[1].callback();
    assert.equal(messages.length, 3);

    idle = false;
    loop.onAgentStart(runtime, ctx);
    idle = true;
    loop.onSettled(runtime, ctx);
    assert.equal(timers.size, 0);
    assert.equal(loop.isActive(), false);
  });
});
