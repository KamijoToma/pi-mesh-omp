import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import meshExtension from "../src/extension/index.js";
import { MeshClient } from "../src/client/client.js";
import type { ExtensionAPI, CommandDefinition, InboundMessage, SessionContext, ToolDefinition } from "../src/extension/pi-types.js";
import { IDENTITY_PENDING_NAME, MeshIdentity } from "../src/shared/identity-store.js";
import { makeTempDirs, startTestBroker, waitFor } from "./helpers.js";

type Hook = (event: unknown, ctx: SessionContext) => unknown;
type ManagedTimer = Parameters<NonNullable<SessionContext["clearTimer"]>>[0];

async function lifecycleFixture(omp: boolean) {
  const dirs = makeTempDirs("mesh-lifecycle-");
  const savedEnvironment: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (key.startsWith("MESH_") && value !== undefined) {
      savedEnvironment[key] = value;
      delete process.env[key];
    }
  }
  process.env.MESH_RUNTIME_DIR = dirs.runtimeDir;
  process.env.MESH_STATE_DIR = dirs.stateDir;
  process.env.MESH_INBOUND_BATCH_MS = "0";
  const broker = await startTestBroker(dirs.runtimeDir);
  const observer = new MeshClient({ alias: "observer", runtimeDir: dirs.runtimeDir });
  await observer.connect();
  let sessionId = "original";
  let sequence = 0;
  let cancelNew = false;
  let modelId = "quota-model";
  let idle = true;
  let alias = "";
  const names = new Map<string, string>();
  const hooks = new Map<string, Hook[]>();
  const tools = new Map<string, ToolDefinition>();
  const commands = new Map<string, CommandDefinition>();
  const sent: InboundMessage[] = [];
  const notifications: { message: string; severity: string | undefined; sessionId: string }[] = [];
  const events = new EventEmitter();
  const timers = new Map<ManagedTimer, { callback: () => void; ms: number }>();
  const entries: { type: string; data: unknown }[] = [];
  const api = {
    ...(omp ? { pi: {} } : {}),
    on: (name: string, handler: Hook) => {
      const handlers = hooks.get(name) ?? [];
      handlers.push(handler);
      hooks.set(name, handlers);
    },
    registerTool: (tool: ToolDefinition) => tools.set(tool.name, tool),
    registerCommand: (name: string, command: CommandDefinition) => commands.set(name, command),
    sendMessage: (message: InboundMessage) => { sent.push(message); events.emit("sent", message); },
    appendEntry: (type: string, data: unknown) => entries.push({ type, data }),
    events: {
      emit: (name: string, data: unknown) => {
        if (name === "mesh:alias" && data !== null && typeof data === "object" && "alias" in data && typeof data.alias === "string") alias = data.alias;
        events.emit(name, data);
      },
    },
    getSessionName: () => names.get(sessionId),
    setSessionName: (name: string) => names.set(sessionId, name),
  } as unknown as ExtensionAPI;

  const emit = async (name: string, event: unknown = {}, ctx = context()): Promise<unknown> => {
    let result: unknown;
    for (const handler of hooks.get(name) ?? []) {
      const value = await handler(event, ctx);
      if (value !== undefined) result = value;
    }
    return result;
  };
  const context = (): SessionContext => {
    const contextSessionId = sessionId;
    const ensureFresh = (): void => {
      if (contextSessionId !== sessionId) throw new Error("stale extension UI");
    };
    return {
      cwd: dirs.root, mode: "tui", sessionManager: { getSessionId: () => sessionId },
      ui: {
        notify: (message, severity) => { ensureFresh(); notifications.push({ message, severity, sessionId }); },
        setWidget: () => ensureFresh(), setStatus: () => ensureFresh(),
      },
      isIdle: () => idle,
      models: omp ? { current: () => ({ provider: "test", id: modelId }) as never, list: () => [], resolve: () => undefined, family: () => "test" } : undefined,
      setTimeout: omp ? (callback, ms = 0) => {
        const timer = {} as ManagedTimer;
        timers.set(timer, { callback, ms });
        return timer;
      } : undefined,
      clearTimer: omp ? (timer) => { timers.delete(timer); } : undefined,
      newSession: async (options) => {
        if (cancelNew) return { cancelled: true };
        if (!omp) await emit("session_shutdown");
        sessionId = `new-${++sequence}`;
        await emit(omp ? "session_switch" : "session_start", { reason: "new" });
        await options?.withSession?.(context());
        return { cancelled: false };
      },
    };
  };
  meshExtension(api);
  const ready = once(events, "mesh:alias");
  await emit("session_start");
  await ready;
  return {
    dirs, observer, sent, notifications, timers, entries,
    identity: new MeshIdentity(dirs.stateDir),
    ready: () => once(events, "mesh:alias"),
    alias: () => alias, sessionId: () => sessionId, emit, context,
    command: async (args: string) => { await commands.get("mesh")!.handler(args, context()); },
    tool: async (name: string, params: Record<string, unknown>) => tools.get(name)!.execute("test-call", params, undefined, undefined, context()),
    cancel: () => { cancelNew = true; },
    modelChanged: () => { modelId = "working-model"; },
    running: () => { idle = false; },
    idle: () => { idle = true; },
    fireNextTimer: () => {
      const next = [...timers.entries()].sort((left, right) => left[1].ms - right[1].ms)[0];
      assert.ok(next);
      timers.delete(next[0]);
      next[1].callback();
    },
    switchTo: async (id: string, reason: "new" | "resume" | "fork" | "branch") => {
      const connected = once(events, "mesh:alias");
      if (!omp) await emit("session_shutdown");
      sessionId = id;
      await emit(omp ? (reason === "branch" ? "session_branch" : "session_switch") : "session_start", { reason });
      await connected;
    },
    receive: async (body: string) => {
      const delivered = once(events, "sent");
      await observer.send({ to: alias, message: body });
      await delivered;
    },
    close: async () => {
      await emit("session_shutdown");
      await observer.close();
      await broker.close();
      dirs.cleanup();
      for (const key of Object.keys(process.env)) if (key.startsWith("MESH_")) delete process.env[key];
      Object.assign(process.env, savedEnvironment);
    },
  };
}

describe("mesh extension session lifecycle", { concurrency: false, timeout: 15_000 }, () => {
  it("omp /mesh new transfers history/rooms/reservations and persists subsequent rename only under the new ID", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      const initialAlias = fixture.alias();
      await fixture.command("join work");
      await fixture.tool("mesh_reserve", { paths: ["src/shared.ts"], reason: "handoff" });
      await fixture.receive("history-before-new");
      const ready = fixture.ready();
      await fixture.command("new --history");
      await ready;
      assert.equal(fixture.alias(), initialAlias);
      const newId = fixture.sessionId();
      assert.notEqual(newId, "original");
      const transferred = fixture.identity.load(newId);
      assert.ok(transferred?.rooms.includes("work"));
      assert.equal(transferred?.reservations[0]?.pattern, "src/shared.ts");
      assert.equal(fs.existsSync(path.join(fixture.dirs.stateDir, IDENTITY_PENDING_NAME)), false);
      assert.ok(fixture.sent.some((message) => message.customType === "mesh-context" && message.content.includes("history-before-new")));
      await fixture.command("alias renamed-after-new");
      assert.equal(fixture.identity.load(newId)?.alias, "renamed-after-new");
      assert.equal(fixture.identity.load("original")?.alias, initialAlias);
    } finally {
      await fixture.close();
    }
  });

  it("omp new/resume/fork/branch/reload preserve identity and inject each incoming message once", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      const initialAlias = fixture.alias();
      await fixture.switchTo("fresh", "new");
      assert.notEqual(fixture.alias(), initialAlias);
      await fixture.switchTo("original", "resume");
      assert.equal(fixture.alias(), initialAlias);
      await fixture.switchTo("forked", "fork");
      assert.equal(fixture.alias(), initialAlias);
      await fixture.switchTo("branched", "branch");
      assert.equal(fixture.alias(), initialAlias);
      await fixture.switchTo("branched", "resume");
      assert.equal(fixture.alias(), initialAlias);
      await fixture.receive("message-after-session-transitions");
      assert.equal(fixture.sent.filter((message) => message.content.includes("message-after-session-transitions")).length, 1);
    } finally {
      await fixture.close();
    }
  });

  it("cancelled omp new leaves the live connection intact and no staged handoff", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      const alias = fixture.alias();
      fixture.cancel();
      await fixture.command("new --history");
      assert.equal(fixture.sessionId(), "original");
      assert.equal(fixture.alias(), alias);
      assert.equal(fs.existsSync(path.join(fixture.dirs.stateDir, IDENTITY_PENDING_NAME)), false);
      await fixture.receive("still-live-after-cancel");
      assert.ok(fixture.sent.some((message) => message.content.includes("still-live-after-cancel")));
    } finally {
      await fixture.close();
    }
  });

  it("both hosts report the reserved target for scalar paths and later multi-target entries", async () => {
    for (const omp of [true, false]) {
      const fixture = await lifecycleFixture(omp);
      try {
        await fixture.observer.reserve(["reserved.ts"], "peer editing");
        await fixture.tool("mesh_status", {}); // Ordered response drains the prior reservation frame.
        const multi = await fixture.emit("tool_call", { toolName: "edit", input: { paths: ["free.ts", "reserved.ts"] } });
        assert.ok(multi !== null && typeof multi === "object" && "block" in multi && multi.block === true);
        assert.ok("reason" in multi && typeof multi.reason === "string" && multi.reason.startsWith("reserved.ts\n"));
        const scalar = await fixture.emit("tool_call", { toolName: "write", input: { path: "reserved.ts" } });
        assert.ok(scalar !== null && typeof scalar === "object" && "block" in scalar && scalar.block === true);
        const allowed = await fixture.emit("tool_call", { toolName: "edit", input: { paths: ["free.ts"] } });
        assert.equal(allowed, undefined);
      } finally {
        await fixture.close();
      }
    }
  });

  it("omp succeeds to idle, preserves busy during continuation, and idle model recovery delivers held inbound exactly once", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      const alias = fixture.alias();
      fixture.running();
      await fixture.emit("agent_start");
      await fixture.emit("tool_call", { toolName: "read", input: {} });
      await waitFor(() => fixture.observer.activityOf(alias)?.state === "busy");
      await fixture.emit("agent_end", { willContinue: true });
      assert.equal(fixture.observer.activityOf(alias)?.state, "busy");
      fixture.idle();
      await fixture.emit("agent_end", { willContinue: false });
      await waitFor(() => fixture.observer.activityOf(alias)?.state === "idle");
      await fixture.emit("turn_end", { message: { stopReason: "error", errorMessage: "insufficient_quota" }, toolResults: [] });
      await waitFor(() => fixture.observer.activityOf(alias)?.state === "blocked");
      await fixture.observer.send({ to: alias, message: "held-for-working-model" });
      await fixture.tool("mesh_status", {}); // Network barrier, not a guessed sleep.
      assert.equal(fixture.sent.some((message) => message.content.includes("held-for-working-model")), false);
      fixture.modelChanged();
      fixture.fireNextTimer();
      await waitFor(() => fixture.sent.some((message) => message.content.includes("held-for-working-model")));
      assert.equal(fixture.sent.filter((message) => message.content.includes("held-for-working-model")).length, 1);
      assert.equal(fixture.timers.size, 0);
      await fixture.emit("agent_end", {});
      assert.equal(fixture.sent.filter((message) => message.content.includes("held-for-working-model")).length, 1);
    } finally {
      await fixture.close();
    }
  });

  it("a successful same-model turn releases held messages without waiting for the hold deadline", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      await fixture.emit("turn_end", { message: { stopReason: "error", errorMessage: "429 rate limit" }, toolResults: [] });
      await fixture.observer.send({ to: fixture.alias(), message: "held-before-successful-turn" });
      await fixture.tool("mesh_status", {});
      assert.equal(fixture.sent.some((message) => message.content.includes("held-before-successful-turn")), false);
      await fixture.emit("agent_start");
      await fixture.emit("turn_end", { message: { stopReason: "stop" }, toolResults: [] });
      await waitFor(() => fixture.sent.some((message) => message.content.includes("held-before-successful-turn")));
      assert.equal(fixture.sent.filter((message) => message.content.includes("held-before-successful-turn")).length, 1);
      assert.equal(fixture.timers.size, 0);
    } finally {
      await fixture.close();
    }
  });

  it("omp switches discard old held queues and cancel model timers before activating the target", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      await fixture.emit("turn_end", { message: { stopReason: "error", errorMessage: "insufficient_quota" }, toolResults: [] });
      await fixture.observer.send({ to: fixture.alias(), message: "outgoing-held-message" });
      await fixture.tool("mesh_status", {});
      assert.equal(fixture.sent.some((message) => message.content.includes("outgoing-held-message")), false);
      const staleCallbacks = [...fixture.timers.values()].map((timer) => timer.callback);
      await fixture.switchTo("other-session", "new");
      assert.equal(fixture.timers.size, 0);
      for (const callback of staleCallbacks) callback();
      assert.equal(fixture.sent.some((message) => message.content.includes("outgoing-held-message")), false);
      await fixture.receive("incoming-session-message");
      assert.equal(fixture.sent.filter((message) => message.content.includes("incoming-session-message")).length, 1);
    } finally {
      await fixture.close();
    }
  });

  it("shutdown closes the peer even when identity persistence fails", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      const alias = fixture.alias();
      fs.rmSync(fixture.dirs.stateDir, { recursive: true, force: true });
      fs.writeFileSync(fixture.dirs.stateDir, "not a directory");
      await fixture.emit("session_shutdown");
      const snapshot = await fixture.observer.status();
      assert.equal(snapshot.peers.some((peer) => peer.alias === alias), false);
    } finally {
      await fixture.close();
    }
  });

  it("Pi keeps withSession handoff and native fork/model/settle hooks", async () => {
    const fixture = await lifecycleFixture(false);
    try {
      const alias = fixture.alias();
      const ready = fixture.ready();
      await fixture.command("new");
      await ready;
      assert.equal(fixture.identity.load(fixture.sessionId())?.alias, alias);
      assert.ok(fixture.notifications.some((notice) => notice.sessionId === fixture.sessionId() && notice.severity === "info"));
      await fixture.emit("session_before_fork");
      await fixture.switchTo("pi-fork", "fork");
      assert.equal(fixture.alias(), alias);
      assert.equal(fixture.identity.load("pi-fork")?.alias, alias);
      assert.equal(fs.existsSync(path.join(fixture.dirs.stateDir, IDENTITY_PENDING_NAME)), false);
      fixture.running();
      await fixture.emit("agent_start");
      await fixture.emit("tool_call", { toolName: "read", input: {} });
      await waitFor(() => fixture.observer.activityOf(alias)?.state === "busy");
      fixture.idle();
      await fixture.emit("agent_settled");
      await waitFor(() => fixture.observer.activityOf(alias)?.state === "idle");
      await fixture.emit("turn_end", { message: { stopReason: "error", errorMessage: "insufficient_quota" }, toolResults: [] });
      await fixture.observer.send({ to: alias, message: "pi-held-message" });
      await fixture.tool("mesh_status", {});
      assert.equal(fixture.sent.some((message) => message.content.includes("pi-held-message")), false);
      await fixture.emit("model_select");
      assert.equal(fixture.sent.filter((message) => message.content.includes("pi-held-message")).length, 1);
      await fixture.emit("model_select");
      assert.equal(fixture.sent.filter((message) => message.content.includes("pi-held-message")).length, 1);
    } finally {
      await fixture.close();
    }
  });
});
