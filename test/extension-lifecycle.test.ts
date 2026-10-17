import assert from "node:assert/strict";
import { EventEmitter, once } from "node:events";
import * as fs from "node:fs";
import * as path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath, pathToFileURL } from "node:url";
import * as Tui from "@mariozechner/pi-tui";
import meshExtension from "../src/extension/index.js";
import createMeshExtension from "../src/extension/implementation.js";
import { installReloadableMesh } from "../src/extension/reload.js";
import { MESH_VERSION } from "../src/shared/version.js";
import { MeshClient } from "../src/client/client.js";
import type { ExtensionAPI, CommandDefinition, InboundMessage, SessionContext, ToolDefinition } from "../src/extension/pi-types.js";
import { IDENTITY_PENDING_NAME, MeshIdentity } from "../src/shared/identity-store.js";
import { makeTempDirs, startTestBroker, waitFor } from "./helpers.js";

type Hook = (event: unknown, ctx: SessionContext) => unknown;
type ManagedTimer = Parameters<NonNullable<SessionContext["clearTimer"]>>[0];

async function lifecycleFixture(omp: boolean, options: { reload?: boolean; deferred?: boolean } = {}) {
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
  if (options.deferred) process.env.MESH_INBOUND_BROADCASTS = "deferred";
  const packageRoot = path.join(dirs.root, "package");
  if (options.reload) {
    fs.mkdirSync(packageRoot, { recursive: true });
    fs.writeFileSync(path.join(packageRoot, "package.json"),
      JSON.stringify({ name: "pi-mesh-extension", type: "module" }));
    fs.cpSync(fileURLToPath(new URL("../src/", import.meta.url)),
      path.join(packageRoot, "dist", "src"), { recursive: true });
  }
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
  if (options.reload) {
    installReloadableMesh(api, createMeshExtension,
      pathToFileURL(path.join(packageRoot, "dist", "src", "extension", "index.js")).href, Tui);
  } else meshExtension(api);
  const ready = once(events, "mesh:alias");
  await emit("session_start");
  await ready;
  return {
    dirs, observer, sent, notifications, timers, entries, names,
    packageRoot,
    updateVersion: (version: string) => {
      fs.writeFileSync(path.join(packageRoot, "dist", "src", "shared", "version.js"),
        `export const MESH_VERSION = ${JSON.stringify(version)};\n`);
    },
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
  it("both hosts keep task titles independent of mesh identity changes", async () => {
    for (const omp of [true, false]) {
      const fixture = await lifecycleFixture(omp);
      try {
        assert.equal(fixture.names.get("original"), undefined);
        await fixture.command("join title-room");
        assert.equal(fixture.names.get("original"), undefined);

        fixture.names.set("original", "Review host-owned task");
        await fixture.command("alias title-reviewer");
        await fixture.tool("mesh_alias", { alias: "title-author" });
        await fixture.command("leave title-room");
        assert.equal(fixture.alias(), "title-author");
        assert.deepEqual(fixture.identity.load("original")?.rooms, ["default"]);
        assert.equal(fixture.names.get("original"), "Review host-owned task");

        // Existing mesh-looking titles belong to the host too; do not migrate them implicitly.
        fixture.names.set("original", "mesh @legacy · default");
        await fixture.command("reset");
        await fixture.switchTo("original", "resume");
        assert.equal(fixture.names.get("original"), "mesh @legacy · default");
        assert.equal(fixture.identity.load("original")?.alias, fixture.alias());

        await fixture.switchTo("untitled-session", "new");
        assert.equal(fixture.names.get("untitled-session"), undefined);
        fixture.names.set("fork-session", "Inherited task title");
        await fixture.switchTo("fork-session", "fork");
        assert.equal(fixture.names.get("fork-session"), "Inherited task title");
      } finally {
        await fixture.close();
      }
    }
  });

  it("both hosts switch out of every old room while join stays additive, and resume restores only the destination", async () => {
    for (const omp of [true, false]) {
      const fixture = await lifecycleFixture(omp);
      try {
        const alias = fixture.alias();
        await fixture.command("join alpha");
        await fixture.command("join beta");
        assert.deepEqual(fixture.identity.load("original")?.rooms, ["default", "alpha", "beta"]);
        await fixture.tool("mesh_reserve", { paths: ["room-switch.ts"], reason: "keep across switch" });
        await fixture.observer.join("ops");
        await fixture.observer.join("alpha");
        await fixture.command("switch ops");
        assert.equal(fixture.alias(), alias);
        assert.deepEqual(fixture.identity.load("original")?.rooms, ["ops"]);
        const self = (await fixture.observer.status()).peers.find((peer) => peer.alias === alias);
        assert.deepEqual(self?.rooms, ["ops"]);
        assert.equal(self?.reservations?.[0]?.pattern, "room-switch.ts");

        const inbound = once(fixture.observer, "inbound");
        const sent = await fixture.tool("mesh_send", { to: "observer", message: "destination-is-default-send-room" });
        assert.equal(sent.details?.status, "delivered");
        const [frame] = await inbound;
        assert.equal(frame.room, "ops");
        await fixture.observer.send({ room: "alpha", broadcast: true, message: "left-room-traffic" });
        await fixture.tool("mesh_status", {});
        assert.equal(fixture.sent.some((message) => message.content.includes("left-room-traffic")), false);

        await fixture.switchTo("other-session", "new");
        await fixture.switchTo("original", "resume");
        const restored = (await fixture.observer.status()).peers.find((peer) => peer.alias === alias);
        assert.deepEqual(restored?.rooms, ["ops"]);
        assert.equal(restored?.reservations?.[0]?.pattern, "room-switch.ts");
      } finally {
        await fixture.close();
      }
    }
  });

  it("switching to an already-joined room retains it, applies observer/alias options and allows a repeated switch", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      await fixture.command("join ops");
      await fixture.command("join alpha");
      await fixture.observer.join("ops");
      await fixture.command("switch ops as room-reviewer observer");
      assert.equal(fixture.alias(), "room-reviewer");
      assert.deepEqual(fixture.identity.load("original")?.rooms, ["ops"]);
      const observerSend = await fixture.tool("mesh_send", { to: "observer", room: "ops", message: "observer-must-not-send" });
      assert.equal(observerSend.details?.reason, "observer_readonly");
      await fixture.command("switch ops");
      await fixture.command("switch ops");
      assert.deepEqual(fixture.identity.load("original")?.rooms, ["ops"]);
      const inbound = once(fixture.observer, "inbound");
      const memberSend = await fixture.tool("mesh_send", { to: "observer", message: "member-after-repeat-switch" });
      assert.equal(memberSend.details?.status, "delivered");
      const [frame] = await inbound;
      assert.equal(frame.room, "ops");
      assert.equal(frame.from, "room-reviewer");
    } finally {
      await fixture.close();
    }
  });

  it("missing or rejected switch destinations preserve the original rooms and a missing room never renames", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      const alias = fixture.alias();
      await fixture.command("join alpha");
      for (const command of ["switch", "switch as should-not-rename", "switch invalid!"]) {
        await fixture.command(command);
        assert.equal(fixture.alias(), alias);
        assert.deepEqual(fixture.identity.load("original")?.rooms, ["default", "alpha"]);
        const self = (await fixture.observer.status()).peers.find((peer) => peer.alias === alias);
        assert.deepEqual(self?.rooms, ["default", "alpha"]);
      }
      const inbound = once(fixture.observer, "inbound");
      const result = await fixture.tool("mesh_send", { to: "observer", message: "still-in-original-room" });
      assert.equal(result.details?.status, "delivered");
      const [frame] = await inbound;
      assert.equal(frame.room, "default");
    } finally {
      await fixture.close();
    }
  });

  it("both hosts route messages and broadcast mentions by the custom name and restore it on resume", async () => {
    for (const omp of [true, false]) {
      const fixture = await lifecycleFixture(omp, { deferred: true });
      try {
        const oldAlias = fixture.alias();
        await fixture.command("join work");
        await fixture.tool("mesh_reserve", { paths: ["named-agent.ts"], reason: "named work" });
        const renamed = await fixture.tool("mesh_alias", { alias: " @Reviewer " });
        assert.equal(renamed.details?.status, "renamed");
        assert.equal(fixture.alias(), "reviewer");
        const snapshot = await fixture.observer.status();
        assert.equal(snapshot.peers.some((peer) => peer.alias === oldAlias), false);
        const self = snapshot.peers.find((peer) => peer.alias === "reviewer");
        assert.deepEqual(self?.rooms, ["default", "work"]);
        assert.equal(self?.reservations?.[0]?.pattern, "named-agent.ts");

        await fixture.observer.send({ broadcast: true, message: "@Reviewer review the named-change" });
        await fixture.tool("mesh_status", {});
        assert.equal(fixture.sent.filter((message) => message.customType === "mesh-inbound" &&
          message.content.includes("@Reviewer review the named-change")).length, 1);
        await fixture.observer.send({ broadcast: true, message: `@${oldAlias} obsolete-name-mention` });
        await fixture.tool("mesh_status", {});
        assert.equal(fixture.sent.some((message) => message.content.includes("obsolete-name-mention")), false);
        await fixture.receive("direct-to-custom-name");
        assert.equal(fixture.sent.filter((message) => message.content.includes("direct-to-custom-name")).length, 1);

        const inbound = once(fixture.observer, "inbound");
        const sending = fixture.tool("mesh_send", {
          to: "observer", message: "named-request", awaitReply: true, timeoutMs: 3000,
        });
        const [request] = await inbound;
        assert.equal(request.from, "reviewer");
        await fixture.observer.reply(request.id, "named-answer");
        const answer = await sending;
        assert.equal(answer.details?.status, "reply");
        assert.ok(answer.content.some((part) => part.text.includes("named-answer")));

        assert.equal(fixture.identity.load("original")?.alias, "reviewer");
        await fixture.switchTo("other-session", "new");
        await fixture.switchTo("original", "resume");
        assert.equal(fixture.alias(), "reviewer");
        const current = await fixture.tool("mesh_alias", {});
        assert.equal(current.details?.alias, "reviewer");
        await fixture.receive("direct-after-named-resume");
        assert.equal(fixture.sent.filter((message) => message.content.includes("direct-after-named-resume")).length, 1);
      } finally {
        await fixture.close();
      }
    }
  });

  it("refuses invalid and occupied names without losing the current identity or routing", async () => {
    const fixture = await lifecycleFixture(true);
    try {
      await fixture.tool("mesh_alias", { alias: "reviewer" });
      for (const alias of ["", "a", "bad_name", "1reviewer", "r".repeat(33), "observer"]) {
        const result = await fixture.tool("mesh_alias", { alias });
        assert.equal(result.details?.status, "blocked");
        assert.equal(result.details?.reason, alias === "observer" ? "alias_taken" : "invalid_alias");
        assert.equal(fixture.alias(), "reviewer");
        assert.equal(fixture.identity.load("original")?.alias, "reviewer");
      }
      const unchanged = await fixture.tool("mesh_alias", { alias: "@REVIEWER" });
      assert.equal(unchanged.details?.status, "unchanged");
      const snapshot = await fixture.observer.status();
      assert.ok(snapshot.peers.some((peer) => peer.alias === "observer"));
      assert.ok(snapshot.peers.some((peer) => peer.alias === "reviewer"));
      await fixture.receive("direct-after-refused-name");
      assert.equal(fixture.sent.filter((message) => message.content.includes("direct-after-refused-name")).length, 1);
    } finally {
      await fixture.close();
    }
  });

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

describe("mesh built-version hot reload", { concurrency: false, timeout: 15_000 }, () => {
  it("replaces the client module graph while preserving identity, reply targets and reserve deadlines", async () => {
    const fixture = await lifecycleFixture(true, { reload: true });
    try {
      const alias = fixture.alias();
      await fixture.command("join work");
      await fixture.command("log on");
      await fixture.tool("mesh_reserve", { paths: ["hot-reload.ts"], autoReleaseMs: 1000 });
      const original = await fixture.observer.send({ to: alias, message: "reply-after-reload" });
      assert.ok(original.status === "delivered");
      await waitFor(() => fixture.sent.some((message) => message.content.includes("reply-after-reload")));
      const version = `${MESH_VERSION}-reload-test`;
      fixture.updateVersion(version);
      await fixture.command("reload");
      await waitFor(() => fixture.notifications.some((notification) => notification.message.includes(version)));
      assert.equal(fixture.sessionId(), "original");
      assert.equal(fixture.alias(), alias);
      const peer = (await fixture.observer.status()).peers.find((candidate) => candidate.alias === alias);
      assert.equal(peer?.clientVersion, version);
      assert.deepEqual(peer?.rooms, ["default", "work"]);
      assert.equal(peer?.reservations?.[0]?.pattern, "hot-reload.ts");
      const history = await fixture.tool("mesh_history", { withBodies: true });
      assert.ok(history.content.some((part) => part.text.includes("reply-after-reload")));
      const answer = once(fixture.observer, "inbound");
      const replied = await fixture.tool("mesh_reply", { msgId: original.msgId, message: "same-target-new-code" });
      assert.equal(replied.details?.status, "delivered");
      const [frame] = await answer;
      assert.equal(frame.replyTo, original.msgId);
      assert.equal(frame.body, "same-target-new-code");
      await waitFor(() => fixture.observer.reservationsOf(alias).length === 0, 2000);
      await fixture.receive("one-new-generation-delivery");
      assert.equal(fixture.sent.filter((message) => message.content.includes("one-new-generation-delivery")).length, 1);
      assert.ok(fs.readdirSync(path.join(fixture.dirs.stateDir, "transcripts")).some((name) => name.endsWith(".jsonl")));
    } finally { await fixture.close(); }
  });

  it("waits for an active reply and busy host instead of cancelling either", async () => {
    const fixture = await lifecycleFixture(true, { reload: true });
    try {
      fixture.running();
      await fixture.emit("agent_start");
      const inbound = once(fixture.observer, "inbound");
      const sending = fixture.tool("mesh_send", {
        to: "observer", message: "finish-before-reload", awaitReply: true, timeoutMs: 3000,
      });
      const [request] = await inbound;
      const version = `${MESH_VERSION}-queued-test`;
      fixture.updateVersion(version);
      await fixture.command("reload");
      const before = (await fixture.observer.status()).peers.find((peer) => peer.alias === fixture.alias());
      assert.equal(before?.clientVersion, MESH_VERSION);
      await fixture.observer.reply(request.id, "reply-not-cancelled");
      const result = await sending;
      assert.equal(result.details?.status, "reply");
      assert.ok(result.content.some((part) => part.text.includes("reply-not-cancelled")));
      fixture.idle();
      await fixture.emit("agent_end", { willContinue: false });
      await waitFor(() => fixture.notifications.some((notification) => notification.message.includes(version)));
      const after = (await fixture.observer.status()).peers.find((peer) => peer.alias === fixture.alias());
      assert.equal(after?.clientVersion, version);
      assert.equal(fixture.sessionId(), "original");
    } finally { await fixture.close(); }
  });

  it("retains both deferred and already-queued broadcasts without reinjecting queued messages", async () => {
    const fixture = await lifecycleFixture(true, { reload: true, deferred: true });
    try {
      await fixture.observer.send({ room: "default", broadcast: true, message: "already-next-turn" });
      await fixture.tool("mesh_status", {});
      await fixture.emit("input");
      assert.equal(fixture.sent.filter((message) => message.content.includes("already-next-turn")).length, 1);
      await fixture.observer.send({ room: "default", broadcast: true, message: "still-local-inbox" });
      await fixture.tool("mesh_status", {});
      const version = `${MESH_VERSION}-inbox-test`;
      fixture.updateVersion(version);
      await fixture.command("reload");
      await waitFor(() => fixture.notifications.some((notification) => notification.message.includes(version)));
      await fixture.command("inbox");
      const listing = fixture.notifications.at(-1)?.message ?? "";
      assert.ok(listing.includes("already-next-turn") && listing.includes("still-local-inbox"));
      await fixture.emit("input");
      await fixture.emit("before_agent_start");
      await fixture.emit("agent_start");
      assert.equal(fixture.sent.filter((message) => message.content.includes("already-next-turn")).length, 1);
      assert.equal(fixture.sent.filter((message) => message.content.includes("still-local-inbox")).length, 1);
      await fixture.command("inbox");
      assert.equal(fixture.notifications.at(-1)?.message.includes("still-local-inbox"), false);
    } finally { await fixture.close(); }
  });

  it("keeps a provider hold across reload and delivers held frames once after model recovery", async () => {
    const fixture = await lifecycleFixture(true, { reload: true });
    try {
      await fixture.emit("turn_end", {
        message: { stopReason: "error", errorMessage: "insufficient_quota" }, toolResults: [],
      });
      await fixture.observer.send({ to: fixture.alias(), message: "held-through-new-code" });
      await fixture.tool("mesh_status", {});
      const version = `${MESH_VERSION}-hold-test`;
      fixture.updateVersion(version);
      await fixture.command("reload");
      await waitFor(() => fixture.notifications.some((notification) => notification.message.includes(version)));
      assert.equal(fixture.sent.some((message) => message.content.includes("held-through-new-code")), false);
      await waitFor(() => fixture.observer.activityOf(fixture.alias())?.state === "blocked");
      fixture.modelChanged();
      fixture.fireNextTimer();
      assert.equal(fixture.sent.filter((message) => message.content.includes("held-through-new-code")).length, 1);
      await fixture.emit("agent_end", { willContinue: false });
      assert.equal(fixture.sent.filter((message) => message.content.includes("held-through-new-code")).length, 1);
    } finally { await fixture.close(); }
  });

  it("leaves the previous runtime usable when a built module cannot be imported", async () => {
    const fixture = await lifecycleFixture(true, { reload: true });
    try {
      const alias = fixture.alias();
      fs.writeFileSync(path.join(fixture.packageRoot, "dist", "src", "extension", "implementation.js"),
        "throw new Error('unloadable candidate');\n");
      await fixture.command("reload");
      assert.ok(fixture.notifications.some((notification) => notification.severity === "error"));
      const peer = (await fixture.observer.status()).peers.find((candidate) => candidate.alias === alias);
      assert.equal(peer?.clientVersion, MESH_VERSION);
      await fixture.receive("still-connected-after-invalid-build");
      assert.equal(fixture.sent.filter((message) => message.content.includes("still-connected-after-invalid-build")).length, 1);
      assert.equal(fixture.alias(), alias);
    } finally { await fixture.close(); }
  });

  it("reconstructs the previous version after candidate startup fails without losing old reply targets", async () => {
    const fixture = await lifecycleFixture(true, { reload: true });
    try {
      const alias = fixture.alias();
      const original = await fixture.observer.send({ to: alias, message: "reply-after-rollback" });
      assert.ok(original.status === "delivered");
      await fixture.tool("mesh_status", {});
      const extensionDir = path.join(fixture.packageRoot, "dist", "src", "extension");
      fs.renameSync(path.join(extensionDir, "implementation.js"), path.join(extensionDir, "implementation-base.js"));
      fs.writeFileSync(path.join(extensionDir, "implementation.js"), [
        'import create from "./implementation-base.js";',
        "export default function(api, host) {",
        "  const instance = create(api, host);",
        "  return { ...instance, restore: async (ctx, state) => {",
        "    await instance.restore(ctx, state);",
        "    throw new Error('candidate startup failed after connecting');",
        "  } };",
        "}",
      ].join("\n"));
      await fixture.command("reload");
      assert.ok(fixture.notifications.some((notification) => notification.severity === "error"));
      assert.equal(fixture.alias(), alias);
      const answer = once(fixture.observer, "inbound");
      const result = await fixture.tool("mesh_reply", { msgId: original.msgId, message: "rollback-target-kept" });
      assert.equal(result.details?.status, "delivered");
      const [frame] = await answer;
      assert.equal(frame.replyTo, original.msgId);
      await fixture.receive("one-rollback-delivery");
      assert.equal(fixture.sent.filter((message) => message.content.includes("one-rollback-delivery")).length, 1);
    } finally { await fixture.close(); }
  });

  it("cancels an outgoing session's queued reload instead of transferring its identity to a new session", async () => {
    const fixture = await lifecycleFixture(true, { reload: true });
    try {
      const originalAlias = fixture.alias();
      fixture.running();
      fixture.updateVersion(`${MESH_VERSION}-session-test`);
      await fixture.command("reload");
      await fixture.switchTo("another", "new");
      fixture.idle();
      await fixture.emit("agent_end", { willContinue: false });
      assert.notEqual(fixture.alias(), originalAlias);
      assert.equal(fixture.identity.load("another")?.alias, fixture.alias());
      const peer = (await fixture.observer.status()).peers.find((candidate) => candidate.alias === fixture.alias());
      assert.equal(peer?.clientVersion, MESH_VERSION);
      await fixture.receive("new-session-uninterrupted");
      assert.equal(fixture.sent.filter((message) => message.content.includes("new-session-uninterrupted")).length, 1);
    } finally { await fixture.close(); }
  });

  it("preserves mixed member/observer room permissions when replacing the client", async () => {
    const fixture = await lifecycleFixture(true, { reload: true });
    try {
      await fixture.command("join audit observer");
      await fixture.observer.join("audit");
      const before = await fixture.tool("mesh_send", {
        to: "observer", room: "audit", message: "denied-before-reload",
      });
      assert.equal(before.details?.reason, "observer_readonly");
      const version = `${MESH_VERSION}-observer-test`;
      fixture.updateVersion(version);
      await fixture.command("reload");
      await waitFor(() => fixture.notifications.some((notification) => notification.message.includes(version)));
      const result = await fixture.tool("mesh_send", {
        to: "observer", room: "audit", message: "must-remain-readonly",
      });
      assert.equal(result.details?.status, "blocked");
      assert.equal(result.details?.reason, "observer_readonly");
      const received = once(fixture.observer, "inbound");
      const allowed = await fixture.tool("mesh_send", {
        to: "observer", room: "default", message: "member-room-still-writable",
      });
      assert.equal(allowed.details?.status, "delivered");
      const [frame] = await received;
      assert.equal(frame.body, "member-room-still-writable");
    } finally { await fixture.close(); }
  });

  it("keeps the loaded version and receives messages after Pi's new-session lifecycle", async () => {
    const fixture = await lifecycleFixture(false, { reload: true });
    try {
      const version = `${MESH_VERSION}-pi-warm-test`;
      fixture.updateVersion(version);
      await fixture.command("reload");
      await waitFor(() => fixture.notifications.some((notification) => notification.message.includes(version)));
      const ready = fixture.ready();
      await fixture.command("new");
      await ready;
      const peer = (await fixture.observer.status()).peers.find((candidate) => candidate.alias === fixture.alias());
      assert.equal(peer?.clientVersion, version);
      await fixture.receive("same-hot-code-after-pi-new");
      assert.equal(fixture.sent.filter((message) => message.content.includes("same-hot-code-after-pi-new")).length, 1);
    } finally { await fixture.close(); }
  });
});
