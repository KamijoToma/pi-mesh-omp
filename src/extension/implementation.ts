// Replaceable mesh implementation; the stable entry owns host registrations.
// Restored memberships reconnect non-blockingly; fresh sessions wait for /mesh join.
import { MeshClient, type MeshClientReloadState } from "../client/client.js";
import { loadConfig } from "../shared/config.js";
import { runtimeDir, stateDir } from "../shared/paths.js";
import * as fs from "node:fs";
import { registerCommands } from "./commands.js";
import { MeshGuards, type MeshGuardsReloadState } from "./guards.js";
import { MeshHud } from "./hud.js";
import { attachClientListeners, type MeshInboundReloadState } from "./attach.js";
import { renderLiveEntry, renderMeshInbound, renderVerdictEntry } from "./renderer.js";
import type { PersistedIdentity } from "../shared/identity-store.js";
import { identityFromClient, MeshIdentity } from "../shared/identity-store.js";
import { MeshLedger } from "./ledger.js";
import type { ExtensionAPI } from "./pi-types.js";
import { findToolConflict } from "../shared/reservations.js";
import { armReserveTimers, closeServeHandles, registerTools, type MeshRuntime } from "./tools.js";
import { providerErrorState, providerErrorStateFromMessage } from "./provider-errors.js";
import { MeshTranscript } from "./transcript.js";
import { analyzeTurn, countRejected, countToolCalls, renderVerdict, type TurnSample } from "./watchdog.js";
import type { SessionContext } from "./pi-types.js";
import { MeshActivity, type MeshActivityReloadState } from "./activity.js";
import { isOmpHost, registerHostLifecycle, reportConnectionError } from "./host.js";
import { MESH_VERSION } from "../shared/version.js";
import type { MeshExtensionInstance } from "./reload.js";
import { configureHostTui, type MeshHostTui } from "./tui.js";

interface MeshReloadState {
  schema: "mesh.reload.v2";
  sessionId: string;
  identity: PersistedIdentity;
  client: MeshClientReloadState;
  inbound: MeshInboundReloadState;
  activity?: MeshActivityReloadState;
  guards: MeshGuardsReloadState;
  transcriptOn: boolean;
  reserveDeadlines: [string, number][];
  pendingHistory?: string[];
  startedAt: number;
  stats: Pick<MeshRuntime, "ledgerFailures" | "transcriptFailures" | "injectionFailures" |
    "interruptCount" | "lastInterruptAt" | "lastInterruptOutcome">;
}

export default function createMeshExtension(pi: ExtensionAPI, host: MeshHostTui): MeshExtensionInstance {
  configureHostTui(host);
  let runtime: MeshRuntime | null = null;
  let hud: MeshHud | null = null;
  const getRuntime = (): MeshRuntime | null => runtime;
  let activity: MeshActivity | null = null;
  let runWatchdog: ((message: unknown, toolResults: readonly unknown[]) => void) | undefined;
  let generation = 0;
  const installActivity = (rt: MeshRuntime, restored?: MeshActivityReloadState): void => {
    if (runtime !== rt) return;
    activity?.dispose();
    const controller = new MeshActivity({
      context: () => rt.ctx,
      isCurrent: () => runtime === rt &&
        (rt.ctx?.sessionManager?.getSessionId() ?? "") === rt.sessionId,
      announce: (state) => rt.client.sendActivity(state),
      holdUntil: (until) => { rt.rateLimitedUntil = until; },
      flushHeld: () => rt.flushHeld?.(),
      observeModel: isOmpHost(pi),
    }, restored);
    activity = controller;
    rt.stopHold = () => controller.dispose();
  };

  // Tools + command are registered IMMEDIATELY (they answer `blocked` offline).
  registerTools(pi, getRuntime);
  registerCommands(pi, getRuntime, () => hud?.onLocalChange(), () => hud, installActivity);
  // live inbound entries rendered while a tool call runs (outside the
  // LLM context — zero tokens), so bursts are visible in real time.
  if (typeof pi.registerEntryRenderer === "function") {
    pi.registerEntryRenderer<{ from?: string; room?: string; body?: string; at?: string }>(
      "mesh-live",
      (entry, _options, theme) => ({
        render: (width: number) =>
          renderLiveEntry(entry.data, width, {
            fg: (color, text) => theme.fg(color, text),
            bg: (color, text) => theme.bg?.(color, text) ?? text,
            bold: (text) => theme.bold?.(text) ?? text,
          }),
        invalidate: () => {},
      }),
    );
  }
  // mesh-verdict: the colored mesh_wait_all result (display only — the
  // entry lives OUTSIDE the LLM context; the tool result carries the text).
  if (typeof pi.registerEntryRenderer === "function") {
    pi.registerEntryRenderer<{ head?: string; answers?: { to: string; response: string }[]; missing?: { to: string; msgId: string }[] }>(
      "mesh-verdict",
      (entry, _options, theme) => ({
        render: (width: number) =>
          renderVerdictEntry(entry.data, width, {
            fg: (color, text) => theme.fg(color, text),
            bg: (color, text) => theme.bg?.(color, text) ?? text,
            bold: (text) => theme.bold?.(text) ?? text,
            fgAnsi: (color) => (theme as unknown as { getFgAnsi(c: string): string }).getFgAnsi(color),
          }),
        invalidate: () => {},
      }),
    );
  }
  // colored rendering of mesh messages (simple + batches).
  if (typeof pi.registerMessageRenderer === "function") {
    pi.registerMessageRenderer<{ kind?: string; count?: number }>(
      "mesh-inbound",
      (message, _options, theme) => {
        const content = typeof message.content === "string" ? message.content : "";
        const details = message.details;
        return {
          render: (width: number) =>
            renderMeshInbound(content, details, width, {
              fg: (color, text) => theme.fg(color, text),
              bg: (color, text) => theme.bg?.(color, text) ?? text,
              bold: (text) => theme.bold?.(text) ?? text,
            }),
          invalidate: () => {},
        };
      },
    );
  }


  /** Persist the current client state as this session's identity. */
  const saveIdentity = (rt: MeshRuntime): void => {
    rt.identity.save(identityFromClient(rt.sessionId, rt.client));
  };
  /** Live HUD accessor (the module-level `hud` variable is reassigned). */
  const getHudRef = (): MeshHud | null => hud;

  const detachSession = async (socketClosed = false): Promise<void> => {
    const rt = runtime;
    runtime = null;
    generation++;
    activity?.dispose();
    activity = null;
    runWatchdog = undefined;
    const h = hud;
    hud = null;
    try {
      h?.detach();
    } catch {
      // Pi invalidates outgoing UI before replacement; teardown must still close the client.
    }
    if (rt === null) return;
    rt.markDetached?.();
    rt.stopHold?.();
    closeServeHandles(rt);
    if (rt.reserveTimers !== undefined) {
      for (const entry of rt.reserveTimers.values()) clearTimeout(entry.timer);
      rt.reserveTimers.clear();
    }
    // rt.sessionId is the outgoing ID even when the host manager already changed.
    saveIdentity(rt);
    if (!socketClosed) await rt.client.close().catch(() => {});
  };

  const startSession = async (ctx: SessionContext, handoff = false, restored?: MeshReloadState): Promise<void> => {
    const transferred = handoff && runtime !== null
      ? identityFromClient(runtime.sessionId, runtime.client)
      : undefined;
    const sessionId = ctx.sessionManager?.getSessionId() ?? "";
    const teardown = detachSession();
    const currentGeneration = generation;
    await teardown;
    if (generation !== currentGeneration || (ctx.sessionManager?.getSessionId() ?? "") !== sessionId) return;
    const sDir = stateDir();
    const rDir = runtimeDir();
    const config = loadConfig(sDir);
    const identity = new MeshIdentity(sDir);
    let persisted: PersistedIdentity | null = restored?.identity ?? identity.load(sessionId);
    let pendingHistory = restored?.pendingHistory;
    if (persisted === null) {
      const pending = identity.consumePending();
      persisted = transferred ?? pending?.identity ?? null;
      pendingHistory = pending?.history;
    }
    const client = new MeshClient({
      alias: restored === undefined ? config.alias ?? persisted?.alias : restored.identity.alias,
      rooms: persisted?.rooms ?? [],
      roomRoles: persisted?.roomRoles,
      initialReservations: persisted?.reservations,
      runtimeDir: rDir,
      config,
      strictAlias: restored !== undefined,
    });
    if (restored !== undefined) client.restoreAfterReload(restored.client);
    const rt: MeshRuntime = {
      client,
      ledger: new MeshLedger(sDir, config.ledgerMaxBytes),
      transcript: new MeshTranscript(sDir, restored?.transcriptOn ?? config.transcript, config.transcriptRetentionDays),
      guards: new MeshGuards(),
      ctx,
      stateDir: sDir,
      runtimeDir: rDir,
      sessionId,
      identity,
      pendingHistory,
      startedAt: restored?.startedAt ?? Date.now(),
      ledgerFailures: 0,
      transcriptFailures: 0,
      injectionFailures: 0,
      appendEntry: (type, data) => pi.appendEntry(type, data),
      interruptReceipts: config.interruptReceipts !== false,
      interruptCount: 0,
      ...restored?.stats,
    };
    runtime = rt;
    if (restored !== undefined) rt.guards.restoreAfterReload(restored.guards);
    saveIdentity(rt);
    const isCurrent = (): boolean => runtime === rt && generation === currentGeneration &&
      (rt.ctx?.sessionManager?.getSessionId() ?? "") === rt.sessionId;
    attachClientListeners(pi, rt, getHudRef, ctx, client, saveIdentity, undefined, isCurrent, restored?.inbound);
    installActivity(rt, restored?.activity);
    let lastSample: TurnSample | null = null;
    runWatchdog = (message, toolResults): void => {
      if (config.watchdog === false || !isCurrent()) return;
      try {
        let fileBytes: number | null = null;
        try {
          const file = rt.ctx?.sessionManager?.getSessionFile?.();
          if (file !== undefined && file !== "") fileBytes = fs.statSync(file).size;
        } catch {
          // File sampling is best-effort; count-based analysis still works.
        }
        const sample: TurnSample = {
          fileBytes,
          toolCalls: countToolCalls(message),
          rejectedCalls: countRejected(toolResults),
          at: Date.now(),
        };
        const verdict = analyzeTurn(sample, lastSample, {
          spikeBytes: config.watchdogSpikeBytes ?? 2_097_152,
          maxCalls: config.watchdogMaxCalls ?? 64,
          compactionBytes: config.watchdogCompactionBytes ?? 1_048_576,
        });
        lastSample = sample;
        if (verdict.type === "ok") return;
        if (verdict.type === "compaction") {
          rt.onCompactionDetected?.();
          return;
        }
        rt.ctx?.ui.notify(renderVerdict(verdict), "warning");
        rt.appendEntry?.("mesh-watchdog", { verdict, at: sample.at });
      } catch {
        // Diagnostics must not break the turn.
      }
    };
    hud = new MeshHud({ getRuntime });
    hud.setConnecting(client.roomRoles.size > 0);
    hud.attach(ctx);
    if (restored !== undefined && client.roomRoles.size > 0) {
      await client.connect();
      if (!isCurrent()) throw new Error("mesh reload cancelled by session change");
      for (const [pattern, expiresAt] of restored.reserveDeadlines) {
        if (client.reservations.some((reservation) => reservation.pattern === pattern)) {
          armReserveTimers(rt, [pattern], Math.max(0, expiresAt - Date.now()));
        }
      }
    } else if (client.roomRoles.size > 0) {
      // Only an existing membership authorizes automatic connection/retry.
      void client.connect().catch((error: unknown) => {
        if (isCurrent()) {
          hud?.setConnecting(false);
          reportConnectionError(pi, rt.ctx, error);
        }
      });
    }
  };

  const current = (ctx: SessionContext): MeshRuntime | null => {
    const rt = runtime;
    if (rt === null || (ctx.sessionManager?.getSessionId() ?? "") !== rt.sessionId) return null;
    rt.ctx = ctx;
    return rt;
  };

  pi.on("session_start", (_event, ctx) => startSession(ctx));
  registerHostLifecycle(pi, {
    switchSession: startSession,
    settled: (ctx) => { if (current(ctx) !== null) activity?.settled(); },
    modelSelected: (ctx) => { if (current(ctx) !== null) activity?.recovered(); },
    beforeFork: () => {
      const rt = runtime;
      if (rt !== null) rt.identity.savePending(identityFromClient(rt.sessionId, rt.client));
    },
  });
  pi.on("input", (_event, ctx) => {
    const rt = current(ctx);
    if (rt === null) return;
    activity?.checkModel();
    if (ctx.isIdle?.() !== false) rt.deferredInbox?.queueForPrompt();
  });
  pi.on("before_agent_start", (_event, ctx) => {
    const rt = current(ctx);
    if (rt === null) return;
    activity?.checkModel();
    rt.deferredInbox?.promptStarting();
  });
  pi.on("agent_start", (_event, ctx) => {
    const rt = current(ctx);
    if (rt === null) return;
    activity?.beginRun();
    rt.deferredInbox?.agentStarting();
  });
  pi.on("turn_end", (event, ctx) => {
    if (current(ctx) === null) return;
    const turn = event as { message?: { stopReason?: string; errorMessage?: string }; toolResults?: unknown[] };
    runWatchdog?.(turn.message, turn.toolResults ?? []);
    const message = turn.message;
    if (message?.stopReason === "error") {
      const state = providerErrorStateFromMessage(message.errorMessage ?? "");
      if (state !== undefined) activity?.failed(state);
    } else if (message?.stopReason === "stop" || message?.stopReason === "toolUse") {
      activity?.successfulTurn();
    }
  });
  pi.on("after_provider_response", (event, ctx) => {
    if (current(ctx) === null) return;
    const response = event !== null && typeof event === "object" && "status" in event ? event.status : undefined;
    const state = providerErrorState(typeof response === "number" ? response : 0);
    if (state !== undefined) activity?.failed(state);
  });
  pi.on("tool_call", (event, ctx) => {
    const rt = current(ctx);
    if (rt === null) return;
    activity?.toolCalled();
    if (!rt.client.isOnline() || (event.toolName !== "edit" && event.toolName !== "write")) return;
    const conflict = findToolConflict(event.input, rt.client.peerReservationMap, rt.client.alias, rt.client.reservationTtlMs);
    if (conflict === undefined) return;
    const { path, alias: holder, reservation: res } = conflict;
    const lines = [path, `Reserved by: @${holder}`];
    if (res.reason !== undefined && res.reason !== "") lines.push(`Reason: "${res.reason}"`);
    if (res.since !== undefined) lines.push(`Since: ${res.since}`);
    lines.push("", `Coordinate via mesh_send({ to: "${holder}", message: "..." }) — or wait for mesh_release.`);
    return { block: true, reason: lines.join("\n") };
  });

  pi.on("tool_result", (_event, ctx) => {
    const rt = current(ctx);
    try {
      rt?.batcher?.flushNow();
    } catch {
      // A send during teardown must not break the handler.
    }
  });
  pi.on("session_shutdown", () => detachSession());
  return {
    reloadApiVersion: 2,
    version: MESH_VERSION,
    isReloadSafe: () => runtime !== null && runtime.client.isReloadSafe() &&
      runtime.isInboundReloadSafe?.() !== false && (runtime.pendingToolOperations?.size ?? 0) === 0,
    suspend: async () => {
      const rt = runtime;
      if (rt === null || rt.captureInboundForReload === undefined) {
        throw new Error("mesh: no initialized runtime to transfer");
      }
      const currentGeneration = generation;
      rt.pauseForReload?.();
      hud?.detach();
      const activityState = activity?.snapshotForReload();
      activity?.dispose();
      const reserveDeadlines: [string, number][] = [];
      for (const [pattern, entry] of rt.reserveTimers ?? []) {
        reserveDeadlines.push([pattern, entry.expiresAt]);
        clearTimeout(entry.timer);
      }
      rt.reserveTimers?.clear();
      // Offers do NOT survive reload: the URL points at a listener in the
      // outgoing module graph. Close them so peers get refused sockets
      // instead of a URL that outlives its server.
      closeServeHandles(rt);
      // Keep paused listeners alive until the old socket has drained.
      await rt.client.close();
      if (runtime !== rt || generation !== currentGeneration) {
        throw new Error("mesh reload cancelled by session change");
      }
      const state: MeshReloadState = {
        schema: "mesh.reload.v2",
        sessionId: rt.sessionId,
        identity: identityFromClient(rt.sessionId, rt.client),
        client: rt.client.snapshotForReload(),
        inbound: rt.captureInboundForReload(),
        activity: activityState,
        guards: rt.guards.snapshotForReload(),
        transcriptOn: rt.transcript.isEnabled(),
        reserveDeadlines,
        pendingHistory: rt.pendingHistory,
        startedAt: rt.startedAt,
        stats: {
          ledgerFailures: rt.ledgerFailures,
          transcriptFailures: rt.transcriptFailures,
          injectionFailures: rt.injectionFailures,
          interruptCount: rt.interruptCount,
          lastInterruptAt: rt.lastInterruptAt,
          lastInterruptOutcome: rt.lastInterruptOutcome,
        },
      };
      await detachSession(true);
      return state;
    },
    restore: async (ctx, value) => {
      if (value === null || typeof value !== "object" || !("schema" in value) ||
          value.schema !== "mesh.reload.v2") {
        throw new Error("incompatible mesh reload state; restart the host for this version");
      }
      const state = value as MeshReloadState;
      if (state.sessionId !== (ctx.sessionManager?.getSessionId() ?? "")) {
        throw new Error("mesh reload cancelled by session change");
      }
      await startSession(ctx, false, state);
      if (runtime === null) throw new Error("mesh reload cancelled by session change");
    },
    activate: () => {
      activity?.announceCurrent();
      runtime?.activateAfterReload?.();
    },
    dispose: () => detachSession(),
  };
}
