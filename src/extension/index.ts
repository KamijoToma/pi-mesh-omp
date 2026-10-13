// extension/index.ts — Pi extension entrypoint. Thin adapter: all logic
// lives in client/; this file wires lifecycle, events, ledger and transcript.
// connect is NON-blocking — tools answer `blocked` when the broker is down.
import { MeshClient } from "../client/client.js";
import { loadConfig } from "../shared/config.js";
import { runtimeDir, stateDir } from "../shared/paths.js";
import * as fs from "node:fs";
import { registerCommands } from "./commands.js";
import { MeshGuards } from "./guards.js";
import { MeshHud } from "./hud.js";
import { attachClientListeners } from "./attach.js";
import { renderLiveEntry, renderMeshInbound, renderVerdictEntry } from "./renderer.js";
import type { PersistedIdentity } from "../shared/identity-store.js";
import { identityFromClient, MeshIdentity } from "../shared/identity-store.js";
import { MeshLedger } from "./ledger.js";
import type { ExtensionAPI } from "./pi-types.js";
import { findToolConflict } from "../shared/reservations.js";
import { registerTools, type MeshRuntime } from "./tools.js";
import { providerErrorState, providerErrorStateFromMessage } from "./provider-errors.js";
import { MeshTranscript } from "./transcript.js";
import { analyzeTurn, countRejected, countToolCalls, renderVerdict, type TurnSample } from "./watchdog.js";
import type { SessionContext } from "./pi-types.js";
import { MeshActivity } from "./activity.js";
import { isOmpHost, registerHostLifecycle, reportConnectionError } from "./host.js";

export default function meshExtension(pi: ExtensionAPI): void {
  let runtime: MeshRuntime | null = null;
  let hud: MeshHud | null = null;
  const getRuntime = (): MeshRuntime | null => runtime;
  let activity: MeshActivity | null = null;
  let runWatchdog: ((message: unknown, toolResults: readonly unknown[]) => void) | undefined;
  let generation = 0;
  const installActivity = (rt: MeshRuntime): void => {
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
    });
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

  const detachSession = async (): Promise<void> => {
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
    if (rt.reserveTimers !== undefined) {
      for (const timer of rt.reserveTimers.values()) clearTimeout(timer);
      rt.reserveTimers.clear();
    }
    // rt.sessionId is the outgoing ID even when the host manager already changed.
    saveIdentity(rt);
    await rt.client.close().catch(() => {});
  };

  const startSession = async (ctx: SessionContext, handoff = false): Promise<void> => {
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
    let persisted: PersistedIdentity | null = identity.load(sessionId);
    let pendingHistory: string[] | undefined;
    if (persisted === null) {
      const pending = identity.consumePending();
      persisted = transferred ?? pending?.identity ?? null;
      pendingHistory = pending?.history;
    }
    const client = new MeshClient({
      alias: config.alias ?? persisted?.alias,
      rooms: persisted !== null && persisted.rooms.length > 0 ? persisted.rooms : config.rooms,
      initialReservations: persisted?.reservations,
      runtimeDir: rDir,
      config,
    });
    const rt: MeshRuntime = {
      client,
      ledger: new MeshLedger(sDir, config.ledgerMaxBytes),
      transcript: new MeshTranscript(sDir, config.transcript, config.transcriptRetentionDays),
      guards: new MeshGuards(),
      ctx,
      stateDir: sDir,
      runtimeDir: rDir,
      sessionId,
      identity,
      pendingHistory,
      startedAt: Date.now(),
      ledgerFailures: 0,
      transcriptFailures: 0,
      injectionFailures: 0,
      appendEntry: (type, data) => pi.appendEntry(type, data),
      interruptReceipts: config.interruptReceipts !== false,
      interruptCount: 0,
    };
    runtime = rt;
    saveIdentity(rt);
    const isCurrent = (): boolean => runtime === rt && generation === currentGeneration &&
      (rt.ctx?.sessionManager?.getSessionId() ?? "") === rt.sessionId;
    attachClientListeners(pi, rt, getHudRef, ctx, client, saveIdentity, undefined, isCurrent);
    installActivity(rt);
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
    hud.setConnecting(true);
    hud.attach(ctx);
    // Non-blocking startup; /mesh status can retry a failed connection.
    void client.connect().catch((error: unknown) => {
      if (isCurrent()) {
        hud?.setConnecting(false);
        reportConnectionError(pi, rt.ctx, error);
      }
    });
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
  pi.on("session_shutdown", detachSession);
}
