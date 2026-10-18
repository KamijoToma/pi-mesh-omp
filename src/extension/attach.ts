// extension/attach.ts — client→session wiring, shared by session_start and
// /mesh reset: both need a fully-wired MeshClient, and reset must be
// able to REPLACE the client in-place without a pi reload.
import type { MeshClient, WelcomeInfo } from "../client/client.js";
import type { MeshFrame } from "../protocol/envelope.js";
import { buildBatchMessage, bypassesBatch, batchDetails, InboundBatcher } from "./batcher.js";
import { DeferredInbox, type DeferredInboxReloadState } from "./deferred-inbox.js";
import { classifyInbound } from "./inbound-policy.js";
import type { MeshHud } from "./hud.js";
import { tagInboundReplyChain, recordInboundDelivery, injectInbound, handleInboundFrame, type FormatOpts, type InjectOpts } from "./inbound.js";
import { ReplyHintTracker, type ReplyHintReloadState } from "./reply-hints.js";
import type { ExtensionAPI, InboundMessage, SessionContext } from "./pi-types.js";
import { interruptToolOperations, type MeshRuntime } from "./tools.js";

export interface MeshInboundReloadState {
  deferred: DeferredInboxReloadState;
  held: { frame: MeshFrame; matchedReply: boolean; interruptedWaits?: boolean }[];
  batched: MeshFrame[];
  fullContextSent: boolean;
  lastPeersOnline: string[];
  replyHints: ReplyHintReloadState;
  liveCooldowns: [string, number][];
}

/** live-entry previews shown while the agent is busy (cooldown). */
const LIVE_COOLDOWN_MS = 1_500;

/** Format opts for one inbound frame (compact mode + gated hints). */
export function formatOptsFor(
  frame: MeshFrame,
  verbose: boolean,
  homeRoom: string,
  hints: ReplyHintTracker,
): FormatOpts {
  return {
    verbose,
    homeRoom,
    showReplyHint: hints.shouldShow(frame.from),
  };
}

/** Compact reconnect diff line (~30 tokens vs ~500 for the full block). */
export function buildReconnectDiff(prev: readonly string[], next: readonly string[]): string {
  const before = new Set(prev);
  const after = new Set(next);
  const joined = [...after].filter((a) => !before.has(a));
  const left = [...before].filter((b) => !after.has(b));
  const parts: string[] = [];
  if (joined.length > 0) parts.push(`+${joined.map((a) => `@${a}`).join(", ")}`);
  if (left.length > 0) parts.push(`−${left.map((a) => `@${a}`).join(", ")}`);
  const chg = parts.length > 0 ? ` Peers: ${parts.join(" · ")}.` : " No peer changes.";
  return `[mesh] reconnected${chg} Rooms: ${after.size > 0 ? "online" : "none"} — full status: mesh_status.`;
}

/** Public, process-local identity bridge for other extensions. */
export function exposeAlias(pi: ExtensionAPI, alias: string, rooms: readonly string[]): void {
  (globalThis as Record<symbol, unknown>)[Symbol.for("pi-mesh:alias")] = alias;
  pi.events?.emit("mesh:alias", { alias, rooms: [...rooms] });
}

/**
 * Attach every client event handler to `client`. `rt` is the CURRENT runtime
 * object (its `.client` field may be replaced later by /mesh reset — the
 * handlers always read the runtime they were attached with, so the reset path
 * re-attaches on the new client).
 */
export function attachClientListeners(
  pi: ExtensionAPI,
  rt: MeshRuntime,
  getHud: () => MeshHud | null,
  ctx: SessionContext,
  client: MeshClient,
  saveIdentity: (rt: MeshRuntime) => void,
  /** min gap between two live entries of the SAME agent (anti-flood). */
  liveCooldownMs: number = LIVE_COOLDOWN_MS,
  isCurrent: () => boolean = () =>
    (rt.ctx?.sessionManager?.getSessionId() ?? rt.sessionId) === rt.sessionId,
  restored?: MeshInboundReloadState,
): void {
  // D41: after /reload, session switch or replacement, the OLD client may
  // still receive frames while the pi/ctx it captured is STALE — calling
  // pi.* then throws "extension ctx is stale" as an uncaughtException and
  // kills pi. `detached` flips at session_shutdown; every pi/ctx touch in
  // client handlers goes through guarded try/catch as belt-and-braces.
  let detached = false;
  let paused = restored !== undefined;
  let fullContextSent = restored?.fullContextSent ?? false;
  let lastPeersOnline = new Set(restored?.lastPeersOnline);
  const hintTracker = new ReplyHintTracker();
  if (restored !== undefined) hintTracker.restoreAfterReload(restored.replyHints);
  const liveCooldowns = new Map<string, number>(restored?.liveCooldowns);
  const pendingInjections = new Set<MeshFrame>();
  rt.isInboundReloadSafe = () => pendingInjections.size === 0;
  const active = (): boolean => !detached && isCurrent() && rt.client === client;
  rt.isCurrent = active;
  const guarded = (fn: () => void): void => {
    if (!active()) return;
    try {
      fn();
    } catch {
      // stale ctx / shutting down — the event belongs to the old session
    }
  };
  // Force delivery can keep polling after the initial guarded call returns.
  // Keep every later send/abort scoped to this runtime, not the next session.
  const deliveryHost: Pick<ExtensionAPI, "sendMessage"> = {
    sendMessage: (message, options) => {
      if (!active()) throw new Error("mesh inbound session is no longer active");
      pi.sendMessage(message, options);
    },
  };
  const deliveryContext: SessionContext = {
    cwd: ctx.cwd,
    ui: ctx.ui,
    abort: ctx.abort === undefined ? undefined : () => {
      if (active()) (rt.ctx ?? ctx).abort?.();
    },
    isIdle: ctx.isIdle === undefined ? undefined : () => {
      if (!active()) return true;
      try {
        return (rt.ctx ?? ctx).isIdle?.() !== false;
      } catch {
        return true; // Let the guarded host send reject a stale context.
      }
    },
  };

  const deps = {
    ledger: rt.ledger,
    transcript: rt.transcript,
    selfAlias: client.alias,
    counters: rt,
    read: (msgId: string, from: string) => client.sendRead(msgId, from),
    isReplyToReply: (replyTo: string) => client.isReplyToReply(replyTo),
    onSettled: (frame: MeshFrame) => { pendingInjections.delete(frame); },
  };
  // batch inbound messages over a short window → ONE injection (one
  // turn) for a burst; force/remind bypass the batcher (immediate delivery).
  const batcher = new InboundBatcher(
    client.inboundBatchMs,
    client.inboundBatchMaxHoldMs,
    () => rt.ctx?.isIdle?.() === false, // busy = a turn is running (e.g. sleep)
    (frames: MeshFrame[]) => {
      if (!active()) return;
      const first = frames[0];
      if (first === undefined) return;
      const batch = buildBatchMessage(frames, (frame) =>
        formatOptsFor(frame, client.contextVerbosity === "full", client.homeRoom, hintTracker),
      );
      const message: InboundMessage = {
        customType: "mesh-inbound", content: batch.content, display: true, details: batchDetails(frames),
      };
      pendingInjections.add(first);
      handleInboundFrame(deliveryHost, deliveryContext, first, deps, () => {
        if (!active()) return;
        deliveryHost.sendMessage(message, { triggerTurn: true, deliverAs: batch.deliverAs });
        for (const frame of frames) recordInboundDelivery(frame, deps);
      });
  },
  );
  const deferredInbox = new DeferredInbox(deliveryHost, (frame) =>
    formatOptsFor(frame, client.contextVerbosity === "full", client.homeRoom, hintTracker),
  () => guarded(() => getHud()?.refresh()), (frames) => {
    for (const frame of frames) recordInboundDelivery(frame, deps);
  });
  if (restored !== undefined) deferredInbox.restoreAfterReload(restored.deferred);
  rt.deferredInbox = deferredInbox;
  guarded(() => getHud()?.refresh());
  // rate-limited hold: while the provider rejects turns (429), inbound
  // frames are NOT injected — every injection would burn another failed
  // turn. They are queued and delivered when the hold expires.
  const heldFrames: { frame: MeshFrame; matchedReply: boolean; interruptedWaits?: boolean }[] = [...(restored?.held ?? [])];
  const pausedBatches: MeshFrame[] = [...(restored?.batched ?? [])];
  const wakeForInput = (frame: MeshFrame, interruptedWaits = false): boolean => {
    if (frame.receipt === true || ("__replyChain" in frame && frame.__replyChain === true)) return false;
    const operationWoke = interruptToolOperations(rt, "inbound_message", frame.id);
    const taskWoke = frame.priority !== "force" && client.interruptWaits("inbound_message", frame.id);
    return interruptedWaits || operationWoke || taskWoke;
  };
  const deliver = (frame: MeshFrame, matchedReply: boolean, interruptedWaits = false): void => {
    tagInboundReplyChain(frame, deps);
    getHud()?.refresh();
    if (classifyInbound(frame, client.alias, client.inboundBroadcasts, matchedReply) === "deferred") {
      deferredInbox.push(frame);
      return;
    }
    const wakeWaiting = wakeForInput(frame, interruptedWaits);
    const opts: FormatOpts = {
      ...formatOptsFor(frame, client.contextVerbosity === "full", client.homeRoom, hintTracker),
    };
    if (wakeWaiting || bypassesBatch(frame) || client.inboundBatchMs <= 0) {
      batcher.flushNow(); // deliver any pending batch first (ordering)
      const injectOpts: InjectOpts = {
        ...opts,
        wakeWaiting,
        replyChain: "__replyChain" in frame && frame.__replyChain === true,
        onInjected: () => recordInboundDelivery(frame, deps),
      };
      // force+interrupt: wire the honest report (receipt + local notify +
      // memory-only counters) before injection runs its repeater path
      if (frame.interrupt === true) {
        injectOpts.onInterruptReport = (report) => {
          if (!active()) return;
          rt.interruptCount = (rt.interruptCount ?? 0) + 1;
          rt.lastInterruptAt = new Date().toISOString();
          rt.lastInterruptOutcome = report.outcome;
          guarded(() => {
            (rt.ctx ?? ctx).ui.notify(`⚠ turn interrupted by force from @${frame.from ?? "?"}`, "warning");
          });
          if (rt.interruptReceipts !== false && frame.id !== undefined) {
            const wording =
              report.outcome === "aborted"
                ? `⚠ ${frame.id} interrupted: turn aborted, message delivered`
                : report.outcome === "already_idle"
                  ? `⚠ ${frame.id} interrupt: already idle, message delivered`
                  : report.outcome === "abort_unavailable"
                    ? `⚠ ${frame.id} interrupt: abort unavailable on this host, message queued`
                    : `⚠ ${frame.id} interrupt: still busy after ${report.aborts} aborts, message queued`;
            void client.reply(frame.id, wording, { receipt: true }).catch(() => {
              // best effort — the delivery itself already happened
            });
          }
        };
      }
      if (frame.priority !== "force") {
        pendingInjections.add(frame);
        handleInboundFrame(deliveryHost, deliveryContext, frame, deps, () => {
          if (active()) injectInbound(deliveryHost, deliveryContext, frame, injectOpts);
        });
        return;
      }
      guarded(() => {
        let completed = false;
        const injectionHost: Pick<ExtensionAPI, "sendMessage"> = {
          sendMessage: (message, options) => {
            try {
              deliveryHost.sendMessage(message, options);
            } finally {
              completed = true;
              pendingInjections.delete(frame);
            }
          },
        };
        const result = injectInbound(injectionHost, deliveryContext, frame, injectOpts);
        if (result.aborted && !completed) pendingInjections.add(frame);
      });
      return;
    }
    // while the agent is busy (sleep/long tool), the frame is HELD for
    // the batch — show it LIVE in the conversation right now (entry outside
    // the LLM context) so the burst is visible in real time.
    // at most one live entry per agent per cooldown — a 30-reply burst
    // shows a representative preview instead of flooding the conversation
    // (the batch carries the full set at the end).
    if (rt.ctx?.isIdle?.() === false) {
      const from = frame.from ?? "";
      const last = liveCooldowns.get(from) ?? 0;
      const now = Date.now();
      if (now - last >= liveCooldownMs) {
        liveCooldowns.set(from, now);
        if (liveCooldowns.size > 128) liveCooldowns.clear(); // bound
        guarded(() => {
          pi.appendEntry("mesh-live", {
            from: frame.from,
            room: frame.room,
            priority: frame.priority,
            body: frame.body,
            at: frame.ts,
          });
        });
      }
    }
    batcher.push(frame);
  };
  // flush everything queued while rate-limited (called at hold expiry)
  rt.flushHeld = (): void => {
    if (!active() || paused) return;
    const frames = heldFrames.splice(0);
    for (const { frame, matchedReply, interruptedWaits } of frames) deliver(frame, matchedReply, interruptedWaits);
  };
  client.on("inbound", (frame: MeshFrame, meta?: { matchedReply?: boolean; interruptedWaits?: boolean }) => {
    if (!active()) return;
    const matchedReply = meta?.matchedReply === true;
    if (paused) {
      heldFrames.push({ frame, matchedReply, interruptedWaits: meta?.interruptedWaits });
      return;
    }
    if (frame.priority !== "force" && rt.rateLimitedUntil !== undefined && rt.rateLimitedUntil > Date.now()) {
      tagInboundReplyChain(frame, deps);
      const wake = classifyInbound(frame, client.alias, client.inboundBroadcasts, matchedReply) === "deferred"
        ? false : wakeForInput(frame, meta?.interruptedWaits);
      heldFrames.push({ frame, matchedReply, interruptedWaits: wake });
      return;
    }
    deliver(frame, matchedReply, meta?.interruptedWaits);
  });
  rt.batcher = batcher;
  rt.clearInbound = () => {
    heldFrames.length = 0;
    pausedBatches.length = 0;
    pendingInjections.clear();
    batcher.clear();
    deferredInbox.clear();
    lastPeersOnline.clear();
    liveCooldowns.clear();
    fullContextSent = false;
  };
  client.on("broker_restart", () => {
    guarded(() => (rt.ctx ?? ctx).ui.notify(
      "mesh broker restarted: peers will re-register; the previous broker's in-memory mailbox is not recovered",
      "warning",
    ));
  });
  rt.pauseForReload = () => {
    paused = true;
    pausedBatches.push(...batcher.takeForReload());
  };
  rt.captureInboundForReload = () => ({
    deferred: deferredInbox.snapshotForReload(),
    held: [...heldFrames],
    batched: [...pausedBatches],
    fullContextSent,
    lastPeersOnline: [...lastPeersOnline],
    replyHints: hintTracker.snapshotForReload(),
    liveCooldowns: [...liveCooldowns],
  });
  rt.activateAfterReload = () => {
    if (!active()) return;
    paused = false;
    for (const frame of pausedBatches) batcher.push(frame);
    pausedBatches.length = 0;
    if (rt.rateLimitedUntil === undefined || rt.rateLimitedUntil <= Date.now()) rt.flushHeld?.();
  };

  // presence → appendEntry ONLY, no turn
  client.on("presence", (frame: MeshFrame) => {
    if (!active()) return;
    guarded(() => {
      pi.appendEntry("mesh", {
        kind: "mesh-presence",
        alias: frame.from,
        status: frame.status,
        room: frame.room,
        ts: frame.ts,
      });
    });
    getHud()?.scheduleStatusRefresh(); // debounced ≤1/s trailing (hello floods)
  });

  client.on("ready", (welcome: WelcomeInfo) => {
    if (!active()) return;
    guarded(() => exposeAlias(pi, client.alias, client.rooms));
    getHud()?.setConnecting(false);
    getHud()?.fetchStatus(); // fire-and-forget, never blocks session_start
  // persist identity as soon as we are connected (covers the very
  // first connect AND every reconnect/rename/reset). Disk-only — safe.
    saveIdentity(rt);
  // identity: tell the agent who it is, once per connection, so it
  // never has to guess its alias (the old file-based mesh had agents
  // confusing each other's identities). display:false keeps it out of
  // the UI; triggerTurn:false avoids an extra turn. The context keeps the
  // FULL picture: peers sharing a room are listed first, the other
  // sessions (with their rooms) after — the agent must know who is where.
  //
  // M2: the FULL block goes out ONCE per session; reconnects (and
  // renames) send a one-line diff instead — measured in cs-room: the
  // master received the ~500-token block 5×, pure context duplication.
    const othersNow = welcome.peers.filter((p) => p.alias !== client.alias).map((p) => p.alias);
    if (fullContextSent) {
      guarded(() => pi.sendMessage(
        {
          customType: "mesh-context",
          content: buildReconnectDiff([...lastPeersOnline], othersNow),
          display: false,
        },
        { triggerTurn: false },
      ));
      lastPeersOnline = new Set(othersNow);
      return;
    }
    fullContextSent = true;
    lastPeersOnline = new Set(othersNow);
  // watchdog hook: a compaction wipes the context — re-teach everything
  // (full block + reply hints) exactly when it was lost.
    rt.onCompactionDetected = () => {
      if (!active()) return;
      fullContextSent = false;
      hintTracker.reset();
    };
    const roomList = (welcome.rooms.length > 0 ? welcome.rooms.join(",") : "default");
    const myRooms = new Set(roomList.split(","));
    const others = welcome.peers.filter((p) => p.alias !== client.alias);
    const online = others.filter((p) => p.rooms.some((r) => myRooms.has(r)));
    const far = others.filter((p) => !p.rooms.some((r) => myRooms.has(r)));
  // D40: peers on OTHER machines are tagged with their connection origin
  // (via) so the agent knows who is remote (e.g. a MacBook over the LAN).
    const viaTag = (p: { via?: string }): string =>
      p.via !== undefined ? ` [${p.via} — other machine]` : "";
    const peerLine =
      online.length > 0
        ? `Online peers: ${online.map((p) => `@${p.alias}${viaTag(p)}`).join(", ")}.`
        : "Online peers: (none).";
    const farLine =
      far.length > 0
        ? ` Other sessions: ${far
            .slice(0, 12)
            .map((p) => `@${p.alias} (${p.rooms.join(",") || "?"})${viaTag(p)}`)
            .join(", ")}${far.length > 12 ? ` (+${far.length - 12} more)` : ""}.`
        : "";
    let context =
      `[mesh] you are @${client.alias} (rooms: ${roomList}). ` +
      `Your alias is stable across /reload; use mesh_alias {alias: "reviewer"} or /mesh alias to choose a recognizable name. ` +
      `/mesh reset gives a fresh alias; /mesh new hands the identity over — if in doubt, mesh_status shows your alias. ` +
      `${peerLine}${farLine} ` +
      `mesh_send/mesh_reply to talk, mesh_status for a live snapshot, ` +
      `mesh_reserve to claim files before editing them. ` +
  // the general orchestrator pattern — launch bursts, wait_all once
      `Mission bursts: mesh_send(..., awaitReply: true, block: false) then ` +
      `mesh_wait_all for the group verdict — never mesh_history to check. ` +
      `ESC cancels a pending mesh_wait_all (verdict CANCELLED, missions stay reportable). ` +
      `mesh_send replyTo: [aliases] designates who receives the reply instead of you.`;
  // a /mesh new handoff may carry the previous session's history —
  // inject it as context so the fresh conversation keeps the thread.
    if (rt.pendingHistory !== undefined && rt.pendingHistory.length > 0) {
      context += `\n\n[mesh] transferred history from the previous session:\n${rt.pendingHistory.join("\n")}`;
    }
    guarded(() => {
      pi.sendMessage(
        {
          customType: "mesh-context",
          content: context,
          display: false,
        },
        { triggerTurn: false },
      );
    });
  });
  client.on("renamed", () => {
    if (!active()) return;
    guarded(() => exposeAlias(pi, client.alias, client.rooms));
    saveIdentity(rt); // disk-only
  });
  client.on("alias_fallback", ({ from, to }: { from: string; to: string }) => {
    if (!active()) return;
  // Another live peer holds our persisted alias (e.g. crashed session) —
  // we took a random one; persist it so the next reload does not fight
  // for the same alias again.
    saveIdentity(rt); // disk-only
    guarded(() => {
      (rt.ctx ?? ctx).ui.notify(`mesh: alias @${from} taken — now connected as @${to}`, "warning");
    });
  });
  client.on("closed", () => { if (active()) getHud()?.onClosed(); });
  client.on("pending", () => guarded(() => getHud()?.refresh()));

  // async drop receipt: the broker dropped one of OUR messages from the
  // recipient's offline mailbox. The client
  // already settled its mission; wake any other passive wait so the explicit
  // failure reaches the model without waiting for an unrelated task.
  client.on("dropped_offline", (frame: MeshFrame) => {
    if (!active()) return;
    const target = frame.to ?? "?";
    pendingInjections.add(frame);
    handleInboundFrame(deliveryHost, deliveryContext, frame, deps, () => {
      if (!active()) return;
      const operationWoke = interruptToolOperations(rt, "dropped_offline", frame.id);
      const taskWoke = client.interruptWaits("dropped_offline", frame.id);
      deliveryHost.sendMessage(
        {
          customType: "mesh-inbound",
          content:
            `[mesh] drop notice: message ${frame.id} to @${target} was dropped from the ` +
            `offline mailbox and is no longer queued. Check whether the work ` +
            `still needs to be reissued.`,
          display: true,
          details: { kind: "mesh-drop-notice", msgId: frame.id, to: target },
        },
        { triggerTurn: true, deliverAs: operationWoke || taskWoke ? "steer" : "followUp" },
      );
    });
    try {
      rt.ledger.append({
        event: "dropped_offline",
        id: frame.id,
        to: target,
        room: frame.room,
      });
    } catch {
      // best effort — the inline notice already went out
    }
  });

  // D41: flip the guard — called from session_shutdown before client.close().
  // Frames still in flight on the old socket are then dropped silently
  // instead of reaching the stale pi/ctx.
  rt.markDetached = () => {
    detached = true;
    interruptToolOperations(rt, "session_closed");
    heldFrames.length = 0;
    pausedBatches.length = 0;
    pendingInjections.clear();
    deferredInbox.clear();
    // Drain only into the detached guard: cancels batching timers without injection.
    batcher.flushNow();
  };
}
