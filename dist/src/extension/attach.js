// extension/attach.ts — client→session wiring, shared by session_start and
// /mesh reset: both need a fully-wired MeshClient, and reset must be
// able to REPLACE the client in-place without a pi reload.
import * as fs from "node:fs";
import { buildBatchMessage, bypassesBatch, batchDetails, InboundBatcher } from "./batcher.js";
import { DeferredInbox } from "./deferred-inbox.js";
import { classifyInbound } from "./inbound-policy.js";
import { tagInboundReplyChain, recordInboundDelivery, injectInbound } from "./inbound.js";
import { ReplyHintTracker } from "./reply-hints.js";
/** live-entry previews shown while the agent is busy (cooldown). */
const LIVE_COOLDOWN_MS = 1_500;
/** Format opts for one inbound frame (compact mode + gated hints). */
export function formatOptsFor(frame, verbose, homeRoom, hints) {
    return {
        verbose,
        homeRoom,
        showReplyHint: hints.shouldShow(frame.from),
    };
}
/** Compact reconnect diff line (~30 tokens vs ~500 for the full block). */
export function buildReconnectDiff(prev, next) {
    const before = new Set(prev);
    const after = new Set(next);
    const joined = [...after].filter((a) => !before.has(a));
    const left = [...before].filter((b) => !after.has(b));
    const parts = [];
    if (joined.length > 0)
        parts.push(`+${joined.map((a) => `@${a}`).join(", ")}`);
    if (left.length > 0)
        parts.push(`−${left.map((a) => `@${a}`).join(", ")}`);
    const chg = parts.length > 0 ? ` Peers: ${parts.join(" · ")}.` : " No peer changes.";
    return `[mesh] reconnected${chg} Rooms: ${after.size > 0 ? "online" : "none"} — full status: mesh_status.`;
}
/** Public, process-local identity bridge for other extensions. */
export function exposeAlias(pi, alias, rooms) {
    globalThis[Symbol.for("pi-mesh:alias")] = alias;
    pi.events?.emit("mesh:alias", { alias, rooms: [...rooms] });
}
/** session name keeps the first user message after the mesh identity. */
const SESSION_NAME_MSG_MAX = 80;
const SESSION_NAME_SCAN_BYTES = 256 * 1024; // first 256 KiB of the session file
/**
 * Read the FIRST user message of the session file (what pi's /resume shows
 * when no name is set). Best effort; the scan stops at the first user text.
 */
function firstUserMessage(sessionFile) {
    if (sessionFile === undefined || sessionFile === "")
        return undefined;
    let head;
    try {
        head = fs.readFileSync(sessionFile, "utf8").slice(0, SESSION_NAME_SCAN_BYTES);
    }
    catch {
        return undefined;
    }
    for (const line of head.split("\n")) {
        if (line.trim() === "")
            continue;
        let entry;
        try {
            entry = JSON.parse(line);
        }
        catch {
            continue;
        }
        if (entry.type !== "message" || entry.message?.role !== "user")
            continue;
        const content = entry.message.content;
        const text = Array.isArray(content)
            ? content
                .map((c) => (typeof c === "object" && c !== null && c.type === "text" ? c.text ?? "" : ""))
                .join(" ")
                .trim()
            : typeof content === "string"
                ? content.trim()
                : "";
        if (text.length > 0)
            return text.length > SESSION_NAME_MSG_MAX ? `${text.slice(0, SESSION_NAME_MSG_MAX - 1)}…` : text;
    }
    return undefined;
}
/**
 * name the pi session so /resume and the session selector show the mesh
 * identity AND the conversation: `mesh @agent-1 · cs-room — <first message>`.
 * A user-defined name is NEVER overwritten; the mesh name is refreshed on
 * ready/rename/join/leave. The first message is kept exactly like pi's
 * default display (session.name ?? session.firstMessage) so nothing is lost.
 */
export function updateSessionName(pi, rt) {
    try {
        if (typeof pi.getSessionName !== "function" || typeof pi.setSessionName !== "function")
            return;
        const current = pi.getSessionName();
        if (current !== undefined && current !== "" && !current.startsWith("mesh "))
            return;
        const rooms = rt.client.rooms.length > 0 ? ` · ${rt.client.rooms.join(",")}` : "";
        const first = firstUserMessage(rt.ctx?.sessionManager?.getSessionFile?.());
        pi.setSessionName(`mesh @${rt.client.alias}${rooms}${first !== undefined ? ` — ${first}` : ""}`);
    }
    catch {
        // best effort
    }
}
/**
 * Attach every client event handler to `client`. `rt` is the CURRENT runtime
 * object (its `.client` field may be replaced later by /mesh reset — the
 * handlers always read the runtime they were attached with, so the reset path
 * re-attaches on the new client).
 */
export function attachClientListeners(pi, rt, getHud, ctx, client, saveIdentity, 
/** min gap between two live entries of the SAME agent (anti-flood). */
liveCooldownMs = LIVE_COOLDOWN_MS, isCurrent = () => (rt.ctx?.sessionManager?.getSessionId() ?? rt.sessionId) === rt.sessionId) {
    // D41: after /reload, session switch or replacement, the OLD client may
    // still receive frames while the pi/ctx it captured is STALE — calling
    // pi.* then throws "extension ctx is stale" as an uncaughtException and
    // kills pi. `detached` flips at session_shutdown; every pi/ctx touch in
    // client handlers goes through guarded try/catch as belt-and-braces.
    let detached = false;
    let fullContextSent = false;
    let lastPeersOnline = new Set();
    const hintTracker = new ReplyHintTracker();
    const liveCooldowns = new Map();
    const active = () => !detached && isCurrent() && rt.client === client;
    const guarded = (fn) => {
        if (!active())
            return;
        try {
            fn();
        }
        catch {
            // stale ctx / shutting down — the event belongs to the old session
        }
    };
    // Force delivery can keep polling after the initial guarded call returns.
    // Keep every later send/abort scoped to this runtime, not the next session.
    const deliveryHost = {
        sendMessage: (message, options) => {
            if (!active())
                throw new Error("mesh inbound session is no longer active");
            pi.sendMessage(message, options);
        },
    };
    const deliveryContext = {
        cwd: ctx.cwd,
        ui: ctx.ui,
        abort: ctx.abort === undefined ? undefined : () => {
            if (active())
                (rt.ctx ?? ctx).abort?.();
        },
        isIdle: ctx.isIdle === undefined ? undefined : () => {
            if (!active())
                return true;
            try {
                return (rt.ctx ?? ctx).isIdle?.() !== false;
            }
            catch {
                return true; // Let the guarded host send reject a stale context.
            }
        },
    };
    const deps = {
        ledger: rt.ledger,
        transcript: rt.transcript,
        selfAlias: client.alias,
        counters: rt,
        read: (msgId, from) => client.sendRead(msgId, from),
        isReplyToReply: (replyTo) => client.isReplyToReply(replyTo),
    };
    // batch inbound messages over a short window → ONE injection (one
    // turn) for a burst; force/remind bypass the batcher (immediate delivery).
    const batcher = new InboundBatcher(client.inboundBatchMs, client.inboundBatchMaxHoldMs, () => rt.ctx?.isIdle?.() === false, // busy = a turn is running (e.g. sleep)
    (frames) => {
        if (!active())
            return;
        const batch = buildBatchMessage(frames, (f) => formatOptsFor(f, client.contextVerbosity === "full", client.homeRoom, hintTracker));
        guarded(() => {
            pi.sendMessage({
                customType: "mesh-inbound",
                content: batch.content,
                display: true,
                details: batchDetails(frames),
            }, { triggerTurn: true, deliverAs: batch.deliverAs });
            for (const frame of frames)
                recordInboundDelivery(frame, deps);
        });
    });
    const deferredInbox = new DeferredInbox(deliveryHost, (frame) => formatOptsFor(frame, client.contextVerbosity === "full", client.homeRoom, hintTracker), () => guarded(() => getHud()?.refresh()), (frames) => {
        for (const frame of frames)
            recordInboundDelivery(frame, deps);
    });
    rt.deferredInbox = deferredInbox;
    guarded(() => getHud()?.refresh());
    // rate-limited hold: while the provider rejects turns (429), inbound
    // frames are NOT injected — every injection would burn another failed
    // turn. They are queued and delivered when the hold expires.
    const heldFrames = [];
    const deliver = (frame, matchedReply) => {
        tagInboundReplyChain(frame, deps);
        getHud()?.refresh();
        if (classifyInbound(frame, client.alias, client.inboundBroadcasts, matchedReply) === "deferred") {
            deferredInbox.push(frame);
            return;
        }
        const opts = {
            ...formatOptsFor(frame, client.contextVerbosity === "full", client.homeRoom, hintTracker),
        };
        if (bypassesBatch(frame) || client.inboundBatchMs <= 0) {
            batcher.flushNow(); // deliver any pending batch first (ordering)
            const injectOpts = {
                ...opts,
                replyChain: "__replyChain" in frame && frame.__replyChain === true,
                onInjected: () => recordInboundDelivery(frame, deps),
            };
            // force+interrupt: wire the honest report (receipt + local notify +
            // memory-only counters) before injection runs its repeater path
            if (frame.interrupt === true) {
                injectOpts.onInterruptReport = (report) => {
                    if (!active())
                        return;
                    rt.interruptCount = (rt.interruptCount ?? 0) + 1;
                    rt.lastInterruptAt = new Date().toISOString();
                    rt.lastInterruptOutcome = report.outcome;
                    guarded(() => {
                        (rt.ctx ?? ctx).ui.notify(`⚠ turn interrupted by force from @${frame.from ?? "?"}`, "warning");
                    });
                    if (rt.interruptReceipts !== false && frame.id !== undefined) {
                        const wording = report.outcome === "aborted"
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
            guarded(() => {
                injectInbound(deliveryHost, deliveryContext, frame, injectOpts);
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
                if (liveCooldowns.size > 128)
                    liveCooldowns.clear(); // bound
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
    rt.flushHeld = () => {
        if (!active())
            return;
        const frames = heldFrames.splice(0);
        for (const { frame, matchedReply } of frames)
            deliver(frame, matchedReply);
    };
    client.on("inbound", (frame, meta) => {
        if (!active())
            return;
        if (rt.rateLimitedUntil !== undefined && rt.rateLimitedUntil > Date.now()) {
            heldFrames.push({ frame, matchedReply: meta?.matchedReply === true }); // no injection while the provider rejects turns
            return;
        }
        deliver(frame, meta?.matchedReply === true);
    });
    rt.batcher = batcher;
    rt.clearInbound = () => {
        heldFrames.length = 0;
        batcher.clear();
        deferredInbox.clear();
        lastPeersOnline.clear();
        liveCooldowns.clear();
        fullContextSent = false;
    };
    client.on("broker_restart", () => {
        guarded(() => (rt.ctx ?? ctx).ui.notify("mesh broker restarted: peers will re-register; the previous broker's in-memory mailbox is not recovered", "warning"));
    });
    // presence → appendEntry ONLY, no turn
    client.on("presence", (frame) => {
        if (!active())
            return;
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
    client.on("ready", (welcome) => {
        if (!active())
            return;
        guarded(() => exposeAlias(pi, client.alias, client.rooms));
        guarded(() => updateSessionName(pi, rt));
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
            guarded(() => pi.sendMessage({
                customType: "mesh-context",
                content: buildReconnectDiff([...lastPeersOnline], othersNow),
                display: false,
            }, { triggerTurn: false }));
            lastPeersOnline = new Set(othersNow);
            return;
        }
        fullContextSent = true;
        lastPeersOnline = new Set(othersNow);
        // watchdog hook: a compaction wipes the context — re-teach everything
        // (full block + reply hints) exactly when it was lost.
        rt.onCompactionDetected = () => {
            if (!active())
                return;
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
        const viaTag = (p) => p.via !== undefined ? ` [${p.via} — other machine]` : "";
        const peerLine = online.length > 0
            ? `Online peers: ${online.map((p) => `@${p.alias}${viaTag(p)}`).join(", ")}.`
            : "Online peers: (none).";
        const farLine = far.length > 0
            ? ` Other sessions: ${far
                .slice(0, 12)
                .map((p) => `@${p.alias} (${p.rooms.join(",") || "?"})${viaTag(p)}`)
                .join(", ")}${far.length > 12 ? ` (+${far.length - 12} more)` : ""}.`
            : "";
        let context = `[mesh] you are @${client.alias} (rooms: ${roomList}). ` +
            `Your alias is stable across /reload; it changes only via /mesh alias, ` +
            `/mesh reset or /mesh new — if in doubt, mesh_status shows your alias. ` +
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
            pi.sendMessage({
                customType: "mesh-context",
                content: context,
                display: false,
            }, { triggerTurn: false });
        });
    });
    client.on("renamed", () => {
        if (!active())
            return;
        guarded(() => exposeAlias(pi, client.alias, client.rooms));
        guarded(() => updateSessionName(pi, rt));
        saveIdentity(rt); // disk-only
    });
    client.on("alias_fallback", ({ from, to }) => {
        if (!active())
            return;
        // Another live peer holds our persisted alias (e.g. crashed session) —
        // we took a random one; persist it so the next reload does not fight
        // for the same alias again.
        saveIdentity(rt); // disk-only
        guarded(() => {
            (rt.ctx ?? ctx).ui.notify(`mesh: alias @${from} taken — now connected as @${to}`, "warning");
        });
    });
    client.on("closed", () => { if (active())
        getHud()?.onClosed(); });
    client.on("pending", () => guarded(() => getHud()?.refresh()));
    // async drop receipt: the broker dropped one of OUR messages from the
    // recipient's offline mailbox (TTL expiry or cap eviction). The client
    // already settled any live awaitReply mission; the session must LEARN
    // the message will never arrive so a re-send decision is possible.
    client.on("dropped_offline", (frame) => {
        if (!active())
            return;
        const target = frame.to ?? "?";
        guarded(() => {
            pi.sendMessage({
                customType: "mesh-inbound",
                content: `[mesh] drop notice: message ${frame.id} to @${target} was dropped from the ` +
                    `offline mailbox (TTL expiry or cap). It will never be delivered — ` +
                    `re-send with mesh_send if still relevant.`,
                display: true,
                details: { kind: "mesh-drop-notice", msgId: frame.id, to: target },
            }, { triggerTurn: true, deliverAs: "followUp" });
        });
        try {
            rt.ledger.append({
                event: "dropped_offline",
                id: frame.id,
                to: target,
                room: frame.room,
            });
        }
        catch {
            // best effort — the inline notice already went out
        }
    });
    // D41: flip the guard — called from session_shutdown before client.close().
    // Frames still in flight on the old socket are then dropped silently
    // instead of reaching the stale pi/ctx.
    rt.markDetached = () => {
        detached = true;
        heldFrames.length = 0;
        deferredInbox.clear();
        // Drain only into the detached guard: cancels batching timers without injection.
        batcher.flushNow();
    };
}
