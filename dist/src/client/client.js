// client/client.ts — MeshClient: connect/send/reply/status/join/leave/close.
// Pi-independent: emits events; the extension adapter consumes them.
import { EventEmitter } from "node:events";
import { appendFileSync, mkdirSync, readFileSync } from "node:fs";
import net from "node:net";
import tls from "node:tls";
import { randomBytes } from "node:crypto";
import { buildFrame, isValidAlias, isValidRefPath, isValidReservationPattern, isValidRoom, normalizeAlias, parseFrameLine, } from "../protocol/envelope.js";
import { encodeFrame, FrameDecoder, FrameSizeError, sha256 } from "../protocol/frames.js";
import { ACK_TIMEOUT_MS, ALIAS_RAND_CHARS, DEFAULT_AWAIT_REPLY_TIMEOUT_MS, DEFAULT_CONFIG, DEFAULT_ROOM, HELLO_TIMEOUT_MS, MAX_AWAIT_REPLY_TIMEOUT_MS, MAX_BODY_BYTES, MAX_REFS, MAX_REPLY_TARGETS, MIN_AWAIT_REPLY_TIMEOUT_MS, OUTBOX_FLUSH_CAP, STATUS_REQ_TIMEOUT_MS, TRANSCRIPT_RING_SIZE, } from "../shared/config.js";
import { runtimeDir, stateDir } from "../shared/paths.js";
import { MESH_VERSION } from "../shared/version.js";
import { PendingReplies } from "./pending.js";
import { backoffMs, ensureBroker } from "./reconnect.js";
import { resolveConnection } from "../tailscale/adapter.js";
export function isBroadcastResult(res) {
    return res.status === "delivered" || res.status === "queued_offline";
}
export function formatDurationShort(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    if (hours > 0)
        return `${hours}h${minutes % 60}m`;
    if (minutes > 0)
        return `${minutes}m${seconds % 60}s`;
    return `${seconds}s`;
}
/**
 * activity status from the broker's lastSeenAt — idle after
 * activityIdleMs, STUCK when idle past activityStuckMs AND holding
 * reservations (a peer with claims that never progresses blocks others).
 */
export function computePeerStatus(lastSeenAt, hasReservations, idleMs, stuckMs, now = Date.now()) {
    if (lastSeenAt === undefined)
        return { status: "active" };
    const t = Date.parse(lastSeenAt);
    if (Number.isNaN(t))
        return { status: "active" };
    const elapsed = now - t;
    if (elapsed < idleMs)
        return { status: "active" };
    if (hasReservations && elapsed >= stuckMs)
        return { status: "stuck", idleFor: formatDurationShort(elapsed) };
    return { status: "idle", idleFor: formatDurationShort(elapsed) };
}
const BLOCKED_CODES = new Set([
    "policy_denied",
    "rate_limited",
    "peer_not_found",
    "not_member",
    "observer_readonly",
    "force_requires_reason",
]);
/** D39: offline watchdog kick interval (ms). */
const WATCHDOG_INTERVAL_MS = 60_000;
/** alias_taken: retries of the original alias before falling back to random. */
const ALIAS_RETRY_ATTEMPTS = 4;
const ALIAS_RETRY_DELAY_MS = 250;
/** a reply target stays "handled" for 30 min (duplicates dropped). */
const HANDLED_REPLY_WINDOW_MS = 1_800_000;
function sleepMs(ms) {
    return new Promise((resolve) => {
        const t = setTimeout(resolve, ms);
        t.unref();
    });
}
function defaultAlias() {
    return `agent-${randomBytes(ALIAS_RAND_CHARS / 2).toString("hex").slice(0, ALIAS_RAND_CHARS)}`;
}
export class MeshClient extends EventEmitter {
    /** Current alias — mutable via rename (in-flight alias change). */
    aliasInternal;
    initialRooms;
    /** All rooms this client is (or will be) a member of — re-declared at hello. */
    joinedRooms;
    runtimeDir;
    config;
    noReconnect;
    strictAlias;
    socket = null;
    online = false;
    intentionallyClosed = false;
    aliasFallbackDone = false;
    connecting = null;
    reconnectAttempt = 0;
    reconnectTimer = null;
    heartbeatTimer = null;
    /** D39 watchdog: guarantees recovery if the reconnect chain ever dies
    *  (observed once on a remote TCP client: socket gone, no retry). */
    watchdogTimer = null;
    helloTimer = null;
    #connectionTarget = null;
    #brokerInfo;
    #connectionError;
    #pinnedBrokerId;
    #refreshDiscovery = false;
    #connectionGeneration = 0;
    #connectAbort = null;
    #selectionGeneration = 0;
    #selection = null;
    #socketClosing = null;
    ackWaiters = new Map();
    statusWaiters = new Map();
    outbox = [];
    inbox = new Map(); // msgId → inbound msg (mesh_reply target)
    awaitTargets = new Map();
    pending;
    /** Each active verdict owns only the missions in its starting snapshot. */
    waitAllStops = new Map();
    foregroundSends = new Set();
    waitStopReason = "closed";
    /** Memory-only ring buffer of last frames (bodies included) for mesh_history. */
    transcript = [];
    /** Own file reservations — declared at hello, updated via reserve/release. */
    ownReservations = [];
    /** msgIds we sent that have been READ by peers (read receipts). */
    readBy = new Map();
    /** ids of replies WE sent — used to tag reply-à-reply chains. */
    sentReplies = new Set();
    /** missions sent with awaitReply — who answered (for mesh_wait_all).
    *  status: waiting | answered | expired | failed: a mission that was
    *  blocked/errored at ack or expired is NOT 'waiting' forever). Bounded
    * capped at 200 entries, oldest dropped first. */
    awaitedMissions = new Map();
    /** missions already reported by a wait_all verdict — never re-listed
    *  by a later wait_all (each batch is summarized once). */
    reportedMissions = new Set();
    /** inbox/receipt/mission history caps (Map insertion order = age). */
    static MISSION_CAP = 200;
    static MISSION_DROP = 50;
    static INBOX_CAP = 512;
    static INBOX_DROP = 128;
    static READBY_CAP = 200;
    static READBY_DROP = 50;
    /** Latest known reservations per peer, fed by welcome/reserve broadcasts. */
    peerReservations = new Map();
    /** Latest announced turn state per peer (activity frames). */
    peerActivity = new Map();
    /** Aliases seen online (welcome + presence join) — send-guard hints.
    *  Best-effort: a missing entry is a WARNING, never a block. */
    knownPeers = new Set();
    knownPeerRooms = new Map();
    /**
    * replyTo msgIds already answered/handled: the FIRST reply to a given
    * message is consumed (pending match) or injected (orphan); later replies
    * are deduped by (replyTo + body hash) — an EXACT re-send of an already
    * handled answer is dropped silently (agents re-answering on reminds), but
    * a DIFFERENT answer to the same msgId (e.g. an ack "reçue" then the final
    * report) is still delivered.
    */
    handledReplyTargets = new Map();
    constructor(opts = {}) {
        super();
        this.aliasInternal = normalizeAlias(opts.alias ?? defaultAlias());
        this.initialRooms = opts.rooms ?? [...DEFAULT_CONFIG.rooms];
        this.joinedRooms = new Map();
        for (const room of this.initialRooms) {
            this.joinedRooms.set(room, opts.roomRoles?.[room] ?? "member");
        }
        this.runtimeDir = opts.runtimeDir ?? runtimeDir();
        this.config = { ...DEFAULT_CONFIG, ...opts.config, rooms: this.initialRooms };
        this.noReconnect = opts.noReconnect === true;
        this.strictAlias = opts.strictAlias === true;
        if (opts.initialReservations !== undefined) {
            this.ownReservations = opts.initialReservations.map((r) => ({ ...r }));
        }
        if (opts.onFrame)
            this.on("frame", opts.onFrame);
        this.pending = new PendingReplies((msgId) => this.sendRemind(msgId), () => this.emit("pending"), (msgId, resolution) => {
            this.awaitTargets.delete(msgId);
            const mission = this.awaitedMissions.get(msgId);
            if (mission !== undefined) {
                if (resolution.kind === "reply" && resolution.frame !== undefined) {
                    mission.status = "answered";
                    mission.answered = true;
                    mission.response = resolution.frame.body ?? "";
                    mission.at = resolution.frame.ts;
                    delete mission.reason;
                    delete mission.unavailableReason;
                }
                else if (resolution.kind === "expired") {
                    mission.status = "expired";
                    this.emit("expired", { msgId });
                }
                else {
                    mission.status = "failed";
                    mission.reason = resolution.reason ?? "error";
                }
            }
            this.emit("missions");
        });
    }
    get alias() {
        return this.aliasInternal;
    }
    get connectionTarget() {
        return this.#connectionTarget;
    }
    get brokerInfo() {
        return this.#brokerInfo;
    }
    get connectionError() {
        return this.#connectionError;
    }
    /** All rooms this client is a member of (re-declared at every hello). */
    get rooms() {
        return [...this.joinedRooms.keys()];
    }
    get roomRoles() {
        return this.joinedRooms;
    }
    /** activity status thresholds from config. */
    get activityIdleMs() {
        return this.config.activityIdleMs;
    }
    /**
    * true when a reply targets ANOTHER reply (one we sent, or one we
    * received) — i.e. a reply-à-reply (ack-of-ack chain). Such replies are
    * injected with an info-only label in followUp mode: the LLM decides
    * whether the content is worth reacting to, instead of a silent drop.
    */
    isReplyToReply(replyTo) {
        return this.sentReplies.has(replyTo) || this.inbox.get(replyTo)?.type === "reply";
    }
    /** who read a msgId we sent ({alias, at}) — from read receipts. */
    readsOf(msgId) {
        this.pruneReadBy();
        const r = this.readBy.get(msgId);
        return r !== undefined && r.alias !== this.alias ? [{ ...r }] : [];
    }
    /** all read receipts we hold, newest first. */
    readReceipts(limit = 5) {
        this.pruneReadBy();
        return [...this.readBy.entries()]
            .reverse()
            .slice(0, limit)
            .map(([msgId, r]) => ({ msgId, alias: r.alias, at: r.at }));
    }
    // ---- awaited missions (mesh_wait_all) ----
    /** Cancel pending replies and clear every mission from the previous batch. */
    cancelAllAwaited() {
        this.stopWaitAll("cancelled", "missions_cancelled");
        for (const id of [...this.awaitTargets.keys()]) {
            this.pending.cancel(id, "missions_cancelled");
        }
        this.awaitTargets.clear();
        this.awaitedMissions.clear();
        this.reportedMissions.clear();
    }
    /** Yield passive waits without cancelling missions or aborting the host turn. */
    interruptWaits(reason, msgId) {
        let interrupted = this.waitAllStops.size > 0;
        for (const send of this.foregroundSends) {
            interrupted = send.stop("interrupted", reason, msgId) || interrupted;
        }
        this.stopWaitAll("interrupted", reason, msgId);
        return interrupted;
    }
    stopForegroundSends(reason, preserveSelection) {
        for (const send of this.foregroundSends) {
            if (send.selectionGeneration !== preserveSelection)
                send.stop("error", reason);
        }
    }
    stopWaitAll(status, reason, msgId) {
        for (const stop of this.waitAllStops.keys())
            stop(status, reason, msgId);
    }
    /** bound the awaitedMissions history (oldest dropped first). */
    pruneAwaitedMissions() {
        while (this.awaitedMissions.size > MeshClient.MISSION_CAP) {
            const oldest = this.awaitedMissions.keys().next().value;
            if (oldest === undefined)
                break;
            this.awaitedMissions.delete(oldest);
        }
    }
    /** bound the read-receipt store (oldest dropped first). */
    pruneReadBy() {
        while (this.readBy.size > MeshClient.READBY_CAP) {
            const oldest = this.readBy.keys().next().value;
            if (oldest === undefined)
                break;
            this.readBy.delete(oldest);
        }
    }
    /** bound the inbox (oldest dropped first) — reply targeting stays
    *  reliable for recent messages; ancient ones get reply_without_target. */
    pruneInbox() {
        while (this.inbox.size > MeshClient.INBOX_CAP) {
            const oldest = this.inbox.keys().next().value;
            if (oldest === undefined)
                break;
            this.inbox.delete(oldest);
        }
    }
    /** Missions sent with awaitReply and their answer state: status is
    *  honest — waiting/answered/expired/failed). */
    missionStatus() {
        this.pruneAwaitedMissions();
        return [...this.awaitedMissions.entries()].map(([msgId, m]) => ({
            msgId,
            to: m.to,
            answered: m.answered,
            status: m.status,
            ...(m.reason !== undefined || m.unavailableReason !== undefined
                ? { reason: m.reason ?? m.unavailableReason } : {}),
        }));
    }
    /**
    * block until EVERY awaited mission is answered (or timeout), then
    * return the honest group summary. The turn is suspended inside this tool
    * call — no sleep, no wasted tokens; inbound replies keep flowing and the
    * batch is delivered right after the result.
    * Snapshot every unreported mission, including answers and terminal failures
    * that arrived before this call. Missing missions remain reportable on retry.
    */
    async waitAll(timeoutMs = DEFAULT_AWAIT_REPLY_TIMEOUT_MS, signal) {
        const start = Date.now();
        const deadline = start + Math.min(MAX_AWAIT_REPLY_TIMEOUT_MS, Math.max(25, timeoutMs));
        const targets = new Set(this.awaitTargets.keys());
        for (const id of this.awaitedMissions.keys()) {
            if (!this.reportedMissions.has(id))
                targets.add(id);
        }
        const targetList = [...targets];
        if (this.#selection !== null || this.intentionallyClosed) {
            return this.summarize(start, "cancelled", targetList, this.waitStopReason);
        }
        const targetSet = new Set(targetList);
        let resolve;
        const promise = new Promise((settle) => { resolve = settle; });
        let settled = false;
        let timer = null;
        const done = (status, reason, interruptedBy) => {
            if (settled)
                return;
            settled = true;
            clearTimeout(timer ?? undefined);
            this.waitAllStops.delete(done);
            this.removeListener("missions", tick);
            signal?.removeEventListener("abort", onAbort);
            // Only normal verdicts consume answers. Interrupted/error/cancelled
            // waits may be retried with their partial answers intact.
            if (status === "complete" || status === "timeout") {
                for (const id of targetList) {
                    if (this.awaitedMissions.get(id)?.answered === true)
                        this.reportedMissions.add(id);
                }
                if (this.reportedMissions.size > 512) {
                    let dropped = 0;
                    for (const id of this.reportedMissions) {
                        if (dropped >= 256)
                            break;
                        this.reportedMissions.delete(id);
                        dropped += 1;
                    }
                }
            }
            resolve(this.summarize(start, status, targetList, reason, interruptedBy));
        };
        const onAbort = () => done("cancelled", "aborted");
        const tick = () => {
            if (settled)
                return;
            clearTimeout(timer ?? undefined);
            const missing = targetList.filter((id) => this.awaitedMissions.get(id)?.answered !== true);
            if (missing.length === 0)
                return done("complete");
            if (!this.online)
                return done("error", this.#connectionError ?? "connection_lost");
            let failure;
            for (const id of missing) {
                const mission = this.awaitedMissions.get(id);
                if (mission?.status === "failed" ||
                    (mission?.unavailableReason !== undefined && !this.ackWaiters.has(id))) {
                    failure = mission;
                    break;
                }
            }
            if (failure !== undefined)
                return done("error", failure.reason ?? failure.unavailableReason ?? "error");
            if (missing.every((id) => this.awaitedMissions.get(id)?.status === "expired") || Date.now() >= deadline) {
                return done("timeout");
            }
            timer = setTimeout(tick, Math.min(200, Math.max(1, deadline - Date.now())));
            timer.unref();
        };
        this.waitAllStops.set(done, targetSet);
        this.on("missions", tick);
        if (signal?.aborted)
            onAbort();
        else {
            signal?.addEventListener("abort", onAbort, { once: true });
            tick();
        }
        return promise;
    }
    summarize(startMs, status, targets, reason, interruptedBy) {
        const answers = [];
        const missing = [];
        for (const msgId of targets) {
            const m = this.awaitedMissions.get(msgId);
            if (m === undefined)
                continue;
            if (m.answered && m.response !== undefined) {
                answers.push({ msgId, to: m.to, response: m.response });
            }
            else {
                const missingReason = m.reason ?? m.unavailableReason;
                missing.push({ msgId, to: m.to, ...(missingReason !== undefined ? { reason: missingReason } : {}) });
            }
        }
        return {
            status,
            ...(reason !== undefined ? { reason } : {}),
            ...(interruptedBy !== undefined ? { interruptedBy } : {}),
            total: targets.length,
            answered: answers.length,
            elapsedMs: Date.now() - startMs,
            missing,
            answers,
        };
    }
    /** Announce this session's turn state to the mesh (busy on tool_call,
    *  idle on agent_settled, rate_limited on provider 429s). Fire-and-forget;
    *  the broker shares it with room members and status snapshots. */
    sendActivity(state) {
        if (!this.online)
            return;
        const frame = buildFrame({ type: "activity", from: this.alias, status: state });
        this.writeOrQueue(frame);
    }
    /** Last known turn state of a peer (from snapshots/activity frames). */
    activityOf(alias) {
        return this.peerActivity.get(alias);
    }
    /** How long a peer has been BUSY (ms), or undefined when not busy/unknown.
    *  Used to warn awaitReply senders: a busy-since-long peer with a short
    *  timeout will expire (measured: 6/6 expired missions in cs-room). */
    busyForMs(alias, now = Date.now()) {
        const a = this.peerActivity.get(alias);
        if (a === undefined || a.state !== "busy")
            return undefined;
        const t = Date.parse(a.at);
        if (Number.isNaN(t))
            return undefined;
        return Math.max(0, now - t);
    }
    /** Aliases known online (welcome/presence cache) — best-effort hints. */
    knowsPeer(alias) {
        return this.knownPeers.has(alias);
    }
    get knownPeerList() {
        return [...this.knownPeers];
    }
    /** inbound context verbosity ("compact" | "full"). */
    get contextVerbosity() {
        return this.config.contextVerbosity === "full" ? "full" : "compact";
    }
    /** the session's home room: the room tag is omitted for its frames. */
    get homeRoom() {
        return this.joinedRooms.keys().next().value ?? "default";
    }
    /** send a read receipt for an inbound msgId back to its sender. */
    sendRead(msgId, to) {
        if (!this.online)
            return;
        const frame = buildFrame({ type: "read", from: this.alias, to, reads: msgId });
        this.writeOrQueue(frame);
        this.ring(frame);
    }
    get activityStuckMs() {
        return this.config.activityStuckMs;
    }
    /** reservation TTL (0 = unlimited,. */
    get reservationTtlMs() {
        return this.config.reservationTtlMs;
    }
    get inboundBroadcasts() {
        return this.config.inboundBroadcasts ?? "immediate";
    }
    /** inbound batching window (0 = disabled). */
    get inboundBatchMs() {
        return this.config.inboundBatchMs ?? 0;
    }
    /** max hold while busy (safety cap). */
    get inboundBatchMaxHoldMs() {
        return this.config.inboundBatchMaxHoldMs ?? 30_000;
    }
    isOnline() {
        return this.online;
    }
    /** Additive (HUD): number of live awaitReply pendings. */
    get pendingCount() {
        return this.pending.size;
    }
    /** A live reply/transport promise belongs to this client until it settles. */
    isReloadSafe() {
        return this.pending.size === 0 && this.waitAllStops.size === 0 && this.foregroundSends.size === 0 &&
            this.ackWaiters.size === 0 && this.statusWaiters.size === 0 &&
            this.connecting === null && this.#selection === null && this.#socketClosing === null;
    }
    snapshotForReload() {
        return {
            inbox: [...this.inbox],
            transcript: [...this.transcript],
            readBy: [...this.readBy],
            sentReplies: [...this.sentReplies],
            awaitedMissions: [...this.awaitedMissions],
            reportedMissions: [...this.reportedMissions],
            handledReplyTargets: [...this.handledReplyTargets],
            outbox: [...this.outbox],
            roomRoles: [...this.joinedRooms],
            connectionConfig: {
                brokerUrl: this.config.brokerUrl,
                brokerToken: this.config.brokerToken,
                tlsCa: this.config.tlsCa,
                tlsInsecure: this.config.tlsInsecure,
                meshId: this.config.meshId,
                brokerId: this.config.brokerId,
                tailscale: this.config.tailscale === undefined ? undefined : { ...this.config.tailscale },
            },
            broker: this.#brokerInfo === undefined ? undefined : { ...this.#brokerInfo },
            pinnedBrokerId: this.#pinnedBrokerId,
        };
    }
    /** Restore before connecting so mailbox messages can still target old frames. */
    restoreAfterReload(state) {
        if (state.connectionConfig !== undefined) {
            this.config = {
                ...this.config,
                ...state.connectionConfig,
                // Retained reply targets must never cross into a different broker.
                brokerId: state.connectionConfig.brokerId ?? state.broker?.brokerId,
            };
        }
        this.#brokerInfo = state.broker;
        this.#pinnedBrokerId = state.pinnedBrokerId;
        this.joinedRooms.clear();
        for (const [room, role] of state.roomRoles) {
            if (role !== "member" && role !== "observer") {
                throw new Error("incompatible mesh client reload state; restart the host");
            }
            this.joinedRooms.set(room, role);
        }
        for (const [id, frame] of state.inbox)
            this.inbox.set(id, frame);
        this.transcript.push(...state.transcript);
        for (const [id, receipt] of state.readBy)
            this.readBy.set(id, receipt);
        for (const id of state.sentReplies)
            this.sentReplies.add(id);
        for (const [id, mission] of state.awaitedMissions)
            this.awaitedMissions.set(id, { ...mission });
        for (const id of state.reportedMissions)
            this.reportedMissions.add(id);
        for (const [id, at] of state.handledReplyTargets)
            this.handledReplyTargets.set(id, at);
        this.outbox.push(...state.outbox);
    }
    /**
    * Additive read-only peek at an inbound frame by msgId (ledger enrichment).
    * NEVER mutates the inbox — callers rely on it surviving for future replies.
    */
    peekInbox(msgId) {
        return this.inbox.get(msgId);
    }
    // ---- file reservations ----
    /** This client's reservations (live, mutable by reserve/release). */
    get reservations() {
        return this.ownReservations;
    }
    /** Known reservations of a peer alias (from welcome/status/reserve broadcasts). */
    reservationsOf(alias) {
        return this.peerReservations.get(alias) ?? [];
    }
    /** Snapshot of every peer alias currently holding reservations. */
    get peerReservationAliases() {
        return [...this.peerReservations.keys()];
    }
    /** Live peer→reservations map (read-only view, includes self). */
    get peerReservationMap() {
        return this.peerReservations;
    }
    setOwnReservations(next) {
        this.ownReservations = next;
        this.peerReservations.set(this.alias, next);
    }
    applyPeerReservations(alias, reservations) {
        if (reservations !== undefined && reservations.length > 0) {
            this.peerReservations.set(alias, [...reservations]);
        }
        else {
            this.peerReservations.delete(alias);
        }
    }
    ring(frame) {
        // keep the ring USEFUL — heartbeats/acks (ping/pong/ack) flood it
        // (4 pongs/min) and push real messages (msg/reply/reserve/…) out in
        // minutes, so mesh_history only showed pongs. Only frames the session
        // cares about are kept.
        if (frame.type === "ping" || frame.type === "pong" || frame.type === "ack" ||
            frame.type === "activity" // Phase 3: turn-state noise, not history
        )
            return;
        this.transcript.push(frame);
        if (this.transcript.length > TRANSCRIPT_RING_SIZE) {
            this.transcript.splice(0, this.transcript.length - TRANSCRIPT_RING_SIZE);
        }
    }
    /** replies to the same msgId are deduped for this long. */
    pruneHandledReplyTargets(now = Date.now()) {
        for (const [id, ts] of this.handledReplyTargets) {
            if (now - ts > HANDLED_REPLY_WINDOW_MS)
                this.handledReplyTargets.delete(id);
        }
    }
    /**
    * True when this EXACT answer (replyTo + body) was already consumed.
    * Marks the key on first sight so re-sends are dropped.
    */
    isDuplicateReply(replyTo, body, now = Date.now()) {
        const key = `${replyTo}|${sha256(body)}`;
        const ts = this.handledReplyTargets.get(key);
        if (ts !== undefined && now - ts < HANDLED_REPLY_WINDOW_MS)
            return true;
        this.handledReplyTargets.set(key, now);
        this.pruneHandledReplyTargets(now);
        return false;
    }
    /** Mark a reply target as consumed (after pending match or orphan inject). */
    markReplyHandled(replyTo, body, now = Date.now()) {
        this.handledReplyTargets.set(`${replyTo}|${sha256(body)}`, now);
        this.pruneHandledReplyTargets(now);
    }
    // ---- connection lifecycle ----
    connect() {
        if (this.#selection !== null)
            return this.#selection;
        return this.#connectSelected();
    }
    #connectSelected() {
        if (this.online && this.socket) {
            return Promise.resolve({
                alias: this.alias,
                rooms: [...this.joinedRooms.keys()],
                peers: [],
                mailboxCount: 0,
                broker: this.#brokerInfo,
            });
        }
        if (this.connecting !== null)
            return this.connecting;
        // An explicit connect re-arms after close; automatic retries never do.
        this.intentionallyClosed = false;
        clearTimeout(this.reconnectTimer ?? undefined);
        this.reconnectTimer = null;
        const generation = this.#connectionGeneration;
        const controller = new AbortController();
        this.#connectAbort = controller;
        const connection = this.doConnectWithAliasFallback(generation, controller.signal)
            .catch((error) => {
            if (generation === this.#connectionGeneration && !controller.signal.aborted) {
                this.#connectionError = error instanceof Error ? error.message : String(error);
                if (this.config.brokerUrl === undefined && this.config.tailscale?.mode !== undefined && this.config.tailscale.mode !== "off") {
                    this.#refreshDiscovery = true;
                }
                if (this.socket?.destroyed)
                    this.socket = null;
            }
            throw error;
        })
            .finally(() => {
            if (this.connecting !== connection)
                return;
            this.connecting = null;
            this.#connectAbort = null;
            if (!this.online && generation === this.#connectionGeneration && !controller.signal.aborted)
                this.#scheduleReconnect();
        });
        this.connecting = connection;
        return connection;
    }
    /** Select a new graph atomically for public connect/send callers.
     * A later selection or explicit close cancels this one; it never joins
     * itself through the public connect barrier. */
    selectTailnet(config) {
        const generation = ++this.#selectionGeneration;
        const selecting = Promise.resolve().then(async () => {
            if (generation !== this.#selectionGeneration)
                throw new Error("connection_cancelled");
            await this.#closeConnection("reset", generation);
            if (generation !== this.#selectionGeneration)
                throw new Error("connection_cancelled");
            this.config = { ...config, rooms: [...this.joinedRooms.keys()] };
            this.#connectionTarget = null;
            this.#brokerInfo = undefined;
            this.#pinnedBrokerId = undefined;
            this.#connectionError = undefined;
            this.#refreshDiscovery = true;
            this.reconnectAttempt = 0;
            this.outbox.length = 0;
            this.inbox.clear();
            this.cancelAllAwaited();
            this.awaitedMissions.clear();
            this.reportedMissions.clear();
            this.readBy.clear();
            this.sentReplies.clear();
            this.handledReplyTargets.clear();
            this.transcript.length = 0;
            this.#clearPeerCaches();
            return this.#connectSelected();
        }).finally(() => {
            if (this.#selection === selecting)
                this.#selection = null;
        });
        this.#selection = selecting;
        return selecting;
    }
    #clearPeerCaches() {
        this.peerReservations.clear();
        this.peerActivity.clear();
        this.knownPeers.clear();
        this.knownPeerRooms.clear();
    }
    #awaitConnection(operation, signal) {
        if (signal === undefined)
            return operation;
        return new Promise((resolve, reject) => {
            const abort = () => reject(new Error("connection_cancelled"));
            if (signal.aborted)
                abort();
            else
                signal.addEventListener("abort", abort, { once: true });
            operation.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
        });
    }
    /**
    * doConnect, but on alias_taken: first RETRY the original alias with a
    * short backoff — the old connection may be mid-close (the /reload
    * handover, where session_shutdown and session_start are back-to-back).
    * Only after the retries fail (a genuinely live peer holds the alias,
    * e.g. a crashed session that never disconnected) fall back to a fresh
    * random alias instead of looping forever. Emits `alias_fallback` so the
    * extension can notify the user and persist the new identity.
    */
    async doConnectWithAliasFallback(generation, signal) {
        for (let attempt = 0;; attempt += 1) {
            try {
                return await this.doConnect(generation, signal);
            }
            catch (err) {
                const msg = err instanceof Error ? err.message : String(err);
                if (signal.aborted || generation !== this.#connectionGeneration)
                    throw new Error("connection_cancelled");
                if (!msg.includes("alias_taken"))
                    throw err;
                if (attempt < ALIAS_RETRY_ATTEMPTS) {
                    await this.#awaitConnection(sleepMs(ALIAS_RETRY_DELAY_MS * 2 ** attempt), signal);
                    continue;
                }
                // strictAlias: keep the transient RETRIES (a mid-close race must not
                // fail the caller) but skip the random-alias FALLBACK — a one-shot
                // caller (CLI --alias) wants THIS alias or an honest failure, never
                // a surprise identity.
                if (this.strictAlias || this.aliasFallbackDone)
                    throw err;
                this.aliasFallbackDone = true;
                const previous = this.aliasInternal;
                this.aliasInternal = defaultAlias();
                try {
                    const welcome = await this.doConnect(generation, signal);
                    this.emit("alias_fallback", { from: previous, to: this.aliasInternal });
                    return welcome;
                }
                catch {
                    this.aliasFallbackDone = false;
                    throw err; // surface the original alias_taken
                }
            }
        }
    }
    async doConnect(generation, signal) {
        const target = await this.#awaitConnection(resolveConnection(this.config, { refresh: this.#refreshDiscovery }), signal);
        if (generation !== this.#connectionGeneration || signal?.aborted)
            throw new Error("connection_cancelled");
        this.#connectionTarget = target;
        this.#refreshDiscovery = false;
        const endpoint = target.endpoint;
        let socket;
        if (endpoint !== null && (endpoint.kind === "tcp" || endpoint.kind === "tls")) {
            const opts = {
                host: endpoint.host,
                port: endpoint.port,
                ...(endpoint.kind === "tls"
                    ? {
                        ca: this.config.tlsCa !== undefined ? readFileSync(this.config.tlsCa) : undefined,
                        rejectUnauthorized: this.config.tlsInsecure === true ? false : undefined,
                    }
                    : {}),
            };
            socket = endpoint.kind === "tls" ? tls.connect(opts) : net.createConnection(opts);
        }
        else if (endpoint !== null && endpoint.kind === "unix") {
            socket = net.createConnection(endpoint.path);
        }
        else {
            const sockPath = await this.#awaitConnection(ensureBroker(this.runtimeDir), signal);
            if (generation !== this.#connectionGeneration || signal?.aborted)
                throw new Error("connection_cancelled");
            socket = net.createConnection(sockPath);
        }
        this.socket = socket;
        this.debug(`doConnect: socket created → ${target.url ?? "local socket/pipe"}`);
        // persistent error listener — a write racing a broker-side destroy
        // (e.g. invalid_token: broker answers + closes) must never surface as an
        // unhandled 'error' event after the handshake promise already settled.
        // MESH_DEBUG logs the wire-level reason (no bodies).
        socket.on("error", (err) => {
            this.debug(`socket error: ${err.name}: ${err.message}`);
        });
        const decoder = new FrameDecoder(this.config.maxFrameBytes);
        // Start mixed-role reconnects read-only; restore member rooms before ready.
        let helloRole = "member";
        for (const role of this.joinedRooms.values()) {
            if (role === "observer") {
                helloRole = "observer";
                break;
            }
        }
        const welcome = await new Promise((resolve, reject) => {
            let settled = false;
            let accepted = false;
            const cancelled = () => generation !== this.#connectionGeneration || signal?.aborted === true;
            const fail = (reason) => {
                if (settled)
                    return;
                settled = true;
                clearTimeout(this.helloTimer ?? undefined);
                signal?.removeEventListener("abort", abort);
                if (this.socket === socket)
                    this.socket = null;
                socket.destroy();
                reject(new Error(reason));
            };
            const abort = () => fail("connection_cancelled");
            signal?.addEventListener("abort", abort, { once: true });
            this.helloTimer = setTimeout(() => fail("timeout: hello handshake"), HELLO_TIMEOUT_MS);
            this.helloTimer.unref();
            socket.once("connect", () => {
                if (cancelled()) {
                    fail("connection_cancelled");
                    return;
                }
                const hello = buildFrame({
                    type: "hello",
                    from: this.alias,
                    rooms: [...this.joinedRooms.keys()],
                    role: helloRole,
                    reservations: this.ownReservations.length > 0 ? [...this.ownReservations] : undefined,
                    token: this.config.brokerToken !== undefined ? sha256(this.config.brokerToken) : undefined,
                    clientVersion: MESH_VERSION, // M1: peers see each other's version
                    meshId: target.meshId,
                });
                socket.write(encodeFrame(hello, this.config.maxFrameBytes));
            });
            socket.once("error", () => fail("broker_unavailable"));
            socket.on("data", (chunk) => {
                if (cancelled()) {
                    fail("connection_cancelled");
                    return;
                }
                let lines;
                try {
                    lines = decoder.push(chunk);
                }
                catch {
                    fail("oversized");
                    return;
                }
                for (const line of lines) {
                    const parsed = parseFrameLine(line);
                    if (!parsed.ok)
                        continue;
                    const frame = parsed.frame;
                    if (!settled) {
                        if (frame.type === "welcome") {
                            const discovered = target.source === "tailscale-service" || target.source === "tailscale-node";
                            const broker = frame.broker;
                            if (broker === undefined && (discovered || target.meshId !== undefined || this.config.brokerId !== undefined)) {
                                fail("broker_metadata_missing: selected broker did not return mesh/broker identity");
                                return;
                            }
                            if (broker !== undefined) {
                                if (target.meshId !== undefined && broker.meshId !== target.meshId) {
                                    fail(`mesh_mismatch: expected ${target.meshId}, received ${broker.meshId}`);
                                    return;
                                }
                                const expected = this.config.brokerId ?? (discovered ? this.#pinnedBrokerId : undefined);
                                if (expected !== undefined && broker.brokerId !== expected) {
                                    fail(`broker_mismatch: expected ${expected}, received ${broker.brokerId}`);
                                    return;
                                }
                            }
                            const previous = this.#brokerInfo;
                            this.#brokerInfo = broker;
                            if (discovered && broker !== undefined)
                                this.#pinnedBrokerId = broker.brokerId;
                            this.#connectionError = undefined;
                            this.#clearPeerCaches();
                            settled = true;
                            accepted = true;
                            signal?.removeEventListener("abort", abort);
                            clearTimeout(this.helloTimer ?? undefined);
                            this.online = true;
                            this.reconnectAttempt = 0;
                            this.attachSocket(socket, decoder);
                            // seed the peer reservation cache from the welcome snapshot
                            for (const p of frame.peers ?? [])
                                this.applyPeerReservations(p.alias, p.reservations);
                            // Phase 3: seed the activity cache too
                            for (const p of frame.peers ?? []) {
                                if (p.activity !== undefined && p.alias !== undefined) {
                                    this.peerActivity.set(p.alias, p.activity);
                                }
                            }
                            // send-guard cache: aliases present in the welcome are known online
                            if (previous !== undefined && broker !== undefined && previous.bootId !== broker.bootId) {
                                for (const [id, mission] of this.awaitedMissions) {
                                    if (!mission.answered && mission.delivery !== "delivered"
                                        && mission.brokerBootId === previous.bootId && this.pending.has(id)) {
                                        this.pending.cancel(id, "broker_state_lost_delivery_unknown");
                                    }
                                }
                                this.emit("broker_restart", { previous, current: broker });
                            }
                            for (const p of frame.peers ?? []) {
                                if (p.alias !== undefined && p.alias !== this.alias) {
                                    this.knownPeers.add(p.alias);
                                    this.knownPeerRooms.set(p.alias, new Set(p.rooms));
                                    for (const mission of this.awaitedMissions.values()) {
                                        if (mission.to === p.alias && p.rooms.includes(mission.room)) {
                                            mission.targetOnline = true;
                                            delete mission.unavailableReason;
                                        }
                                    }
                                }
                            }
                            for (const mission of this.awaitedMissions.values()) {
                                if (!mission.answered && mission.targetOnline
                                    && this.knownPeerRooms.get(mission.to)?.has(mission.room) !== true) {
                                    mission.unavailableReason = "target_offline";
                                }
                            }
                            this.debug("handshake: welcome received");
                            resolve({
                                alias: this.alias,
                                rooms: frame.rooms ?? [...this.joinedRooms.keys()],
                                peers: frame.peers ?? [],
                                mailboxCount: frame.mailboxCount ?? 0,
                                broker,
                            });
                        }
                        else if (frame.type === "error") {
                            fail(frame.code ?? "internal");
                        }
                    }
                    else if (accepted && this.socket === socket && this.online) {
                        this.onFrame(frame);
                    }
                }
            });
            socket.on("close", () => {
                if (!settled)
                    fail("broker_unavailable");
            });
        });
        for (const [room, role] of this.joinedRooms) {
            if (role === helloRole)
                continue;
            if (socket.destroyed)
                throw new Error("connection_lost restoring room roles");
            const frame = buildFrame({ type: "join", from: this.alias, room, role });
            const ack = this.waitAck(frame.id);
            socket.write(encodeFrame(frame, this.config.maxFrameBytes));
            const result = await ack;
            if (result.type === "error") {
                socket.destroy();
                throw new Error(`cannot restore ${room} role: ${result.code ?? "internal"}`);
            }
        }
        if (generation !== this.#connectionGeneration || signal?.aborted || this.socket !== socket || !this.online) {
            socket.destroy();
            throw new Error("connection_cancelled");
        }
        this.startHeartbeat();
        this.startWatchdog();
        this.flushOutbox();
        this.emit("ready", welcome);
        return welcome;
    }
    /** Post-handshake wiring: frames dispatched, close → reconnect w/ backoff. */
    attachSocket(socket, _decoder) {
        socket.on("close", () => this.onSocketClosed(socket));
        socket.on("error", () => { });
    }
    onSocketClosed(closedSocket) {
        this.debug(`onSocketClosed: online=${this.online} stale=${closedSocket !== undefined && this.socket !== closedSocket}`);
        // Ignore stale closes from sockets we have already replaced (rename
        // retry loop, late close of a detached socket): only the CURRENT
        // socket's close may tear the connection down.
        if (closedSocket !== undefined && this.socket !== closedSocket)
            return;
        const wasOnline = this.online;
        this.online = false;
        this.socket = null;
        if (wasOnline) {
            this.#connectionError = "connection_lost";
            this.stopWaitAll("error", "connection_lost");
            this.stopForegroundSends("connection_lost");
        }
        this.stopHeartbeat();
        // in-flight sends: honest error, never delivered)
        for (const [id, w] of this.ackWaiters) {
            clearTimeout(w.timer);
            w.resolve(buildFrame({ type: "error", code: "internal", id, status: "connection_lost" }));
        }
        this.ackWaiters.clear();
        for (const [id, w] of this.statusWaiters) {
            clearTimeout(w.timer);
            w.resolve(buildFrame({ type: "error", code: "internal", id, status: "connection_lost" }));
        }
        this.statusWaiters.clear();
        if (this.intentionallyClosed || this.noReconnect) {
            if (wasOnline)
                this.emit("closed");
            return;
        }
        if (this.config.brokerUrl === undefined && this.config.tailscale?.mode !== undefined && this.config.tailscale.mode !== "off")
            this.#refreshDiscovery = true;
        this.#scheduleReconnect();
    }
    #scheduleReconnect() {
        if (this.intentionallyClosed || this.noReconnect || this.reconnectTimer !== null || this.online)
            return;
        const delay = backoffMs(this.reconnectAttempt);
        this.reconnectAttempt += 1;
        this.reconnectTimer = setTimeout(() => {
            this.reconnectTimer = null;
            if (this.intentionallyClosed)
                return;
            void this.connect().catch(() => { });
        }, delay);
        this.reconnectTimer.unref();
    }
    stopWatchdog() {
        if (this.watchdogTimer)
            clearInterval(this.watchdogTimer);
        this.watchdogTimer = null;
    }
    onFrame(frame) {
        this.ring(frame);
        switch (frame.type) {
            case "ack": {
                const mission = this.awaitedMissions.get(frame.id);
                if (mission !== undefined && frame.status === "delivered") {
                    mission.delivery = "delivered";
                    mission.targetOnline = true;
                }
                else if (mission !== undefined && frame.status === "queued_offline" && mission.delivery !== "delivered") {
                    mission.delivery = "queued_offline";
                    mission.targetOnline = false;
                    delete mission.unavailableReason;
                }
                const w = this.ackWaiters.get(frame.id);
                if (w) {
                    this.ackWaiters.delete(frame.id);
                    clearTimeout(w.timer);
                }
                if (mission !== undefined && (frame.status === "delivered" || frame.status === "queued_offline")) {
                    this.emit("missions");
                }
                if (w) {
                    w.resolve(frame);
                    break;
                }
                // async drop notice: the broker dropped a message we sent earlier
                // from the recipient's offline mailbox (TTL expiry or cap eviction).
                // No send is waiting for this ack — settle any live awaitReply mission
                // NOW (it can never be answered from a dropped message) and tell the
                // extension so the session learns the message will not arrive.
                if (frame.status === "dropped_offline") {
                    if (this.pending.has(frame.id))
                        this.pending.cancel(frame.id, "dropped_offline");
                    if (mission !== undefined && !mission.answered) {
                        mission.status = "failed";
                        mission.reason = "dropped_offline";
                        this.emit("missions");
                    }
                    this.emit("dropped_offline", frame);
                }
                break;
            }
            case "status_res": {
                const w = this.statusWaiters.get(frame.id);
                if (w) {
                    this.statusWaiters.delete(frame.id);
                    clearTimeout(w.timer);
                    w.resolve(frame);
                }
                break;
            }
            case "msg":
            case "mailbox":
                if (frame.id) {
                    this.inbox.set(frame.id, frame);
                    this.pruneInbox();
                }
                this.emit("inbound", frame);
                break;
            case "reserve": {
                // full-state replacement broadcast (empty array = released)
                const alias = frame.from;
                if (alias !== undefined && alias !== this.alias) {
                    this.applyPeerReservations(alias, frame.reservations);
                    this.emit("reservations", { from: alias, reservations: frame.reservations ?? [] });
                }
                break;
            }
            case "remind":
                this.emit("inbound", frame);
                break;
            case "reply": {
                const replyTo = frame.replyTo;
                const body = frame.body ?? "";
                // machine receipts (interrupt outcome) are META: they must never
                // settle a pending mission — the real answer still comes later.
                // They fall through to the orphan-inject path (followUp, no turn).
                if (frame.receipt === true) {
                    if (frame.id) {
                        this.inbox.set(frame.id, frame);
                        this.pruneInbox();
                    }
                    this.emit("inbound", frame, { matchedReply: false });
                    break;
                }
                // Ownership is per snapshot, captured before settlement can finish a verdict.
                let groupOwnsReply = false;
                if (replyTo !== undefined) {
                    for (const targets of this.waitAllStops.values()) {
                        if (targets.has(replyTo)) {
                            groupOwnsReply = true;
                            break;
                        }
                    }
                }
                const wake = replyTo !== undefined && this.pending.isLaunch(replyTo) && !groupOwnsReply;
                const matched = replyTo !== undefined ? this.pending.handleReply(frame) : false;
                if (matched) {
                    // A result must not remain trapped behind a different passive tool.
                    // The send/group that actually carries this answer keeps normal ownership.
                    let interruptedWaits = false;
                    for (const send of this.foregroundSends) {
                        if (send.msgId !== replyTo) {
                            interruptedWaits = send.stop("interrupted", "inbound_message", frame.id) || interruptedWaits;
                        }
                    }
                    for (const [stop, targets] of this.waitAllStops) {
                        if (replyTo !== undefined && !targets.has(replyTo)) {
                            stop("interrupted", "inbound_message", frame.id);
                            interruptedWaits = true;
                        }
                    }
                    if (replyTo !== undefined) {
                        this.markReplyHandled(replyTo, body);
                        const m = this.awaitedMissions.get(replyTo);
                        if (m !== undefined) {
                            m.status = "answered";
                            m.answered = true;
                            m.response = body;
                            m.at = frame.ts;
                        }
                    }
                    if (wake) {
                        // No active verdict owns this background answer: surface it once.
                        if (frame.id) {
                            this.inbox.set(frame.id, frame);
                            this.pruneInbox();
                        }
                        this.emit("inbound", frame, { matchedReply: true, interruptedWaits });
                    }
                    else {
                        this.emit("reply", frame);
                    }
                }
                else if (replyTo !== undefined && this.isDuplicateReply(replyTo, body)) {
                    // this EXACT answer (same msgId + same body) was already
                    // consumed (awaitReply) or injected (orphan) — a re-send on a
                    // remind. Drop it silently. Different answers to the same msgId
                    // (ack then final report) are NOT duplicates and still pass.
                    this.pending.unmatchedReplyCount += 1;
                }
                else {
                    // ORPHAN reply: the sender did not awaitReply, so no pending
                    // exists — but the answer must still reach the session. Surface it
                    // like an inbound frame (stored in the inbox so it can itself be
                    // replied to). Without this, mesh_reply returned "delivered" yet
                    // the sender never saw the response.
                    if (replyTo !== undefined)
                        this.markReplyHandled(replyTo, body);
                    const mission = replyTo !== undefined ? this.awaitedMissions.get(replyTo) : undefined;
                    if (mission !== undefined) {
                        mission.status = "answered";
                        mission.answered = true;
                        mission.response = body;
                        mission.at = frame.ts;
                        delete mission.reason;
                        delete mission.unavailableReason;
                        this.emit("missions");
                    }
                    if (frame.id) {
                        this.inbox.set(frame.id, frame);
                        this.pruneInbox();
                    }
                    this.emit("inbound", frame);
                }
                break;
            }
            case "read": {
                // read receipt for one of our msgIds — tracked, no turn.
                const msgId = frame.reads;
                if (msgId !== undefined && frame.from !== undefined) {
                    this.readBy.set(msgId, { alias: frame.from, at: frame.ts });
                }
                this.emit("read", frame);
                break;
            }
            case "activity": {
                // a peer announced busy/idle/rate_limited — cache it (status/HUD)
                if (frame.from !== undefined && (frame.status === "busy" || frame.status === "idle" || frame.status === "rate_limited" || frame.status === "blocked")) {
                    this.peerActivity.set(frame.from, { state: frame.status, at: frame.ts });
                }
                this.emit("frame", frame);
                break;
            }
            case "presence": {
                const alias = frame.from;
                const room = frame.room;
                if (alias !== undefined && room !== undefined) {
                    const rooms = this.knownPeerRooms.get(alias) ?? new Set();
                    if (frame.status === "online")
                        rooms.add(room);
                    else if (frame.status === "offline")
                        rooms.delete(room);
                    if (rooms.size > 0) {
                        this.knownPeerRooms.set(alias, rooms);
                        this.knownPeers.add(alias);
                    }
                    else {
                        this.knownPeerRooms.delete(alias);
                        this.knownPeers.delete(alias);
                        this.peerReservations.delete(alias);
                        this.peerActivity.delete(alias);
                    }
                    for (const mission of this.awaitedMissions.values()) {
                        if (mission.to !== alias || mission.room !== room || mission.answered)
                            continue;
                        if (frame.status === "online") {
                            mission.targetOnline = true;
                            delete mission.unavailableReason;
                        }
                        else if (frame.status === "offline" && mission.targetOnline) {
                            mission.unavailableReason = "target_offline";
                        }
                    }
                    this.emit("missions");
                }
                this.emit("presence", frame);
                break;
            }
            case "pong":
                break;
            case "error": {
                // broker refusals carry the original frame id → resolve the waiter
                const w = this.ackWaiters.get(frame.id);
                if (w) {
                    this.ackWaiters.delete(frame.id);
                    clearTimeout(w.timer);
                    w.resolve(frame);
                }
                const sw = this.statusWaiters.get(frame.id);
                if (sw) {
                    this.statusWaiters.delete(frame.id);
                    clearTimeout(sw.timer);
                    sw.resolve(frame);
                }
                this.emit("frame", frame);
                return; // skip double-emit below
            }
            default:
                break;
        }
        this.emit("frame", frame);
    }
    /** Wire-level lifecycle debug log (MESH_DEBUG=1). NEVER logs bodies. */
    debug(line) {
        if (this.config.debug !== true)
            return;
        try {
            mkdirSync(stateDir(), { recursive: true });
            appendFileSync(`${stateDir()}/client-debug.log`, `${new Date().toISOString()} [${this.alias}] ${line}\n`);
        }
        catch {
            // best effort — never break the connection path
        }
    }
    /** D39: kick a connect() every WATCHDOG_INTERVAL_MS while offline. Any
    *  state where online=false, no in-flight connect and no timer must heal
    *  itself; connect() is idempotent while a connect is already running. */
    startWatchdog() {
        if (this.watchdogTimer !== null)
            return;
        this.watchdogTimer = setInterval(() => {
            if (this.online ||
                this.connecting !== null ||
                this.reconnectTimer !== null ||
                this.intentionallyClosed ||
                this.noReconnect) {
                return; // healthy, already connecting, or a backoff is armed
            }
            this.debug(`watchdog: offline without in-flight connect — kicking connect() (attempts=${this.reconnectAttempt})`);
            this.connect().catch((err) => {
                this.debug(`watchdog: connect failed: ${err instanceof Error ? err.message : String(err)}`);
            });
        }, WATCHDOG_INTERVAL_MS);
        this.watchdogTimer.unref();
    }
    startHeartbeat() {
        this.stopHeartbeat();
        this.heartbeatTimer = setInterval(() => {
            if (this.online && this.socket && !this.socket.destroyed) {
                this.socket.write(encodeFrame(buildFrame({ type: "ping", from: this.alias }), this.config.maxFrameBytes));
            }
        }, this.config.heartbeatMs);
        this.heartbeatTimer.unref();
    }
    stopHeartbeat() {
        if (this.heartbeatTimer)
            clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
    }
    flushOutbox() {
        const n = Math.min(this.outbox.length, OUTBOX_FLUSH_CAP);
        for (let i = 0; i < n; i += 1) {
            const frame = this.outbox.shift();
            if (frame && this.socket && !this.socket.destroyed) {
                this.socket.write(encodeFrame(frame, this.config.maxFrameBytes));
            }
        }
    }
    writeOrQueue(frame, encoded) {
        if (this.online && this.socket && !this.socket.destroyed) {
            this.socket.write(encoded ?? encodeFrame(frame, this.config.maxFrameBytes));
        }
        else if (this.outbox.length < OUTBOX_FLUSH_CAP) {
            this.outbox.push(frame);
        }
    }
    // ---- send / reply ----
    async send(opts) {
        if (opts.signal?.aborted)
            return { status: "error", reason: "aborted" };
        const broadcast = opts.broadcast === true;
        const to = opts.to !== undefined ? normalizeAlias(opts.to) : undefined;
        if (broadcast) {
            // broadcast needs a room and must NOT carry a target
            if (to !== undefined)
                return { status: "error", reason: "broadcast_with_to" };
        }
        else {
            if (to === undefined || !isValidAlias(to))
                return { status: "error", reason: "invalid_alias" };
            if (to === this.alias)
                return { status: "blocked", reason: "self_send" };
        }
        // Room resolution: explicit room wins; otherwise prefer "default" when
        // still joined, else the first joined room (the client may have left
        // "default" — sending into it would be refused as not_member).
        const room = opts.room ?? (this.joinedRooms.has(DEFAULT_ROOM)
            ? DEFAULT_ROOM
            : this.joinedRooms.keys().next().value);
        if (room === undefined || !isValidRoom(room)) {
            return { status: "error", reason: "not_in_any_room" };
        }
        if (Buffer.byteLength(opts.message, "utf8") > MAX_BODY_BYTES) {
            return { status: "error", reason: "invalid_frame: body too large" };
        }
        if (opts.refs !== undefined) {
            if (opts.refs.length > MAX_REFS || !opts.refs.every(isValidRefPath)) {
                return { status: "error", reason: "invalid_frame: bad refs" };
            }
        }
        // replyTo: normalize to a bounded alias list (single or several).
        let replyTargets;
        if (opts.replyTo !== undefined) {
            const raw = Array.isArray(opts.replyTo) ? opts.replyTo : [opts.replyTo];
            if (raw.length === 0 || raw.length > MAX_REPLY_TARGETS) {
                return { status: "error", reason: "invalid_frame: bad replyTo" };
            }
            replyTargets = raw.map(normalizeAlias);
            if (!replyTargets.every(isValidAlias)) {
                return { status: "error", reason: "invalid_alias" };
            }
        }
        const priority = opts.priority ?? "normal";
        if (priority === "force" && (opts.reason === undefined || opts.reason.trim() === "")) {
            return { status: "error", reason: "force_requires_reason" };
        }
        // interrupt is a force modifier: the friction (force + reason + explicit
        // flag) IS the safety — a standalone interrupt would bypass the policy.
        if (opts.interrupt === true && (priority !== "force" || opts.reason === undefined || opts.reason.trim() === "")) {
            return { status: "error", reason: "interrupt_requires_force_reason" };
        }
        // block:false is ONLY meaningful with awaitReply — a fire-and-forget
        // send has nothing for wait_all to track.
        const launch = opts.awaitReply === true && opts.block === false;
        if (opts.block === false && opts.awaitReply !== true) {
            return { status: "error", reason: "block_requires_awaitReply" };
        }
        const timeoutMs = Math.min(MAX_AWAIT_REPLY_TIMEOUT_MS, Math.max(MIN_AWAIT_REPLY_TIMEOUT_MS, opts.timeoutMs ?? DEFAULT_AWAIT_REPLY_TIMEOUT_MS));
        let frame;
        let written = false;
        let detached = false;
        let confirmedDelivery;
        let resolveForeground;
        const foregroundResult = new Promise((resolve) => { resolveForeground = resolve; });
        const deliveryKnowledge = () => written ? (frame === undefined ? undefined : this.awaitedMissions.get(frame.id)?.delivery)
            ?? confirmedDelivery ?? "unknown" : "not_sent";
        const detach = () => {
            detached = true;
            this.foregroundSends.delete(foreground);
            this.removeListener("missions", onMissions);
            opts.signal?.removeEventListener("abort", onAbort);
        };
        const foreground = {
            selectionGeneration: this.#selectionGeneration,
            stop: (status, reason, interruptedBy) => {
                if (detached || (!launch && frame !== undefined && this.awaitedMissions.get(frame.id)?.answered))
                    return false;
                const delivery = deliveryKnowledge();
                detach();
                if (frame !== undefined) {
                    if (written) {
                        if (status === "interrupted" || reason === "connection_lost" || reason === "target_offline") {
                            this.pending.promoteToLaunch(frame.id);
                        }
                        else {
                            this.pending.cancel(frame.id, reason);
                        }
                    }
                    else {
                        this.pending.cancel(frame.id, reason);
                        this.awaitedMissions.delete(frame.id);
                    }
                }
                const tracked = frame !== undefined && this.pending.has(frame.id);
                resolveForeground(status === "interrupted"
                    ? { status, ...(written && frame !== undefined ? { msgId: frame.id } : {}),
                        reason, ...(interruptedBy !== undefined ? { interruptedBy } : {}), delivery, tracked }
                    : { status, ...(written && frame !== undefined ? { msgId: frame.id } : {}),
                        reason, ...(delivery !== "not_sent" ? { delivery } : {}), tracked });
                return true;
            },
        };
        const onMissions = () => {
            if (frame === undefined || detached || launch || this.ackWaiters.has(frame.id))
                return;
            const mission = this.awaitedMissions.get(frame.id);
            if (mission?.status === "waiting" && mission.unavailableReason !== undefined) {
                foreground.stop("error", mission.unavailableReason);
            }
        };
        const onAbort = () => {
            if (detached)
                return;
            const delivery = deliveryKnowledge();
            detach();
            if (frame !== undefined) {
                const waiter = this.ackWaiters.get(frame.id);
                if (waiter !== undefined) {
                    clearTimeout(waiter.timer);
                    this.ackWaiters.delete(frame.id);
                    waiter.resolve(buildFrame({ type: "error", id: frame.id, code: "internal", status: "aborted" }));
                }
                this.pending.cancel(frame.id, "aborted");
                this.awaitedMissions.delete(frame.id);
            }
            resolveForeground({ status: "error", ...(frame !== undefined ? { msgId: frame.id } : {}),
                reason: "aborted", ...(delivery !== "not_sent" ? { delivery } : {}) });
        };
        this.foregroundSends.add(foreground);
        this.on("missions", onMissions);
        opts.signal?.addEventListener("abort", onAbort, { once: true });
        const run = async () => {
            const selectionGeneration = this.#selectionGeneration;
            if (!this.online || this.#selection !== null) {
                try {
                    await this.connect();
                }
                catch (error) {
                    return { status: "blocked", reason: error instanceof Error ? error.message : String(error) };
                }
            }
            // A yielded connection attempt may finish, but must never send later.
            if (detached)
                return { status: "error", reason: "not_sent" };
            if (opts.signal?.aborted) {
                onAbort();
                return { status: "error", reason: "aborted" };
            }
            if (selectionGeneration !== this.#selectionGeneration || !this.online) {
                return { status: "blocked", reason: "connection_cancelled" };
            }
            frame = buildFrame({
                type: "msg", from: this.alias, to, room, priority, body: opts.message, refs: opts.refs,
                broadcast: broadcast ? true : undefined, replyTargets,
                reasonHash: priority === "force" ? sha256(opts.reason ?? "") : undefined,
                interrupt: opts.interrupt === true ? true : undefined,
                expiresAt: opts.awaitReply ? new Date(Date.now() + timeoutMs).toISOString() : undefined,
            });
            foreground.msgId = frame.id;
            let encoded;
            try {
                encoded = encodeFrame(frame, this.config.maxFrameBytes);
            }
            catch (error) {
                if (error instanceof FrameSizeError)
                    return { status: "error", reason: "oversized" };
                throw error;
            }
            // Register before write; yielding during registration cancels the unsent mission.
            let pendingPromise = null;
            if (opts.awaitReply) {
                this.awaitTargets.set(frame.id, { to: to ?? "*", room });
                this.awaitedMissions.set(frame.id, {
                    to: to ?? "*", room, status: "waiting", answered: false,
                    targetOnline: to !== undefined && this.knownPeerRooms.get(to)?.has(room) === true,
                    brokerBootId: this.#brokerInfo?.bootId,
                });
                this.pruneAwaitedMissions();
                pendingPromise = this.pending.register(frame.id, Date.now() + timeoutMs, launch);
                if (!launch) {
                    const msgId = frame.id;
                    // A real answer already proves completion; a delayed ACK must not
                    // hold its owning tool hostage. ACK bookkeeping continues below.
                    void pendingPromise.then((resolution) => {
                        if (detached || resolution.kind !== "reply" || resolution.frame === undefined)
                            return;
                        detach();
                        resolveForeground({ status: "reply", msgId, response: resolution.frame.body ?? "",
                            outputHash: resolution.frame.bodyHash ?? "" });
                    });
                }
            }
            if (detached)
                return { status: "error", reason: "not_sent" };
            const ackPromise = this.waitAck(frame.id);
            if (this.online && this.socket !== null && !this.socket.destroyed) {
                this.socket.write(encoded);
                written = true;
                this.ring(frame);
            }
            else {
                const waiter = this.ackWaiters.get(frame.id);
                if (waiter !== undefined) {
                    clearTimeout(waiter.timer);
                    this.ackWaiters.delete(frame.id);
                    waiter.resolve(buildFrame({ type: "error", id: frame.id, code: "internal", status: "connection_lost" }));
                }
            }
            // Keep this transaction alive after yielding: its ACK must still update
            // delivery or settle a genuine refusal, without owning the old tool signal.
            const ack = await ackPromise;
            if (ack.type === "error") {
                const reason = ack.status ?? ack.code ?? "internal";
                const retain = written && reason === "connection_lost";
                if (retain)
                    this.pending.promoteToLaunch(frame.id);
                else
                    this.pending.cancel(frame.id, reason);
                const delivery = deliveryKnowledge();
                if (BLOCKED_CODES.has(reason))
                    return { status: "blocked", msgId: frame.id, reason };
                return { status: "error", msgId: frame.id, reason,
                    ...(delivery !== "not_sent" ? { delivery } : {}), ...(retain ? { tracked: this.pending.has(frame.id) } : {}) };
            }
            const mission = this.awaitedMissions.get(frame.id);
            const delivery = mission?.delivery ?? confirmedDelivery ?? ack.status;
            if (delivery !== "delivered" && delivery !== "queued_offline") {
                const reason = `unexpected_ack_status: ${ack.status ?? "missing"}`;
                this.pending.cancel(frame.id, reason);
                return { status: "error", msgId: frame.id, reason, delivery: "unknown" };
            }
            confirmedDelivery = delivery;
            if (!opts.awaitReply || pendingPromise === null || launch || detached) {
                return { status: delivery, msgId: frame.id, deliveredCount: ack.deliveredCount, totalCount: ack.totalCount };
            }
            onMissions();
            if (detached)
                return { status: delivery, msgId: frame.id };
            const resolution = await pendingPromise;
            if (resolution.kind === "reply" && resolution.frame !== undefined) {
                return { status: "reply", msgId: frame.id, response: resolution.frame.body ?? "",
                    outputHash: resolution.frame.bodyHash ?? "" };
            }
            if (resolution.kind === "expired")
                return { status: "expired", msgId: frame.id, reason: "expired" };
            return { status: "error", msgId: frame.id, reason: resolution.reason ?? "error", delivery };
        };
        try {
            return await Promise.race([foregroundResult, run()]);
        }
        finally {
            detach();
        }
    }
    async reply(msgId, body, opts = {}) {
        const original = this.inbox.get(msgId);
        const { refs, to, replyAll, room: explicitRoom } = opts;
        // One-shot callers (CLI) never saw the original frame: with an explicit
        // room plus either a target or replyAll, the reply is still routable;
        // anything less stays reply_without_target — never a guess.
        const oneShot = explicitRoom !== undefined && original === undefined && (to !== undefined || replyAll === true);
        if ((!original || original.from === undefined) && !oneShot) {
            return { status: "error", msgId, reason: "reply_without_target" };
        }
        const originalRoom = original?.room ?? explicitRoom;
        if (replyAll === true && to !== undefined) {
            return { status: "error", msgId, reason: "reply_all_with_to" };
        }
        if (Buffer.byteLength(body, "utf8") > MAX_BODY_BYTES) {
            return { status: "error", msgId, reason: "invalid_frame: body too large" };
        }
        if (refs !== undefined && (refs.length > MAX_REFS || !refs.every(isValidRefPath))) {
            return { status: "error", msgId, reason: "invalid_frame: bad refs" };
        }
        if (!this.online) {
            try {
                await this.connect();
            }
            catch {
                return { status: "blocked", msgId, reason: "broker_unavailable" };
            }
        }
        // reply variants
        //  - default: to the original sender (1:1)
        //  - to=<alias>: targeted at another member of the conversation
        //  - replyAll: fan out to the whole room of the original message
        //  - the original msg may carry replyTargets (sender-designated): the
        //    default reply then fans out to ALL of them instead of the sender.
        const designated = original?.replyTargets;
        const replyTargets = replyAll !== true && to === undefined && designated !== undefined && designated.length > 0
            ? designated
            : undefined;
        if (replyTargets !== undefined && replyTargets.length > MAX_REPLY_TARGETS) {
            return { status: "error", msgId, reason: "invalid_frame: bad replyTargets" };
        }
        const target = replyAll === true || replyTargets !== undefined
            ? undefined
            : to !== undefined ? normalizeAlias(to) : original?.from;
        if (replyAll !== true && replyTargets === undefined && (target === undefined || !isValidAlias(target))) {
            return { status: "error", msgId, reason: "invalid_alias" };
        }
        const frame = buildFrame({
            type: "reply",
            from: this.alias,
            to: target,
            room: originalRoom,
            replyTo: msgId,
            replyTargets: replyTargets !== undefined ? [...replyTargets] : undefined,
            body,
            refs,
            replyAll: replyAll === true ? true : undefined,
            receipt: opts.receipt === true ? true : undefined,
        });
        let encoded;
        try {
            encoded = encodeFrame(frame, this.config.maxFrameBytes);
        }
        catch (error) {
            if (error instanceof FrameSizeError)
                return { status: "error", msgId, reason: "oversized" };
            throw error;
        }
        const ackPromise = this.waitAck(frame.id);
        this.writeOrQueue(frame, encoded);
        this.ring(frame);
        const ack = await ackPromise;
        if (ack.type === "error") {
            return { status: "error", msgId: frame.id, reason: ack.code ?? "internal" };
        }
        if (ack.status === "dropped_offline") {
            return { status: "error", msgId: frame.id, reason: "dropped_offline" };
        }
        // remember this reply id so ack-of-ack targeting it can be dropped.
        // cap the set — drop the OLDEST half (insertion order), not all.
        this.sentReplies.add(frame.id);
        if (this.sentReplies.size > 512) {
            let dropped = 0;
            for (const id of this.sentReplies) {
                if (dropped >= 256)
                    break;
                this.sentReplies.delete(id);
                dropped += 1;
            }
        }
        return {
            status: "delivered",
            msgId: frame.id,
            deliveredCount: ack.deliveredCount,
            totalCount: ack.totalCount,
        };
    }
    /** client-side remind, broker stays mute. Max 2 enforced by PendingReplies.
     *  Skips a rate-limited target: poking a peer whose provider rejects every
     *  turn (429) only burns turns — the mission stays pending and wait_all
     *  reports the real reason. */
    sendRemind(msgId) {
        const target = this.awaitTargets.get(msgId);
        if (!target)
            return;
        // skip BOTH failure states: transient (rate_limited) and permanent
        // (blocked — auth errors etc. won't heal by poking)
        const act = this.peerActivity.get(target.to)?.state;
        if (act === "rate_limited" || act === "blocked")
            return;
        const frame = buildFrame({
            type: "remind",
            id: msgId,
            from: this.alias,
            to: target.to,
            room: target.room,
            replyTo: msgId,
            body: `reminder: reply expected for ${msgId}`,
        });
        this.writeOrQueue(frame);
        this.ring(frame);
    }
    /**
    * (re)declare this client's file reservations (add or replace).
    * The broker broadcasts the new full state to every peer. Patterns that are
    * invalid or empty are rejected before the network round-trip.
    */
    async reserve(patterns, reason) {
        const valid = [];
        for (const raw of patterns) {
            const pattern = raw.trim();
            if (pattern.length === 0 || !isValidReservationPattern(pattern)) {
                return { status: "error", reason: `invalid_pattern: ${raw.slice(0, 80)}` };
            }
            valid.push(pattern);
        }
        if (valid.length === 0)
            return { status: "error", reason: "invalid_pattern" };
        if (!this.online) {
            try {
                await this.connect();
            }
            catch {
                return { status: "blocked", reason: "broker_unavailable" };
            }
        }
        const merged = [...this.ownReservations];
        for (const pattern of valid) {
            const idx = merged.findIndex((r) => r.pattern === pattern);
            const entry = {
                pattern,
                reason: reason !== undefined && reason.trim() !== "" ? reason.trim().slice(0, 512) : undefined,
                since: new Date().toISOString(),
            };
            if (idx !== -1)
                merged[idx] = entry;
            else
                merged.push(entry);
        }
        const frame = buildFrame({ type: "reserve", from: this.alias, reservations: merged });
        const ack = await this.roundTrip(frame);
        if (ack.type === "error")
            return { status: "error", reason: ack.status ?? ack.code ?? "internal" };
        this.setOwnReservations(merged);
        this.emit("reservations", { from: this.alias, reservations: merged });
        return { status: "delivered", msgId: frame.id };
    }
    /**
    * release reservations. `patterns` undefined → release ALL.
    * Returns the released patterns.
    */
    async release(patterns) {
        const releasing = patterns === undefined
            ? this.ownReservations.map((r) => r.pattern)
            : patterns.map((p) => p.trim()).filter((p) => p.length > 0);
        if (releasing.length === 0)
            return { status: "delivered", msgId: "", released: [] };
        const merged = this.ownReservations.filter((r) => !releasing.includes(r.pattern));
        if (!this.online) {
            try {
                await this.connect();
            }
            catch {
                return { status: "blocked", reason: "broker_unavailable", released: [] };
            }
        }
        const frame = buildFrame({ type: "reserve", from: this.alias, reservations: merged });
        const ack = await this.roundTrip(frame);
        if (ack.type === "error")
            return { status: "error", reason: ack.status ?? ack.code ?? "internal", released: [] };
        this.setOwnReservations(merged);
        this.emit("reservations", { from: this.alias, reservations: merged });
        return { status: "delivered", msgId: frame.id, released: releasing };
    }
    waitAck(id) {
        return new Promise((resolve) => {
            const timer = setTimeout(() => {
                this.ackWaiters.delete(id);
                resolve(buildFrame({ type: "error", code: "timeout", id }));
            }, ACK_TIMEOUT_MS);
            timer.unref();
            this.ackWaiters.set(id, { resolve, timer });
        });
    }
    // ---- status / rooms / close ----
    status(room) {
        if (!this.online) {
            return Promise.reject(new Error(this.intentionallyClosed ? this.waitStopReason : this.#connectionError ?? "offline"));
        }
        return new Promise((resolve, reject) => {
            const frame = buildFrame({ type: "status_req", from: this.alias, room });
            const timer = setTimeout(() => {
                this.statusWaiters.delete(frame.id);
                reject(new Error("timeout"));
            }, STATUS_REQ_TIMEOUT_MS);
            timer.unref();
            this.statusWaiters.set(frame.id, {
                timer,
                resolve: (res) => {
                    if (res.type !== "status_res") {
                        reject(new Error(res.status ?? res.code ?? "internal"));
                        return;
                    }
                    resolve({ peers: res.peers ?? [], rooms: res.rooms ?? [], stats: res.stats, broker: res.broker });
                },
            });
            this.writeOrQueue(frame);
        });
    }
    async join(room, role = "member") {
        const frame = buildFrame({ type: "join", from: this.alias, room, role });
        const ack = await this.roundTrip(frame);
        if (ack.type === "error")
            throw new Error(ack.status ?? ack.code ?? "internal");
        this.joinedRooms.set(room, role);
    }
    async leave(room) {
        const frame = buildFrame({ type: "leave", from: this.alias, room });
        const ack = await this.roundTrip(frame);
        if (ack.type === "error") {
            if (ack.code === "not_member") {
                // Resync: the broker does not know us in this room — make sure a
                // future reconnect does not try to rejoin it.
                this.joinedRooms.delete(room);
            }
            throw new Error(ack.status ?? ack.code ?? "internal");
        }
        this.joinedRooms.delete(room);
    }
    /**
    * In-flight alias change: detach from the broker under the old alias, then
    * re-hello under the new one. Rooms and reservations are re-declared in the
    * hello (broker state for the old alias — rooms, reservations, mailbox — is
    * dropped with the connection). On failure (e.g. alias_taken) the previous
    * alias is restored and the session reconnects under it.
    * `unchanged: true` when the alias was already the requested one (no-op).
    */
    async rename(newAlias) {
        const target = normalizeAlias(newAlias);
        if (!isValidAlias(target))
            return { ok: false, reason: "invalid_alias" };
        if (target === this.aliasInternal)
            return { ok: true, alias: target, unchanged: true };
        if (!this.online) {
            try {
                await this.connect();
            }
            catch {
                return { ok: false, reason: "broker_unavailable" };
            }
        }
        const generation = this.#connectionGeneration;
        const controller = new AbortController();
        this.#connectAbort = controller;
        const oldAlias = this.aliasInternal;
        const oldSocket = this.socket;
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
        // 1. detach the old alias (no reconnect loop — intentionallyClosed)
        this.intentionallyClosed = true;
        this.stopHeartbeat();
        this.waitStopReason = "renamed";
        this.stopWaitAll("cancelled", "renamed");
        this.stopForegroundSends("renamed");
        this.pending.cancelAll("renamed");
        for (const [id, waiter] of this.ackWaiters) {
            clearTimeout(waiter.timer);
            waiter.resolve(buildFrame({ type: "error", code: "internal", status: "renamed", id }));
        }
        this.ackWaiters.clear();
        for (const [id, waiter] of this.statusWaiters) {
            clearTimeout(waiter.timer);
            waiter.resolve(buildFrame({ type: "error", code: "internal", status: "renamed", id }));
        }
        this.statusWaiters.clear();
        this.socket = null;
        this.online = false;
        if (oldSocket && !oldSocket.destroyed) {
            await new Promise((resolve) => {
                const t = setTimeout(resolve, ACK_TIMEOUT_MS);
                t.unref();
                oldSocket.once("close", () => {
                    clearTimeout(t);
                    resolve();
                });
                oldSocket.end();
            });
        }
        if (generation !== this.#connectionGeneration || controller.signal.aborted)
            return { ok: false, reason: "connection_cancelled" };
        // 2. re-hello under the new alias. NO alias_fallback here: rename
        //  handles alias_taken itself (restore the previous identity below).
        //  A transient alias_taken is RETRIED with backoff: right after
        //  /mesh reset (or a crashed session), the old socket's close may not
        //  have reached the broker yet — a single attempt would fail the
        //  rename even though the alias is about to be free. intentionallyClosed
        //  stays true during the retries so onSocketClosed never schedules a
        //  competing reconnect.
        this.aliasInternal = target;
        let lastErr;
        for (let attempt = 0;; attempt += 1) {
            try {
                await this.doConnect(generation, controller.signal);
                lastErr = undefined;
                break;
            }
            catch (err) {
                lastErr = err;
                const msg = err instanceof Error ? err.message : String(err);
                if (generation !== this.#connectionGeneration || controller.signal.aborted || !msg.includes("alias_taken") || attempt >= ALIAS_RETRY_ATTEMPTS)
                    break;
                try {
                    await this.#awaitConnection(sleepMs(ALIAS_RETRY_DELAY_MS * 2 ** attempt), controller.signal);
                }
                catch (error) {
                    lastErr = error;
                    break;
                }
            }
        }
        if (this.#connectAbort === controller)
            this.#connectAbort = null;
        if (generation !== this.#connectionGeneration || controller.signal.aborted)
            return { ok: false, reason: "connection_cancelled" };
        if (lastErr !== undefined) {
            // restore the previous identity so the session stays usable
            this.aliasInternal = oldAlias;
            this.intentionallyClosed = true;
            try {
                await this.connect();
            }
            catch {
                // broker down entirely — nothing more we can do
            }
            const detail = lastErr instanceof Error ? lastErr.message : String(lastErr);
            return { ok: false, reason: detail.includes("alias_taken") ? "alias_taken" : detail };
        }
        this.intentionallyClosed = false;
        this.emit("renamed", { from: oldAlias, to: target });
        return { ok: true, alias: target };
    }
    async roundTrip(frame) {
        if (!this.online) {
            try {
                await this.connect();
            }
            catch (error) {
                return buildFrame({ type: "error", code: "internal", status: error instanceof Error ? error.message : String(error), id: frame.id });
            }
        }
        const ackPromise = this.waitAck(frame.id);
        this.writeOrQueue(frame);
        return ackPromise;
    }
    close() {
        this.#selectionGeneration += 1;
        return this.#closeConnection();
    }
    async #closeConnection(reason = "closed", preserveSelection) {
        this.intentionallyClosed = true;
        this.waitStopReason = reason;
        this.#connectionGeneration += 1;
        this.#connectAbort?.abort();
        this.#connectAbort = null;
        this.connecting = null;
        clearTimeout(this.helloTimer ?? undefined);
        this.helloTimer = null;
        clearTimeout(this.reconnectTimer ?? undefined);
        this.reconnectTimer = null;
        this.stopHeartbeat();
        this.stopWatchdog();
        // Settle snapshots before reset clears their mission maps.
        this.stopWaitAll("cancelled", reason);
        this.stopForegroundSends(reason, preserveSelection);
        this.pending.cancelAll(reason);
        const socket = this.socket;
        this.socket = null;
        this.online = false;
        for (const [id, waiter] of this.ackWaiters) {
            clearTimeout(waiter.timer);
            waiter.resolve(buildFrame({ type: "error", code: "internal", status: reason, id }));
        }
        this.ackWaiters.clear();
        for (const [id, waiter] of this.statusWaiters) {
            clearTimeout(waiter.timer);
            waiter.resolve(buildFrame({ type: "error", code: "internal", status: reason, id }));
        }
        this.statusWaiters.clear();
        if (socket && !socket.destroyed) {
            const closing = new Promise((resolve) => {
                const timer = setTimeout(() => {
                    socket.destroy();
                    resolve();
                }, ACK_TIMEOUT_MS);
                timer.unref();
                socket.once("close", () => {
                    clearTimeout(timer);
                    resolve();
                });
                socket.end();
            });
            this.#socketClosing = closing;
            try {
                await closing;
            }
            finally {
                if (this.#socketClosing === closing)
                    this.#socketClosing = null;
            }
        }
        else if (this.#socketClosing !== null) {
            await this.#socketClosing;
        }
        this.emit("closed");
    }
}
