import { EventEmitter } from "node:events";
import { type FileReservation, type MeshFrame, type MeshPeerInfo, type MeshPriority, type MeshRole } from "../protocol/envelope.js";
import { type MeshConfig } from "../shared/config.js";
export interface WelcomeInfo {
    alias: string;
    rooms: string[];
    peers: MeshPeerInfo[];
    mailboxCount: number;
}
export interface StatusSnapshot {
    peers: MeshPeerInfo[];
    rooms: string[];
    /** M2: broker counters (status_res) — relayed/refused/mailbox. */
    stats?: {
        relayed: number;
        refused: number;
        mailboxDelivered: number;
        mailboxDropped: number;
    };
}
export interface SendOpts {
    to?: string;
    message: string;
    room?: string;
    priority?: MeshPriority;
    reason?: string;
    awaitReply?: boolean;
    /** with awaitReply — return the delivery result IMMEDIATELY and keep
    *  the mission tracked in the background (reminds, expiry, answered);
    *  mesh_wait_all reports the group verdict later. The general
    *  orchestrator pattern: launch a burst, then wait_all once. */
    block?: boolean;
    timeoutMs?: number;
    refs?: string[];
    /** fan out to every room member (room required, to must be absent). */
    broadcast?: boolean;
    /** aliases that should receive the reply instead of the sender
    *  (default: the sender). Single alias or list — the recipient's
    *  mesh_reply without an explicit `to` goes to ALL of them. Include
    *  yourself if you also want the answer (e.g. with awaitReply). */
    replyTo?: string | string[];
    /** Abort a BLOCKED recipient turn (kills its running tool via the host
    *  abort) so this message lands with priority. Requires priority=force
    *  and a reason — a last-resort unblocking lever, rate-limited and
    *  policy-gated exactly like force. */
    interrupt?: boolean;
    /** Abort a BLOCKING awaitReply send (ESC): cancels the pending, cleans
     *  the mission, and returns {status:"error", reason:"cancelled"} — a
     *  late reply still arrives via the orphan-inject path. Ignored for
     *  LAUNCH sends (they never block). */
    signal?: AbortSignal;
}
export interface ReplyOpts {
    refs?: string[];
    /** target a different member than the original sender. */
    to?: string;
    /** fan the answer out to the whole room of the original message. */
    replyAll?: boolean;
    /** machine receipt (interrupt outcome): rendered INFO ONLY, followUp
    *  without triggerTurn — it must never wake the sender or cascade. */
    receipt?: boolean;
    /** Explicit room for one-shot callers whose inbox never saw the original
    *  frame (CLI): with `to` + `room` a reply is routable without inbox
    *  knowledge — anything less stays reply_without_target (honest). */
    room?: string;
}
export type SendResult = {
    status: "delivered";
    msgId: string;
    deliveredCount?: number;
    totalCount?: number;
} | {
    status: "queued_offline";
    msgId: string;
    deliveredCount?: number;
    totalCount?: number;
} | {
    status: "reply";
    msgId: string;
    response: string;
    outputHash: string;
} | {
    status: "expired" | "blocked" | "error";
    msgId?: string;
    reason: string;
};
export declare function isBroadcastResult(res: SendResult): res is Extract<SendResult, {
    deliveredCount?: number;
}>;
export interface PeerActivityStatus {
    status: "active" | "idle" | "stuck";
    idleFor?: string;
}
export declare function formatDurationShort(ms: number): string;
/**
 * activity status from the broker's lastSeenAt — idle after
 * activityIdleMs, STUCK when idle past activityStuckMs AND holding
 * reservations (a peer with claims that never progresses blocks others).
 */
export declare function computePeerStatus(lastSeenAt: string | undefined, hasReservations: boolean, idleMs: number, stuckMs: number, now?: number): PeerActivityStatus;
export interface MeshClientOpts {
    alias?: string;
    rooms?: string[];
    /** Reservations to declare at hello (persisted identity reload). */
    initialReservations?: FileReservation[];
    runtimeDir?: string;
    config?: Partial<MeshConfig>;
    onFrame?: (f: MeshFrame) => void;
    /** Disable auto-reconnect (ephemeral CLI clients). */
    noReconnect?: boolean;
    /** With an explicit alias: a collision is a hard error (no retry, no
    *  random-alias fallback). For one-shot callers (CLI --alias) a surprise
    *  identity would be dishonest — they want THAT alias or a failure. */
    strictAlias?: boolean;
}
interface AwaitedMission {
    to: string;
    room: string;
    status: "waiting" | "answered" | "expired" | "failed";
    answered: boolean;
    response?: string;
    at?: string;
}
/** Process-local handover only: message bodies must never be written to disk. */
export interface MeshClientReloadState {
    inbox: [string, MeshFrame][];
    transcript: MeshFrame[];
    readBy: [string, {
        alias: string;
        at: string;
    }][];
    sentReplies: string[];
    awaitedMissions: [string, AwaitedMission][];
    reportedMissions: string[];
    handledReplyTargets: [string, number][];
    outbox: MeshFrame[];
    roomRoles: [string, MeshRole][];
}
export interface WaitAllSummary {
    status: "complete" | "timeout" | "cancelled";
    total: number;
    answered: number;
    elapsedMs: number;
    missing: {
        msgId: string;
        to: string;
    }[];
    answers: {
        msgId: string;
        to: string;
        response: string;
    }[];
}
export declare class MeshClient extends EventEmitter {
    /** Current alias — mutable via rename (in-flight alias change). */
    private aliasInternal;
    private readonly initialRooms;
    /** All rooms this client is (or will be) a member of — re-declared at hello. */
    private readonly joinedRooms;
    private readonly runtimeDir;
    private readonly config;
    private readonly noReconnect;
    private readonly strictAlias;
    private socket;
    private online;
    private intentionallyClosed;
    private aliasFallbackDone;
    private connecting;
    private reconnectAttempt;
    private reconnectTimer;
    private heartbeatTimer;
    /** D39 watchdog: guarantees recovery if the reconnect chain ever dies
    *  (observed once on a remote TCP client: socket gone, no retry). */
    private watchdogTimer;
    private helloTimer;
    private readonly ackWaiters;
    private readonly statusWaiters;
    private readonly outbox;
    private readonly inbox;
    private readonly awaitTargets;
    private readonly pending;
    /** waitAll() calls currently in flight (a counter: two concurrent
     *  waits must not clear each other's suppression) — while > 0, matched
     *  LAUNCH answers skip session injection (the verdict carries that
     *  batch). */
    private waitAllInFlight;
    /** Memory-only ring buffer of last frames (bodies included) for mesh_history. */
    readonly transcript: MeshFrame[];
    /** Own file reservations — declared at hello, updated via reserve/release. */
    private ownReservations;
    /** msgIds we sent that have been READ by peers (read receipts). */
    private readonly readBy;
    /** ids of replies WE sent — used to tag reply-à-reply chains. */
    private readonly sentReplies;
    /** missions sent with awaitReply — who answered (for mesh_wait_all).
    *  status: waiting | answered | expired | failed: a mission that was
    *  blocked/errored at ack or expired is NOT 'waiting' forever). Bounded
    * capped at 200 entries, oldest dropped first. */
    private readonly awaitedMissions;
    /** missions already reported by a wait_all verdict — never re-listed
    *  by a later wait_all (each batch is summarized once). */
    private readonly reportedMissions;
    /** inbox/receipt/mission history caps (Map insertion order = age). */
    private static readonly MISSION_CAP;
    private static readonly MISSION_DROP;
    private static readonly INBOX_CAP;
    private static readonly INBOX_DROP;
    private static readonly READBY_CAP;
    private static readonly READBY_DROP;
    /** Latest known reservations per peer, fed by welcome/reserve broadcasts. */
    private readonly peerReservations;
    /** Latest announced turn state per peer (activity frames). */
    private readonly peerActivity;
    /** Aliases seen online (welcome + presence join) — send-guard hints.
    *  Best-effort: a missing entry is a WARNING, never a block. */
    private readonly knownPeers;
    /**
    * replyTo msgIds already answered/handled: the FIRST reply to a given
    * message is consumed (pending match) or injected (orphan); later replies
    * are deduped by (replyTo + body hash) — an EXACT re-send of an already
    * handled answer is dropped silently (agents re-answering on reminds), but
    * a DIFFERENT answer to the same msgId (e.g. an ack "reçue" then the final
    * report) is still delivered.
    */
    private readonly handledReplyTargets;
    constructor(opts?: MeshClientOpts);
    get alias(): string;
    /** All rooms this client is a member of (re-declared at every hello). */
    get rooms(): readonly string[];
    /** activity status thresholds from config. */
    get activityIdleMs(): number;
    /**
    * true when a reply targets ANOTHER reply (one we sent, or one we
    * received) — i.e. a reply-à-reply (ack-of-ack chain). Such replies are
    * injected with an info-only label in followUp mode: the LLM decides
    * whether the content is worth reacting to, instead of a silent drop.
    */
    isReplyToReply(replyTo: string): boolean;
    /** who read a msgId we sent ({alias, at}) — from read receipts. */
    readsOf(msgId: string): {
        alias: string;
        at: string;
    }[];
    /** all read receipts we hold, newest first. */
    readReceipts(limit?: number): {
        msgId: string;
        alias: string;
        at: string;
    }[];
    /** cancel every pending awaited mission (e.g. before a reset).
    *  awaitedMissions is wiped here, so recently-answered missions of past
    *  batches cannot leak into the next wait_all verdict. */
    cancelAllAwaited(): void;
    /** bound the awaitedMissions history (oldest dropped first). */
    private pruneAwaitedMissions;
    /** bound the read-receipt store (oldest dropped first). */
    private pruneReadBy;
    /** bound the inbox (oldest dropped first) — reply targeting stays
    *  reliable for recent messages; ancient ones get reply_without_target. */
    private pruneInbox;
    /** Missions sent with awaitReply and their answer state: status is
    *  honest — waiting/answered/expired/failed). */
    missionStatus(): {
        msgId: string;
        to: string;
        answered: boolean;
        status: string;
    }[];
    /**
    * block until EVERY awaited mission is answered (or timeout), then
    * return the honest group summary. The turn is suspended inside this tool
    * call — no sleep, no wasted tokens; inbound replies keep flowing and the
    * batch is delivered right after the result.
    * snapshot: still-pending missions (awaitTargets) PLUS missions
    * answered recently (≤ 5 min) that no previous verdict reported yet — a
    * fast answer that resolved before this call must still be in the summary.
    */
    waitAll(timeoutMs: number, signal?: AbortSignal): Promise<WaitAllSummary>;
    private summarize;
    /** Announce this session's turn state to the mesh (busy on tool_call,
    *  idle on agent_settled, rate_limited on provider 429s). Fire-and-forget;
    *  the broker shares it with room members and status snapshots. */
    sendActivity(state: "busy" | "idle" | "rate_limited" | "blocked"): void;
    /** Last known turn state of a peer (from snapshots/activity frames). */
    activityOf(alias: string): {
        state: "busy" | "idle" | "rate_limited" | "blocked";
        at: string;
    } | undefined;
    /** How long a peer has been BUSY (ms), or undefined when not busy/unknown.
    *  Used to warn awaitReply senders: a busy-since-long peer with a short
    *  timeout will expire (measured: 6/6 expired missions in cs-room). */
    busyForMs(alias: string, now?: number): number | undefined;
    /** Aliases known online (welcome/presence cache) — best-effort hints. */
    knowsPeer(alias: string): boolean;
    get knownPeerList(): readonly string[];
    /** inbound context verbosity ("compact" | "full"). */
    get contextVerbosity(): "compact" | "full";
    /** the session's home room: the room tag is omitted for its frames. */
    get homeRoom(): string;
    /** send a read receipt for an inbound msgId back to its sender. */
    sendRead(msgId: string, to: string): void;
    get activityStuckMs(): number;
    /** reservation TTL (0 = unlimited,. */
    get reservationTtlMs(): number;
    get inboundBroadcasts(): "immediate" | "deferred";
    /** inbound batching window (0 = disabled). */
    get inboundBatchMs(): number;
    /** max hold while busy (safety cap). */
    get inboundBatchMaxHoldMs(): number;
    isOnline(): boolean;
    /** Additive (HUD): number of live awaitReply pendings. */
    get pendingCount(): number;
    /** A live reply/transport promise belongs to this client until it settles. */
    isReloadSafe(): boolean;
    snapshotForReload(): MeshClientReloadState;
    /** Restore before connecting so mailbox messages can still target old frames. */
    restoreAfterReload(state: MeshClientReloadState): void;
    /**
    * Additive read-only peek at an inbound frame by msgId (ledger enrichment).
    * NEVER mutates the inbox — callers rely on it surviving for future replies.
    */
    peekInbox(msgId: string): MeshFrame | undefined;
    /** This client's reservations (live, mutable by reserve/release). */
    get reservations(): readonly FileReservation[];
    /** Known reservations of a peer alias (from welcome/status/reserve broadcasts). */
    reservationsOf(alias: string): readonly FileReservation[];
    /** Snapshot of every peer alias currently holding reservations. */
    get peerReservationAliases(): readonly string[];
    /** Live peer→reservations map (read-only view, includes self). */
    get peerReservationMap(): ReadonlyMap<string, readonly FileReservation[]>;
    private setOwnReservations;
    private applyPeerReservations;
    private ring;
    /** replies to the same msgId are deduped for this long. */
    private pruneHandledReplyTargets;
    /**
    * True when this EXACT answer (replyTo + body) was already consumed.
    * Marks the key on first sight so re-sends are dropped.
    */
    private isDuplicateReply;
    /** Mark a reply target as consumed (after pending match or orphan inject). */
    private markReplyHandled;
    connect(): Promise<WelcomeInfo>;
    /**
    * doConnect, but on alias_taken: first RETRY the original alias with a
    * short backoff — the old connection may be mid-close (the /reload
    * handover, where session_shutdown and session_start are back-to-back).
    * Only after the retries fail (a genuinely live peer holds the alias,
    * e.g. a crashed session that never disconnected) fall back to a fresh
    * random alias instead of looping forever. Emits `alias_fallback` so the
    * extension can notify the user and persist the new identity.
    */
    private doConnectWithAliasFallback;
    private doConnect;
    /** Post-handshake wiring: frames dispatched, close → reconnect w/ backoff. */
    private attachSocket;
    private onSocketClosed;
    private stopWatchdog;
    private onFrame;
    /** Wire-level lifecycle debug log (MESH_DEBUG=1). NEVER logs bodies. */
    private debug;
    /** D39: kick a connect() every WATCHDOG_INTERVAL_MS while offline. Any
    *  state where online=false, no in-flight connect and no timer must heal
    *  itself; connect() is idempotent while a connect is already running. */
    private startWatchdog;
    private startHeartbeat;
    private stopHeartbeat;
    private flushOutbox;
    private writeOrQueue;
    send(opts: SendOpts): Promise<SendResult>;
    reply(msgId: string, body: string, opts?: ReplyOpts): Promise<SendResult>;
    /** client-side remind, broker stays mute. Max 2 enforced by PendingReplies.
     *  Skips a rate-limited target: poking a peer whose provider rejects every
     *  turn (429) only burns turns — the mission stays pending and wait_all
     *  reports the real reason. */
    private sendRemind;
    /**
    * (re)declare this client's file reservations (add or replace).
    * The broker broadcasts the new full state to every peer. Patterns that are
    * invalid or empty are rejected before the network round-trip.
    */
    reserve(patterns: string[], reason?: string): Promise<SendResult>;
    /**
    * release reservations. `patterns` undefined → release ALL.
    * Returns the released patterns.
    */
    release(patterns?: string[]): Promise<{
        released: string[];
    } & SendResult>;
    private waitAck;
    status(room?: string): Promise<StatusSnapshot>;
    join(room: string, role?: MeshRole): Promise<void>;
    leave(room: string): Promise<void>;
    /**
    * In-flight alias change: detach from the broker under the old alias, then
    * re-hello under the new one. Rooms and reservations are re-declared in the
    * hello (broker state for the old alias — rooms, reservations, mailbox — is
    * dropped with the connection). On failure (e.g. alias_taken) the previous
    * alias is restored and the session reconnects under it.
    * `unchanged: true` when the alias was already the requested one (no-op).
    */
    rename(newAlias: string): Promise<{
        ok: true;
        alias: string;
        unchanged?: boolean;
    } | {
        ok: false;
        reason: string;
    }>;
    private roundTrip;
    close(): Promise<void>;
}
export {};
