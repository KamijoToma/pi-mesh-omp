export type MeshPriority = "normal" | "urgent" | "force";
export type MeshRole = "member" | "observer";
export interface FileReservation {
    pattern: string;
    reason?: string;
    since?: string;
}
export interface MeshPeerInfo {
    alias: string;
    rooms: string[];
    role?: MeshRole;
    since?: string;
    /** last broker-seen activity (heartbeat/tool frames) — status calc. */
    lastSeenAt?: string;
    reservations?: FileReservation[];
    /** M1: extension version (from the hello frame). */
    clientVersion?: string;
    /** Announced turn state (busy/idle/rate_limited). */
    activity?: {
        state: "busy" | "idle" | "rate_limited" | "blocked";
        at: string;
    };
    /** D40: how this peer reaches the broker — "tcp:<ip>"/"tls:<ip>" for
    *  remote machines, undefined for broker-local (unix socket) peers. */
    via?: string;
}
export interface MeshFrame {
    v: 1;
    type: string;
    id: string;
    from?: string;
    to?: string;
    room?: string;
    replyTo?: string;
    /** msg: aliases the sender designates to receive the reply (default: the
    *  sender). reply: fan-out targets chosen by the replier. */
    replyTargets?: string[];
    priority?: MeshPriority;
    body?: string;
    bodyHash?: string;
    refs?: string[];
    reasonHash?: string;
    expiresAt?: string;
    code?: string;
    status?: string;
    peers?: MeshPeerInfo[];
    rooms?: string[];
    role?: MeshRole;
    mailboxCount?: number;
    queuedAt?: string;
    interruptStatus?: string;
    reservations?: FileReservation[];
    broadcast?: boolean;
    replyAll?: boolean;
    deliveredCount?: number;
    totalCount?: number;
    /** msgId the sender is acknowledging as READ (read receipt). */
    reads?: string;
    /** shared auth token hash (hello only) — required on tcp/tls brokers. */
    token?: string;
    /** msg: sender demands the recipient's running tool be interrupted
    *  (abort the stuck turn) so the message lands with priority. Requires
    *  priority=force + reason at SEND time (reason travels hashed only). */
    interrupt?: boolean;
    /** reply: machine receipt (e.g. interrupt outcome) — rendered INFO ONLY,
    *  followUp without triggerTurn: it must never wake or cascade. */
    receipt?: boolean;
    /** M1: extension version of the sender (hello) — shown in status snapshots
    *  so stale sessions are visible at a glance. */
    clientVersion?: string;
    /** M2: broker counters (status_res). */
    stats?: {
        relayed: number;
        refused: number;
        mailboxDelivered: number;
        mailboxDropped: number;
    };
    ts: string;
}
export declare const FRAME_TYPES: readonly ["hello", "welcome", "msg", "ack", "reply", "remind", "presence", "mailbox", "status_req", "status_res", "join", "leave", "reserve", "read", "ping", "pong", "activity", "error"];
export type MeshFrameType = (typeof FRAME_TYPES)[number];
export declare const ERROR_CODES: readonly ["invalid_frame", "oversized", "hello_required", "invalid_alias", "alias_taken", "invalid_room", "invalid_token", "not_member", "observer_readonly", "last_room", "peer_not_found", "rate_limited", "policy_denied", "force_requires_reason", "hash_mismatch", "reply_without_target", "timeout", "expired", "shutting_down", "internal"];
export type MeshErrorCode = (typeof ERROR_CODES)[number];
export declare const PRIORITIES: readonly ["normal", "urgent", "force"];
export declare const ROLES: readonly ["member", "observer"];
export declare const ACK_STATUSES: readonly ["delivered", "queued_offline", "dropped_offline", "ok"];
export declare const FORBIDDEN_PERSISTED_KEYS: readonly ["body", "task", "prompt", "output", "content", "message", "rationale", "text", "diff", "patch"];
/** Recursive scan: true if any object key is forbidden (fail-closed before ledger append). */
export declare function hasForbiddenPersistedKey(value: unknown): boolean;
/** trim + strip leading '@' + lowercase. No -/_ equivalence. */
export declare function normalizeAlias(raw: string): string;
export declare function isValidAlias(alias: string): boolean;
export declare function isValidRoom(room: string): boolean;
export declare const MAX_RESERVATION_PATTERN_CHARS = 512;
export declare const MAX_RESERVATION_REASON_CHARS = 512;
export declare function isValidReservationPattern(pattern: string): boolean;
/** Shape-check a reservations array; returns false on any malformed entry. */
export declare function isValidReservations(value: unknown): value is FileReservation[];
/** Repo-relative refs only: reject "..", leading "/", "\", ".env". */
export declare function isValidRefPath(ref: string): boolean;
export interface BuildFrameOpts {
    type: MeshFrameType;
    id?: string;
    from?: string;
    to?: string;
    room?: string;
    replyTo?: string;
    replyTargets?: string[];
    priority?: MeshPriority;
    body?: string;
    refs?: string[];
    reasonHash?: string;
    expiresAt?: string;
    code?: MeshErrorCode;
    status?: string;
    peers?: MeshPeerInfo[];
    rooms?: string[];
    role?: MeshRole;
    mailboxCount?: number;
    queuedAt?: string;
    interruptStatus?: string;
    reservations?: FileReservation[];
    broadcast?: boolean;
    replyAll?: boolean;
    deliveredCount?: number;
    totalCount?: number;
    reads?: string;
    token?: string;
    interrupt?: boolean;
    receipt?: boolean;
    /** M1: extension version (hello). */
    clientVersion?: string;
    /** M2: broker counters (status_res). */
    stats?: {
        relayed: number;
        refused: number;
        mailboxDelivered: number;
        mailboxDropped: number;
    };
}
/** Build a protocol-valid frame; bodyHash auto-computed when body present. */
export declare function buildFrame(opts: BuildFrameOpts): MeshFrame;
export type ValidationResult = {
    ok: true;
    frame: MeshFrame;
} | {
    ok: false;
    code: MeshErrorCode;
    detail: string;
};
export interface ValidateOpts {
    maxBodyBytes?: number;
}
/** Validate an arbitrary value against rules. Unknown keys tolerated. */
export declare function validateFrame(value: unknown, opts?: ValidateOpts): ValidationResult;
/** Parse one NDJSON line into a validated frame. */
export declare function parseFrameLine(line: string, opts?: ValidateOpts): ValidationResult;
