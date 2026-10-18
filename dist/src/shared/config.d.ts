export declare const DEFAULT_MAX_FRAME_BYTES = 65536;
export declare const HARD_MIN_FRAME_BYTES = 1024;
export declare const HARD_MAX_FRAME_BYTES = 1048576;
export declare const MAX_BODY_BYTES = 32768;
export declare const MAX_FRAME_ID_CHARS = 64;
export declare const MAX_REFS = 8;
export declare const MAX_REF_CHARS = 256;
/** max aliases a sender can designate to receive the reply. */
export declare const MAX_REPLY_TARGETS = 8;
export declare const HELLO_TIMEOUT_MS = 5000;
export declare const WRITE_TIMEOUT_MS = 5000;
export declare const DEFAULT_HEARTBEAT_MS = 15000;
export declare const DEFAULT_BROKER_SILENCE_MS = 45000;
export declare const SWEEP_INTERVAL_MS = 15000;
export declare const STATUS_REQ_TIMEOUT_MS = 5000;
export declare const ACK_TIMEOUT_MS = 5000;
export declare const DEFAULT_MAILBOX_CAP = 100;
export declare const DEFAULT_MAILBOX_TTL_MS = 3600000;
export declare const MAILBOX_PURGE_INTERVAL_MS = 60000;
export declare const DEFAULT_MAX_ROOMS_PER_PEER = 16;
export declare const MAX_PEERS_PER_ROOM = 64;
export declare const DEFAULT_ROOM = "default";
export declare const DEFAULT_RATE_MSG_PER_MIN = 30;
export declare const DEFAULT_RATE_URGENT_PER_MIN = 15;
export declare const DEFAULT_RATE_FORCE_PER_MIN = 1;
export declare const RATE_BUCKET_WINDOW_MS = 60000;
export declare const RECONNECT_BASE_MS = 250;
export declare const RECONNECT_MAX_MS = 5000;
export declare const OUTBOX_FLUSH_CAP = 50;
export declare const DEFAULT_AWAIT_REPLY_TIMEOUT_MS = 3600000;
export declare const DEFAULT_WAIT_ALL_TIMEOUT_MS = 3600000;
export declare const MIN_AWAIT_REPLY_TIMEOUT_MS = 25;
export declare const MAX_AWAIT_REPLY_TIMEOUT_MS = 3600000;
export declare const MAX_REMINDS = 2;
export declare const ENSURE_BROKER_POLL_MS = 50;
export declare const ENSURE_BROKER_MAX_POLLS = 60;
export declare const LOCK_RETRY_MAX = 3;
export declare const BROKER_STARTUP_DIAGNOSTIC_MAX_BYTES = 4096;
export declare const BROKER_STOP_TIMEOUT_MS = 5000;
export declare const BROKER_STOP_POLL_MS = 50;
export declare const TAILSCALE_COMMAND_TIMEOUT_MS = 5000;
export declare const TAILSCALE_MAX_OUTPUT_BYTES = 1048576;
export declare const TAILSCALE_DISCOVERY_CACHE_MS = 30000;
export declare const DEFAULT_TAILSCALE_PORT = 8712;
export declare const MAX_BROKER_ID_CHARS = 64;
export declare const TAILSCALE_MAX_SERVICE_PORTS = 65535;
export declare const TAILSCALE_DIAGNOSTIC_MAX_CHARS = 4096;
export declare const PROVIDER_RATE_LIMIT_HOLD_MS = 60000;
export declare const PROVIDER_BLOCKED_HOLD_MS = 1800000;
/** Observe model changes only while an error hold has pending recovery. */
export declare const PROVIDER_MODEL_WATCH_MS = 500;
/** last N ledger lines printed by `mesh tail` (no -f yet — Phase 3). */
export declare const CLI_TAIL_LINES = 20;
/** default `mesh send --await` timeout: a one-shot CLI script must not hang
 * for the 60-min mission default — long awaits belong to a live session. */
export declare const CLI_SEND_TIMEOUT_MS = 30000;
/** how long `mesh reserve` keeps its claim alive before closing: long enough
 * for peers to receive the reservation broadcast, short enough that the
 * debug one-shot never lingers (the claim dies with the connection anyway). */
export declare const CLI_RESERVE_GRACE_MS = 1500;
/** `mesh ping` await budget: short by design — liveness probing must not hang
 * for the send default; peer turns often take longer, expired ≠ down. */
export declare const CLI_PING_TIMEOUT_MS = 5000;
/** `mesh wait` default budget matches the extension's mesh_wait_all. */
export declare const CLI_WAIT_TIMEOUT_MS = 3600000;
/** max records `mesh ledger`/`mesh tail --limit` may print (bound, V5). */
export declare const CLI_LEDGER_MAX_LIMIT = 200;
/** default record count for `mesh ledger`. */
export declare const CLI_LEDGER_DEFAULT_LIMIT = 20;
/** Countdown line interval while `reserve --hold` runs. */
export declare const CLI_HOLD_TICK_MS = 30000;
/** Settle before the reserve dry-run conflict check: the welcome snapshot
 * already seeds peer claims — this only covers broadcasts in flight. */
export declare const CLI_RESERVE_SETTLE_MS = 250;
/** Mailbox flush above this count prints a read-receipt noise warning at
 * attach (each flushed message renders = one read frame to its sender). */
export declare const ATTACH_BULK_READ_THRESHOLD = 20;
/** interrupt (force modifier): how long to wait for the aborted run to
 * settle before falling back to a queued steer — a process-tree SIGKILL is
 * fast, the HOST settle is what can lag. */
export declare const INTERRUPT_IDLE_MAX_MS = 10000;
/** interrupt: re-abort when still busy after this long (an abort can be
 * swallowed by an end-of-turn race) — bounded retries, never a storm. */
export declare const INTERRUPT_REABORT_AFTER_MS = 1500;
/** interrupt: max ADDITIONAL aborts after the first one. */
export declare const INTERRUPT_REABORT_MAX = 2;
/** Prefix for fresh standalone aliases (`mesh attach` without --alias). */
export declare const STANDALONE_ALIAS_PREFIX = "standalone";
/** max backlog lines `mesh tail -f` replays at startup. */
export declare const CLI_TAIL_BACKLOG_MAX_LINES = 1000;
export declare const DEFAULT_INBOUND_BATCH_MS = 250;
/** One bounded retry when session injection throws (transient host state
 *  such as a turn teardown) before the failure is counted — the broker
 *  already acked `delivered`, so a silent drop is the worst outcome. */
export declare const INJECTION_RETRY_MS = 250;
export declare const DEFAULT_ACTIVITY_IDLE_MS = 120000;
export declare const DEFAULT_ACTIVITY_STUCK_MS = 900000;
/** Reservations expire after this long for CONFLICT CHECKS (peer side).
 *  6 h covers a long-but-legit run (measured: 3 h GPU render runs) with ×2
 *  margin, while stale claims left behind by a finished agent (measured:
 *  5 h+ held while idle) stop blocking peers. 0 = unlimited (opt-out). */
export declare const DEFAULT_RESERVATION_TTL_MS = 21600000;
/** Hard cap for `mesh reserve --hold`: at/above the system TTL a hold would
 * silently expire mid-hold and the countdown would misrepresent it. */
export declare const CLI_HOLD_MAX_MS = 21600000;
/** notify when ONE turn grows the session file by this much (bytes). */
export declare const DEFAULT_WATCHDOG_SPIKE_BYTES = 2097152;
/** notify when ONE assistant message carries more tool calls than this. */
export declare const DEFAULT_WATCHDOG_MAX_CALLS = 64;
/** a session-file drop of this size = compaction (context resync trigger). */
export declare const DEFAULT_WATCHDOG_COMPACTION_BYTES = 1048576;
export type InboundBroadcastPolicy = "immediate" | "deferred";
export type ContextVerbosity = "compact" | "full";
export declare const ALIAS_RAND_CHARS = 6;
export declare const MSG_ID_RAND_CHARS = 8;
export declare const ALIAS_REGEX: RegExp;
export declare const ROOM_REGEX: RegExp;
export declare const SHA256_HEX_REGEX: RegExp;
export declare const TRANSCRIPT_RING_SIZE = 200;
export declare const DEFAULT_LEDGER_MAX_BYTES = 5242880;
export declare const DEFAULT_TRANSCRIPT_RETENTION_DAYS = 7;
export type MeshEndpoint = {
    kind: "unix";
    path: string;
} | {
    kind: "tcp";
    host: string;
    port: number;
} | {
    kind: "tls";
    host: string;
    port: number;
};
/** Parse `tcp://host:port`, `tls://host:port`, `unix:///path`. */
export declare function parseEndpoint(url: string): MeshEndpoint | null;
export interface TailscaleConfig {
    mode: "off" | "service" | "node";
    service?: string;
    host?: string;
    tag?: string;
    port: number;
    execPath?: string;
}
export interface MeshConfig {
    alias?: string;
    rooms: string[];
    maxFrameBytes: number;
    heartbeatMs: number;
    brokerSilenceMs: number;
    mailboxCap: number;
    mailboxTtlMs: number;
    maxRoomsPerPeer: number;
    transcript: boolean;
    transcriptRetentionDays: number;
    ledgerMaxBytes: number;
    /** idle after this long without activity (status display). */
    activityIdleMs: number;
    /** flagged stuck when idle this long AND holding reservations. */
    activityStuckMs: number;
    /** reservations expire after this long (0 = unlimited).
    *  Conflict checks (edit/write blocking) ignore expired peer claims. */
    reservationTtlMs: number;
    /** context watchdog: notify on abnormal one-turn growth / tool-call
    *  bursts (rejected duplicate calls after a degenerate generation). */
    watchdog?: boolean;
    watchdogSpikeBytes?: number;
    watchdogMaxCalls?: number;
    /** interrupt receipts: answer force+interrupt senders with the honest
    *  outcome (aborted / still busy). Default true; opt-out for quiet meshes. */
    interruptReceipts?: boolean;
    watchdogCompactionBytes?: number;
    /** "compact" (default): minimal inbound prefix + reply hints on first
    *  sight only. "full": legacy verbose format (rollback switch). */
    contextVerbosity?: ContextVerbosity;
    /** broker listen endpoint (broker side). Default: local socket. */
    listen?: string;
    /** broker endpoint the CLIENT connects to (remote or local). */
    brokerUrl?: string;
    /** shared auth token — REQUIRED for tcp/tls endpoints. */
    brokerToken?: string;
    /** Logical mesh identity, checked before joining an automatically discovered broker. */
    meshId?: string;
    /** Stable broker identity; persisted by the standalone broker when omitted. */
    brokerId?: string;
    /** Opt-in discovery. Explicit brokerUrl always takes precedence. */
    tailscale?: TailscaleConfig;
    /** TLS server cert/key (broker, for tls:// listen). */
    tlsCert?: string;
    tlsKey?: string;
    /** TLS CA (client, to verify a custom server cert). */
    tlsCa?: string;
    /** accept self-signed certs (INSECURE — LAN/dev only). */
    tlsInsecure?: boolean;
    /** group inbound messages and inject them as ONE batched message
    *  (ms). 0 disables batching. While the agent is busy (long tool call)
    *  frames are HELD; when the busy period ends they are injected as one
    *  batch. */
    inboundBatchMs?: number;
    /** Unaddressed room broadcasts: wake now (default) or wait for a prompt. */
    inboundBroadcasts?: InboundBroadcastPolicy;
    /** safety cap — flush even while busy after this long (ms). */
    inboundBatchMaxHoldMs?: number;
    /** client connection-lifecycle debug log (MESH_DEBUG=1) — wire-level
    *  events only, NEVER bodies (hash-only discipline). */
    debug?: boolean;
}
export declare const DEFAULT_CONFIG: MeshConfig;
export declare function isMeshId(value: unknown): value is string;
export declare function isBrokerId(value: unknown): value is string;
/**
 * Load config: defaults < <stateDir>/config.json < env.
 * Env overrides: MESH_ALIAS, MESH_ROOMS (csv), MESH_MAX_FRAME_BYTES,
 * MESH_MAILBOX_CAP, MESH_MAILBOX_TTL_MS, MESH_TRANSCRIPT.
 */
export declare function loadConfig(stateDir?: string, env?: NodeJS.ProcessEnv): MeshConfig;
