import { MeshClient } from "../client/client.js";
import { MAX_BODY_BYTES, type MeshConfig } from "../shared/config.js";
import type { DeferredInbox } from "./deferred-inbox.js";
import type { MeshGuards } from "./guards.js";
import { type FileServeHandle } from "../fileserve/serve.js";
import { type MeshIdentity } from "../shared/identity-store.js";
import type { MeshLedger } from "./ledger.js";
import type { ExtensionAPI, SessionContext } from "./pi-types.js";
import type { MeshTranscript } from "./transcript.js";
import type { MeshInboundReloadState } from "./attach.js";
/** Shared per-session runtime, built at session_start (index.ts). */
export interface MeshRuntime {
    client: MeshClient;
    ledger: MeshLedger;
    transcript: MeshTranscript;
    guards: MeshGuards;
    ctx: SessionContext | null;
    stateDir: string;
    runtimeDir: string;
    /** Pi session id — stable across /reload (identity key). */
    sessionId: string;
    /** Identity persistence (alias/rooms/reservations survive reloads). */
    identity: MeshIdentity;
    /** transferred history from a /mesh new handoff (injected at ready). */
    pendingHistory?: string[];
    /** Saved tailnet selection awaiting an explicit room join. */
    pendingTailnetConfig?: MeshConfig;
    /** inbound batching (flush remaining frames on shutdown). */
    batcher?: {
        flushNow(): void;
    };
    /** Unaddressed broadcasts held separately from immediate traffic. */
    deferredInbox?: DeferredInbox;
    /** Discard locally held old-graph messages before an explicit graph selection. */
    clearInbound?: () => void;
    /** Display-only entry outside the LLM context (mesh-verdict colors). */
    appendEntry?: (type: string, data: unknown) => void;
    /** pending auto-release timers per reserved pattern (mesh_reserve
    *  autoReleaseMs) — cleared on release and at session_shutdown. */
    reserveTimers?: Map<string, {
        timer: NodeJS.Timeout;
        expiresAt: number;
    }>;
    /** Live file offers (mesh_file_serve). At most FILE_SERVE_MAX_CONCURRENT:
    *  a new offer replaces the previous one. Closed at session_shutdown and
    *  NOT transferred across /reload — the offer URL points at a listener in
    *  the old module graph, so it dies with it and peers get honest 404s. */
    serveHandles?: FileServeHandle[];
    /** While set (ms epoch), inbound frames are HELD, not injected — the
     *  provider is rejecting turns (429) and every injection burns one. */
    rateLimitedUntil?: number;
    /** Deliver everything queued while rate-limited (called at hold expiry). */
    flushHeld?: () => void;
    /** D41: flip the client-handler detach guard (session_shutdown). */
    markDetached?: () => void;
    /** D41: clear the rate-limit hold timer (session_shutdown). */
    stopHold?: () => void;
    /** Freeze inbound injection, then transfer the queues after the socket closes. */
    pauseForReload?: () => void;
    captureInboundForReload?: () => MeshInboundReloadState;
    activateAfterReload?: () => void;
    /** Force deliveries deferred until host-idle must finish before handover. */
    isInboundReloadSafe?: () => boolean;
    /** Foreground control requests remain tracked until their real result settles. */
    pendingToolOperations?: Set<(reason: string, msgId?: string) => boolean>;
    /** Shared session/client guard for delayed operation results. */
    isCurrent?: () => boolean;
    /** Called by the context watchdog when a compaction is detected —
    *  lets attach.ts resync the mesh context block (conventions lost). */
    onCompactionDetected?: () => void;
    /** interrupt receipts on/off (config interruptReceipts). */
    interruptReceipts?: boolean;
    /** MEMORY-ONLY interrupt stats for mesh_status (never persisted). */
    interruptCount?: number;
    lastInterruptAt?: string;
    lastInterruptOutcome?: string;
    startedAt: number;
    /** inbound-path disk/injection failure counters (see index.ts). */
    ledgerFailures: number;
    transcriptFailures: number;
    injectionFailures: number;
}
export type GetRuntime = () => MeshRuntime | null;
/** Yield foreground control requests without cancelling their transactions. */
export declare function interruptToolOperations(rt: MeshRuntime, reason: string, msgId?: string): boolean;
/** Minimum busy duration before an awaitReply warning (a 30 s bash turn
 *  must not nag — measured incident: 6/6 expired missions toward agents
 *  running 4-10 min GPU renders). */
export declare const BUSY_WARN_MIN_MS = 300000;
export interface TargetCheck {
    /** Hard local error — the target can NEVER be a real alias. */
    error?: string;
    /** How to fix it (appended to the error text). */
    hint?: string;
    /** Non-blocking caution (alias unseen — may be offline/renamed). */
    warning?: string;
}
/**
 * Local mesh_send target validation (M1). Catches address-space
 * confusions BEFORE the broker round-trip — observed in the wild:
 * to:"*" and to:"cs-room-broadcast" (broadcast intent), both doomed.
 * A syntactically-fine alias that is simply not (yet) known is only a
 * WARNING: presence is best-effort, never a reason to block a send.
 */
export declare function checkSendTarget(to: string, knowsPeer: (alias: string) => boolean, knownCount: number): TargetCheck;
/** Busy-target warning for awaitReply sends. Pure. */
export declare function busyTargetWarning(to: string, busyMs: number | undefined, timeoutMs: number, minBusyMs?: number): string | undefined;
/** Phase 3: group verdict of a status snapshot — counts by state, and
 *  "likely done" = idle peers with NO mission of ours still waiting on
 *  them. Pure, exported for tests. */
export interface StatusSummary {
    total: number;
    working: number;
    idle: number;
    stuck: number;
    rateLimited: number;
    blocked: number;
    likelyDone: number;
}
export declare function buildStatusSummary(peers: {
    alias: string;
    activity?: {
        state: "busy" | "idle" | "rate_limited" | "blocked";
        at: string;
    };
    reservations?: {
        pattern: string;
    }[];
    lastSeenAt?: string;
}[], missions: {
    to: string;
    status: string;
}[], idleMs?: number, stuckMs?: number, now?: number): StatusSummary;
/** Shared detail suffix; announced activity wins over the legacy idle heuristic. */
export declare function formatPeerActivity(peer: {
    activity?: {
        state: "busy" | "idle" | "rate_limited" | "blocked";
        at: string;
    };
    lastSeenAt?: string;
    reservations?: readonly unknown[];
}, idleMs: number, stuckMs: number, now?: number): string;
/** Arm one auto-release timer per freshly reserved pattern. */
export declare function armReserveTimers(rt: MeshRuntime, patterns: string[], ms: number): void;
/** Clear auto-release timers (release/shutdown). */
export declare function clearReserveTimers(rt: MeshRuntime, patterns?: string[]): void;
/** Close every live file offer (session_shutdown / cmdReset / detach).
 *  Late downloaders get an honest refused socket, never a hung one. */
export declare function closeServeHandles(rt: MeshRuntime): void;
/**
 * Bind/offer candidates for a file offer, best first. Tailscale self
 * addresses (100.x CGNAT) lead on a tailnet — peers on other machines can
 * always reach them; loopback follows for same-machine handoff; the LAN
 * address is offered when no tailnet is configured.
 */
export declare function serveHostCandidates(status: {
    self: {
        addresses: string[];
    } | null;
} | undefined, family: "IPv4" | "IPv6"): string[];
export declare function registerTools(pi: ExtensionAPI, getRuntime: GetRuntime): void;
export { MAX_BODY_BYTES };
