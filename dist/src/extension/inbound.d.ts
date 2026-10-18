import type { MeshFrame, MeshPriority } from "../protocol/envelope.js";
import type { DeliverAs, ExtensionAPI, InboundMessage, SessionContext } from "./pi-types.js";
export interface InjectedInbound {
    message: InboundMessage;
    deliverAs: DeliverAs;
    aborted: boolean;
}
/** Honest outcome of an interrupt handling — feeds the receipt wording.
 *  NEVER claims a killed process (not observable from the extension). */
export type InterruptOutcome = "aborted" | "already_idle" | "still_busy" | "abort_unavailable";
export interface InterruptReport {
    outcome: InterruptOutcome;
    aborts: number;
    settledMs: number;
}
export interface InjectOpts extends FormatOpts {
    replyChain?: boolean;
    /** A passive mesh wait yielded: surface this message at the next model call. */
    wakeWaiting?: boolean;
    /** Called only after sendMessage successfully hands the message to the host. */
    onInjected?: () => void;
    /** Called once successful delivery concludes the interrupt handling. */
    onInterruptReport?: (report: InterruptReport) => void;
    /** Timing overrides (tests) — defaults come from the named constants. */
    interruptTimings?: {
        pollMs?: number;
        maxMs?: number;
        reabortAfterMs?: number;
        reabortMax?: number;
    };
}
/** Options for the inbound content format.
 *  - verbose: legacy full format (config contextVerbosity:"full")
 *  - showReplyHint: gate the "↩ reply with mesh_reply …" line (ReplyHintTracker)
 *  - homeRoom: the session's primary room — compact mode omits the room tag
 *    when the frame comes from it. */
export interface FormatOpts {
    replyChain?: boolean;
    verbose?: boolean;
    showReplyHint?: boolean;
    homeRoom?: string;
}
/** content format:
 *  VERBOSE (legacy / contextVerbosity "full"):
 *    `[mesh] @from (room X, priority, HH:MM:SS) body` + full hint line.
 *  COMPACT (v0.5 default): `[mesh] @from [room] prio HH:MM:SS body (m_id)`
 *    — room shown only when ≠ homeRoom, priority only when ≠ normal,
 *    hint line only when the ReplyHintTracker asks for it. The short
 *    `(m_id)` suffix is ALWAYS present: mesh_reply correlation never
 *    depends on the hint line. */
export declare function formatInboundContent(frame: MeshFrame, opts?: FormatOpts): string;
/** M4: local HH:MM:SS from an ISO timestamp (best effort). */
export declare function localTime(iso: string): string;
export declare function inboundDetails(frame: MeshFrame): Record<string, unknown>;
/** Map priority → delivery mode. */
export declare function mapPriority(priority: MeshPriority): DeliverAs;
/**
 * a reply is an ANSWER to something the session is waiting for — it
 * must interrupt the current reflection (steer) instead of queuing until the
 * turn ends (followUp), otherwise the agent keeps working on stale context
 * and re-processes the answer later.
 */
export declare function mapReplyDelivery(frame: MeshFrame): DeliverAs;
/** Bound for the post-abort idle wait (force priority). */
export declare const FORCE_IDLE_POLL_MS = 50;
export declare const FORCE_IDLE_MAX_MS = 3000;
/**
 * Deliver a message once the host reports idle, polling every `pollMs` up to
 * `maxMs`. Used for force: after ctx.abort the steer queue may be purged by
 * the host (abort is not guaranteed to preserve queued messages), so we wait
 * for the run to settle and then start a fresh turn with triggerTurn.
 * Falls back to a plain steer send when the deadline passes.
 */
export declare function deliverWhenIdle(pi: Pick<ExtensionAPI, "sendMessage">, ctx: SessionContext, frame: MeshFrame, opts?: InjectOpts, pollMs?: number, maxMs?: number): void;
/**
 * Inject one inbound msg/mailbox/reply/remind frame into the Pi session.
 * force: controlled abort ONLY when the host exposes abort AND reports busy
 * (ctx.isIdle === false), then deliver once idle. Synchronous host failures throw.
 * force+interrupt: the dedicated repeater path (deliverInterrupted).
 */
export declare function injectInbound(pi: Pick<ExtensionAPI, "sendMessage">, ctx: SessionContext | null, frame: MeshFrame, opts?: InjectOpts): InjectedInbound;
/** failure counters for the inbound path, surfaced via /mesh broker. */
export interface InboundFailureCounters {
    ledgerFailures: number;
    transcriptFailures: number;
    injectionFailures: number;
}
export interface InboundDeps {
    ledger: {
        append(input: unknown): unknown;
    };
    transcript: {
        record(dir: "in" | "out", frame: MeshFrame): void;
    };
    selfAlias: string;
    counters: InboundFailureCounters;
    /** send a read receipt for an injected message (msg/mailbox only). */
    read?: (msgId: string, from: string) => void;
    /** tag a reply whose target is itself a reply (info-only delivery). */
    isReplyToReply?: (replyTo: string) => boolean;
    /** format options (verbosity/hints) threaded to injectInbound. */
    format?: FormatOpts;
    /** injection retry delay (defaults to INJECTION_RETRY_MS; tests shrink it). */
    retryMs?: number;
}
/** Tag reply-to-reply context before formatting, without claiming delivery. */
export declare function tagInboundReplyChain(frame: MeshFrame, deps: InboundDeps): void;
export declare function handleInboundFrame(pi: Pick<ExtensionAPI, "sendMessage">, ctx: SessionContext | null, frame: MeshFrame, deps: InboundDeps): void;
/** Persist hash-only metadata and acknowledge a successfully injected frame. */
export declare function recordInboundDelivery(frame: MeshFrame, deps: InboundDeps): void;
