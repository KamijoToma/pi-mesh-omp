import type { MeshFrame, MeshPriority } from "../protocol/envelope.js";
import { type FormatOpts } from "./inbound.js";
import type { DeliverAs, InboundMessage } from "./pi-types.js";
export interface BatchedInjection {
    content: string;
    deliverAs: DeliverAs;
}
/** Bounds for the batched content: named constants). */
export declare const BATCH_MAX_MESSAGES = 12;
export declare const BATCH_MAX_BODY_CHARS = 240;
/** Per-frame format opts builder — defaults keep the legacy behaviour. */
type FormatFor = (frame: MeshFrame) => FormatOpts;
declare function priorityRank(p: MeshPriority): number;
/**
 * one message per frame (formatted), batched content = numbered list.
 * The delivery mode is the most interrupting one in the lot (steer wins over
 * followUp); force frames NEVER go through the batcher (they have their own
 * abort path).
 */
export declare function buildBatchMessage(frames: MeshFrame[], formatFor?: FormatFor): BatchedInjection;
export declare function batchDetails(frames: MeshFrame[]): Record<string, unknown>;
/** Returns true when the frame must be injected IMMEDIATELY (never batched). */
export declare function bypassesBatch(frame: MeshFrame): boolean;
/** Build a single (non-batched) inbound message — used when batchMs = 0. */
export declare function buildSingleMessage(frame: MeshFrame, formatFor?: FormatFor): InboundMessage;
export { priorityRank };
/**
 * accumulates inbound frames and injects them as ONE batched message
 * (single turn). KEY: while the agent is BUSY (e.g. a long sleep/bash), the
 * batcher HOLDS the frames — they do not enter pi's queue one by one. When
 * the busy period ends (tool_result) or the agent turns idle, everything is
 * flushed as a single injection, so the whole burst lands in the
 * conversation at once. Frames that must not wait (force, remind) bypass
 * the batcher entirely.
 */
export declare class InboundBatcher {
    private readonly windowMs;
    private readonly maxHoldMs;
    private readonly isBusy;
    private readonly onFlush;
    private frames;
    private timer;
    private maxTimer;
    constructor(windowMs: number, maxHoldMs: number, isBusy: () => boolean, onFlush: (frames: MeshFrame[]) => void);
    push(frame: MeshFrame): void;
    /** The short window elapsed: flush only when the agent is NOT busy —
    *  otherwise hold (a long tool call is running; more messages may come)
    *  and arm a max-hold fallback so nothing is retained forever. */
    private onWindow;
    /** Move pending frames without injecting into the outgoing generation. */
    takeForReload(): MeshFrame[];
    /** Flush everything now (called on tool_result: the busy period ended). */
    flushNow(): void;
    get pending(): number;
}
