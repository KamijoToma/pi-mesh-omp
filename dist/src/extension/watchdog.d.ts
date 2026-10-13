/** Inputs gathered by the caller at turn_end (all best-effort). */
export interface TurnSample {
    /** Session-file size in bytes after the turn; null when unavailable. */
    fileBytes: number | null;
    /** Tool calls in the assistant message of this turn. */
    toolCalls: number;
    /** Tool results rejected by the host ("was not executed" …). */
    rejectedCalls: number;
    /** Epoch ms. */
    at: number;
}
export interface WatchdogThresholds {
    /** One-turn file growth above this → spike. */
    spikeBytes: number;
    /** More tool calls than this in ONE message → burst. */
    maxCalls: number;
    /** File-size drop beyond this → compaction detected. */
    compactionBytes: number;
}
export type WatchdogVerdict = {
    type: "ok";
} | {
    type: "burst";
    toolCalls: number;
    rejectedCalls: number;
    deltaBytes: number | null;
} | {
    type: "spike";
    deltaBytes: number;
} | {
    type: "compaction";
    deltaBytes: number;
};
/** Count toolCall blocks in a Pi assistant message (content array). */
export declare function countToolCalls(message: unknown): number;
/** Count rejected tool results ("was not executed … token limit …"). */
export declare function countRejected(toolResults: readonly unknown[]): number;
/**
 * Classify the turn against the previous sample.
 * - compaction wins over everything (a file drop is never a burst);
 * - burst is count-based (works even without file stats);
 * - spike is size-based (a huge-but-legal output);
 * - the FIRST sample has no baseline → ok.
 */
export declare function analyzeTurn(now: TurnSample, prev: TurnSample | null, cfg: WatchdogThresholds): WatchdogVerdict;
/** Human line for ctx.ui.notify (also shown in tests). */
export declare function renderVerdict(v: WatchdogVerdict): string;
