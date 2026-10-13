import { formatInboundContent, inboundDetails } from "./inbound.js";
/** Bounds for the batched content: named constants). */
export const BATCH_MAX_MESSAGES = 12;
export const BATCH_MAX_BODY_CHARS = 240;
function priorityRank(p) {
    return p === "force" ? 3 : p === "urgent" ? 2 : 1;
}
/**
 * one message per frame (formatted), batched content = numbered list.
 * The delivery mode is the most interrupting one in the lot (steer wins over
 * followUp); force frames NEVER go through the batcher (they have their own
 * abort path).
 */
export function buildBatchMessage(frames, formatFor = () => ({})) {
    const shown = frames.slice(0, BATCH_MAX_MESSAGES);
    const overflow = frames.length - shown.length;
    const parts = [`[mesh batch — ${frames.length} message${frames.length > 1 ? "s" : ""}]`];
    shown.forEach((f, i) => {
        const opts = formatFor(f);
        const body = formatInboundContent(f, {
            ...opts,
            replyChain: f.__replyChain === true,
        });
        parts.push(`${i + 1}) ${body}`);
    });
    if (overflow > 0)
        parts.push(`… (+${overflow} more — see mesh_history)`);
    parts.push("", "Treat each message independently. Urgent items first. Do NOT acknowledge them one by one.");
    const hasUrgent = frames.some((f) => f.priority === "urgent");
    const hasReply = frames.some((f) => f.type === "reply");
    const deliverAs = hasUrgent || hasReply ? "steer" : "followUp";
    return { content: parts.join("\n"), deliverAs };
}
export function batchDetails(frames) {
    return {
        kind: "mesh-batch",
        count: frames.length,
        messages: frames.map((f) => inboundDetails(f)),
    };
}
/** Returns true when the frame must be injected IMMEDIATELY (never batched). */
export function bypassesBatch(frame) {
    return frame.priority === "force" || frame.type === "remind";
}
/** Build a single (non-batched) inbound message — used when batchMs = 0. */
export function buildSingleMessage(frame, formatFor = () => ({})) {
    return {
        customType: "mesh-inbound",
        content: formatInboundContent(frame, {
            ...formatFor(frame),
            replyChain: frame.__replyChain === true,
        }),
        display: true,
        details: inboundDetails(frame),
    };
}
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
export class InboundBatcher {
    windowMs;
    maxHoldMs;
    isBusy;
    onFlush;
    frames = [];
    timer = null;
    maxTimer = null;
    constructor(windowMs, maxHoldMs, isBusy, onFlush) {
        this.windowMs = windowMs;
        this.maxHoldMs = maxHoldMs;
        this.isBusy = isBusy;
        this.onFlush = onFlush;
    }
    push(frame) {
        this.frames.push(frame);
        if (this.timer === null && this.windowMs > 0) {
            this.timer = setTimeout(() => this.onWindow(), this.windowMs);
            this.timer.unref();
        }
    }
    /** The short window elapsed: flush only when the agent is NOT busy —
    *  otherwise hold (a long tool call is running; more messages may come)
    *  and arm a max-hold fallback so nothing is retained forever. */
    onWindow() {
        this.timer = null;
        if (this.frames.length === 0)
            return;
        if (this.isBusy()) {
            if (this.maxTimer === null) {
                this.maxTimer = setTimeout(() => this.flushNow(), this.maxHoldMs);
                this.maxTimer.unref();
            }
            return;
        }
        this.flushNow();
    }
    /** Flush everything now (called on tool_result: the busy period ended). */
    flushNow() {
        if (this.timer !== null) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        if (this.maxTimer !== null) {
            clearTimeout(this.maxTimer);
            this.maxTimer = null;
        }
        if (this.frames.length === 0)
            return;
        const batch = this.frames;
        this.frames = [];
        this.onFlush(batch);
    }
    get pending() {
        return this.frames.length;
    }
}
