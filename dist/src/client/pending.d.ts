import type { MeshFrame } from "../protocol/envelope.js";
export interface PendingResolution {
    kind: "reply" | "expired" | "error";
    frame?: MeshFrame;
    reason?: string;
}
/**
 * Pending awaitReply tracker.
 * - correlation strict: replyTo === msgId (any other reply ignored + counted)
 * - reminds at T/2 and 3T/4 via onRemind hook (client sends `remind`), max 2
 * - expiry terminal at expiresAt; late reply counted, not delivered
 * - launch flag: block:false missions resolve the SAME way, but the client
 *   additionally delivers the answer frame to the session (wake-on-answer)
 */
export declare class PendingReplies {
    private readonly onRemind;
    private readonly onChanged?;
    private readonly entries;
    /** Replies with no matching live pending (orphan/late), for observability. */
    unmatchedReplyCount: number;
    constructor(onRemind: (msgId: string) => void, onChanged?: (() => void) | undefined);
    register(msgId: string, expiresAtMs: number, launch?: boolean): Promise<PendingResolution>;
    /** True when msgId is a live LAUNCH mission (awaitReply + block:false).
     *  Consulted by the client BEFORE handleReply consumes the entry. */
    isLaunch(msgId: string): boolean;
    /** Route an inbound reply frame. Returns true if it resolved a pending. */
    handleReply(frame: MeshFrame): boolean;
    has(msgId: string): boolean;
    cancel(msgId: string, reason: string): void;
    /** shutdown with live pendings → all resolved error{shutting_down}. */
    cancelAll(reason: string): void;
    get size(): number;
}
