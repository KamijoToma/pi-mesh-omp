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
 * - launch flag: background missions resolve the SAME way, but the client
 *   additionally delivers an answer not owned by an active group
 */
export declare class PendingReplies {
    private readonly onRemind;
    private readonly onChanged?;
    private readonly onSettled?;
    private readonly entries;
    /** Replies with no matching live pending (orphan/late), for observability. */
    unmatchedReplyCount: number;
    constructor(onRemind: (msgId: string) => void, onChanged?: (() => void) | undefined, onSettled?: ((msgId: string, resolution: PendingResolution) => void) | undefined);
    register(msgId: string, expiresAtMs: number, launch?: boolean): Promise<PendingResolution>;
    /** True for a live background mission; checked before handleReply consumes it. */
    isLaunch(msgId: string): boolean;
    /** A yielded tool no longer owns its answer; retain deadlines and correlation. */
    promoteToLaunch(msgId: string): void;
    /** Route an inbound reply frame. Returns true if it resolved a pending. */
    handleReply(frame: MeshFrame): boolean;
    has(msgId: string): boolean;
    cancel(msgId: string, reason: string): void;
    /** Resolve all live entries with the caller's lifecycle reason. */
    cancelAll(reason: string): void;
    get size(): number;
}
