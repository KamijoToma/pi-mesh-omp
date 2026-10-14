import { type MeshPriority, type MeshRole } from "../protocol/envelope.js";
export declare const DUPLICATE_WINDOW_MS = 10000;
export declare const LOOP_GUARD_KEYWORD = "mesh_send(";
export declare const LOOP_GUARD_WARNING = "loopGuard:matched";
/** re-replying to the same msgId inside this window is flagged. */
export declare const REPLY_REPEAT_WINDOW_MS = 600000;
export declare const REPLY_REPEAT_WARNING = "already_replied";
export interface GuardRateLimits {
    msgPerMin: number;
    urgentPerMin: number;
    forcePerMin: number;
}
export declare const DEFAULT_GUARD_LIMITS: GuardRateLimits;
export interface GuardInput {
    from: string;
    to: string;
    room: string;
    body: string;
    priority: MeshPriority;
    /** Sender role in the target room, if known client-side. */
    role?: MeshRole;
}
export type GuardResult = {
    ok: true;
    warnings: string[];
} | {
    ok: false;
    reason: string;
    warnings: string[];
};
interface SlidingWindow {
    count: number;
    windowStart: number;
}
export interface MeshGuardsReloadState {
    recent: [string, number][];
    windows: [string, SlidingWindow][];
    repliedTo: [string, number][];
}
export declare class MeshGuards {
    private readonly limits;
    private readonly now;
    /** (to|room|bodyHash) → last send ts (anti-duplicate window). */
    private readonly recent;
    private readonly windows;
    /** msgId → last reply ts: flag re-replies, never block). */
    private readonly repliedTo;
    constructor(limits?: GuardRateLimits, now?: () => number);
    snapshotForReload(): MeshGuardsReloadState;
    restoreAfterReload(state: MeshGuardsReloadState): void;
    /**
    * track that we replied to a msgId. Returns a WARNING when the same
    * msgId was answered inside REPLY_REPEAT_WINDOW_MS — agents re-answer on
    * reminds and after orchestrator re-sends; the warning (never a block)
    * makes them think before answering twice.
    */
    checkReply(msgId: string, now?: number): {
        warnings: string[];
    };
    checkSend(input: GuardInput): GuardResult;
    private capacity;
    private consumeWindow;
    private consume;
    private pruneRecent;
}
export {};
