/** Show the full instruction again after this silence (ms). */
export declare const HINT_SILENCE_MS: number;
/** Or at least once every N messages from the same sender. */
export declare const HINT_EVERY = 20;
/** Bounded: at most this many senders tracked (LRU-ish, cleared at cap). */
export declare const HINT_TRACKER_CAP = 128;
interface HintState {
    lastHintAt: number;
    count: number;
}
export interface ReplyHintReloadState {
    seen: [string, HintState][];
}
export declare class ReplyHintTracker {
    private readonly now;
    private readonly silenceMs;
    private readonly every;
    private readonly cap;
    private readonly seen;
    constructor(now?: () => number, silenceMs?: number, every?: number, cap?: number);
    snapshotForReload(): ReplyHintReloadState;
    restoreAfterReload(state: ReplyHintReloadState): void;
    /** True when THIS frame should carry the full reply instruction. */
    shouldShow(from: string | undefined): boolean;
    private remember;
    /** Reset (e.g. after a detected compaction — context lost, re-teach). */
    reset(): void;
    get size(): number;
}
export {};
