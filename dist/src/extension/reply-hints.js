/** Show the full instruction again after this silence (ms). */
export const HINT_SILENCE_MS = 30 * 60_000; // 30 min
/** Or at least once every N messages from the same sender. */
export const HINT_EVERY = 20;
/** Bounded: at most this many senders tracked (LRU-ish, cleared at cap). */
export const HINT_TRACKER_CAP = 128;
export class ReplyHintTracker {
    now;
    silenceMs;
    every;
    cap;
    seen = new Map();
    constructor(now = Date.now, silenceMs = HINT_SILENCE_MS, every = HINT_EVERY, cap = HINT_TRACKER_CAP) {
        this.now = now;
        this.silenceMs = silenceMs;
        this.every = every;
        this.cap = cap;
    }
    /** True when THIS frame should carry the full reply instruction. */
    shouldShow(from) {
        if (from === undefined || from === "")
            return true; // unknown → safe default
        const st = this.seen.get(from);
        const t = this.now();
        if (st === undefined) {
            this.remember(from, t);
            return true; // first sight
        }
        st.count += 1;
        const due = t - st.lastHintAt >= this.silenceMs || // long silence
            (this.every > 0 && st.count % this.every === 0); // periodic refresh
        if (due) {
            st.lastHintAt = t;
            return true;
        }
        return false;
    }
    remember(from, t) {
        if (this.seen.size >= this.cap)
            this.seen.clear(); // bounded
        this.seen.set(from, { lastHintAt: t, count: 1 });
    }
    /** Reset (e.g. after a detected compaction — context lost, re-teach). */
    reset() {
        this.seen.clear();
    }
    get size() {
        return this.seen.size;
    }
}
