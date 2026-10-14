// extension/reply-hints.ts — decides whether an inbound message carries the
// full "↩ reply with the mesh_reply tool using msgId …" instruction line or
// just its short msgId suffix. Compact context (v0.5) keeps the instruction
// only on first sight per sender, periodically after (survives /compact),
// and after long silences — cutting ~15-20 tokens per message from the
// permanent LLM context without ever losing the mesh_reply correlation
// (the short `(m_id)` suffix is ALWAYS present).
import type { MeshFrame } from "../protocol/envelope.js";

/** Show the full instruction again after this silence (ms). */
export const HINT_SILENCE_MS = 30 * 60_000; // 30 min
/** Or at least once every N messages from the same sender. */
export const HINT_EVERY = 20;
/** Bounded: at most this many senders tracked (LRU-ish, cleared at cap). */
export const HINT_TRACKER_CAP = 128;

interface HintState {
  lastHintAt: number;
  count: number;
}

export interface ReplyHintReloadState {
  seen: [string, HintState][];
}

export class ReplyHintTracker {
  private readonly seen = new Map<string, HintState>();

  constructor(
    private readonly now: () => number = Date.now,
    private readonly silenceMs: number = HINT_SILENCE_MS,
    private readonly every: number = HINT_EVERY,
    private readonly cap: number = HINT_TRACKER_CAP,
  ) {}

  snapshotForReload(): ReplyHintReloadState {
    return { seen: [...this.seen].map(([sender, state]) => [sender, { ...state }]) };
  }

  restoreAfterReload(state: ReplyHintReloadState): void {
    for (const [sender, hint] of state.seen) this.seen.set(sender, { ...hint });
  }

  /** True when THIS frame should carry the full reply instruction. */
  shouldShow(from: string | undefined): boolean {
    if (from === undefined || from === "") return true; // unknown → safe default
    const st = this.seen.get(from);
    const t = this.now();
    if (st === undefined) {
      this.remember(from, t);
      return true; // first sight
    }
    st.count += 1;
    const due =
      t - st.lastHintAt >= this.silenceMs || // long silence
      (this.every > 0 && st.count % this.every === 0); // periodic refresh
    if (due) {
      st.lastHintAt = t;
      return true;
    }
    return false;
  }

  private remember(from: string, t: number): void {
    if (this.seen.size >= this.cap) this.seen.clear(); // bounded
    this.seen.set(from, { lastHintAt: t, count: 1 });
  }

  /** Reset (e.g. after a detected compaction — context lost, re-teach). */
  reset(): void {
    this.seen.clear();
  }

  get size(): number {
    return this.seen.size;
  }
}
