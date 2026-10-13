// extension/guards.ts — client-side send guards: self-send, duplicates,
// rate caps, loop warning, observer role.
// 1. self-send → blocked
// 2. duplicate (to, room, sha256(body)) within 10 s window → blocked
// 3. client-side rate caps (mirror of broker caps): 30 msg/min, 5 urgent/min, 1 force/min
// 4. loopGuard: body containing the literal `mesh_send(` → WARNING ONLY (never blocks —
//  keyword blocking over-fires; warn instead of hard-block)
// 5. observer role → observer_readonly (broker enforces too; fail early)
import { normalizeAlias } from "../protocol/envelope.js";
import { sha256 } from "../protocol/frames.js";
import { DEFAULT_RATE_FORCE_PER_MIN, DEFAULT_RATE_MSG_PER_MIN, DEFAULT_RATE_URGENT_PER_MIN, RATE_BUCKET_WINDOW_MS, } from "../shared/config.js";
export const DUPLICATE_WINDOW_MS = 10_000;
export const LOOP_GUARD_KEYWORD = "mesh_send(";
export const LOOP_GUARD_WARNING = "loopGuard:matched";
/** re-replying to the same msgId inside this window is flagged. */
export const REPLY_REPEAT_WINDOW_MS = 600_000; // 10 min
export const REPLY_REPEAT_WARNING = "already_replied";
export const DEFAULT_GUARD_LIMITS = {
    msgPerMin: DEFAULT_RATE_MSG_PER_MIN,
    urgentPerMin: DEFAULT_RATE_URGENT_PER_MIN,
    forcePerMin: DEFAULT_RATE_FORCE_PER_MIN,
};
export class MeshGuards {
    limits;
    now;
    /** (to|room|bodyHash) → last send ts (anti-duplicate window). */
    recent = new Map();
    windows = new Map();
    /** msgId → last reply ts: flag re-replies, never block). */
    repliedTo = new Map();
    constructor(limits = DEFAULT_GUARD_LIMITS, now = Date.now) {
        this.limits = limits;
        this.now = now;
    }
    /**
    * track that we replied to a msgId. Returns a WARNING when the same
    * msgId was answered inside REPLY_REPEAT_WINDOW_MS — agents re-answer on
    * reminds and after orchestrator re-sends; the warning (never a block)
    * makes them think before answering twice.
    */
    checkReply(msgId, now = Date.now()) {
        const warnings = [];
        const last = this.repliedTo.get(msgId);
        if (last !== undefined && now - last < REPLY_REPEAT_WINDOW_MS) {
            warnings.push(REPLY_REPEAT_WARNING);
        }
        this.repliedTo.set(msgId, now);
        if (this.repliedTo.size > 512) {
            for (const [id, ts] of this.repliedTo) {
                if (now - ts >= REPLY_REPEAT_WINDOW_MS)
                    this.repliedTo.delete(id);
            }
        }
        return { warnings };
    }
    checkSend(input) {
        const warnings = [];
        const now = this.now();
        const to = normalizeAlias(input.to);
        const from = normalizeAlias(input.from);
        // 1. self-send
        if (to === from)
            return { ok: false, reason: "self_send", warnings };
        // 5. observer fails early (broker would refuse too)
        if (input.role === "observer") {
            return { ok: false, reason: "observer_readonly", warnings };
        }
        // 2. duplicate (to, room, bodyHash) inside the 10 s window
        //  (broadcast sends share the synthetic target "*" so identical
        //  broadcasts to the same room are also deduped)
        const dupKey = `${to}|${input.room}|${sha256(input.body)}`;
        const last = this.recent.get(dupKey);
        if (last !== undefined && now - last < DUPLICATE_WINDOW_MS) {
            return { ok: false, reason: "duplicate_in_window", warnings };
        }
        // 3. client-side caps (urgent/force also consume a msg token, like the broker)
        const kind = input.priority === "force" ? "force" : input.priority === "urgent" ? "urgent" : "msg";
        if (!this.consume(kind, now)) {
            return { ok: false, reason: `rate_limited:${kind}`, warnings };
        }
        this.recent.set(dupKey, now);
        this.pruneRecent(now);
        // 4. loopGuard — warn only, NEVER block
        if (input.body.includes(LOOP_GUARD_KEYWORD))
            warnings.push(LOOP_GUARD_WARNING);
        return { ok: true, warnings };
    }
    capacity(kind) {
        switch (kind) {
            case "msg":
                return this.limits.msgPerMin;
            case "urgent":
                return this.limits.urgentPerMin;
            case "force":
                return this.limits.forcePerMin;
        }
    }
    consumeWindow(kind, now) {
        const cap = this.capacity(kind);
        let w = this.windows.get(kind);
        if (w === undefined || now - w.windowStart >= RATE_BUCKET_WINDOW_MS) {
            w = { count: 0, windowStart: now };
            this.windows.set(kind, w);
        }
        if (w.count >= cap)
            return false;
        w.count += 1;
        return true;
    }
    consume(kind, now) {
        if (kind === "msg")
            return this.consumeWindow("msg", now);
        // urgent/force consume their own bucket AND a msg token (mirror of broker)
        const okKind = this.consumeWindow(kind, now);
        if (!okKind)
            return false;
        return this.consumeWindow("msg", now);
    }
    pruneRecent(now) {
        if (this.recent.size < 256)
            return;
        for (const [key, ts] of this.recent) {
            if (now - ts >= DUPLICATE_WINDOW_MS)
                this.recent.delete(key);
        }
    }
}
