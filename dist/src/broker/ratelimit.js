// broker/ratelimit.ts — per-peer token buckets (msg/urgent/force).
import { DEFAULT_RATE_FORCE_PER_MIN, DEFAULT_RATE_MSG_PER_MIN, DEFAULT_RATE_URGENT_PER_MIN, RATE_BUCKET_WINDOW_MS, } from "../shared/config.js";
import { newBucket } from "./state.js";
export const DEFAULT_RATE_LIMITS = {
    msgPerMin: DEFAULT_RATE_MSG_PER_MIN,
    urgentPerMin: DEFAULT_RATE_URGENT_PER_MIN,
    forcePerMin: DEFAULT_RATE_FORCE_PER_MIN,
};
function getRates(state, alias, limits) {
    let r = state.rates.get(alias);
    if (!r) {
        r = {
            msg: newBucket(limits.msgPerMin),
            urgent: newBucket(limits.urgentPerMin),
            force: newBucket(limits.forcePerMin),
        };
        state.rates.set(alias, r);
    }
    return r;
}
function capacity(kind, limits) {
    switch (kind) {
        case "msg":
            return limits.msgPerMin;
        case "urgent":
            return limits.urgentPerMin;
        case "force":
            return limits.forcePerMin;
    }
}
function refill(bucket, cap, now) {
    if (cap <= 0)
        return;
    const elapsed = now - bucket.lastRefillAt;
    if (elapsed <= 0)
        return;
    const gained = (elapsed / RATE_BUCKET_WINDOW_MS) * cap;
    bucket.tokens = Math.min(cap, bucket.tokens + gained);
    bucket.lastRefillAt = now;
}
/** Consume one token if available. `urgent`/`force` also consume a `msg` token. */
export function checkRate(state, alias, kind, limits = DEFAULT_RATE_LIMITS, now = Date.now()) {
    const rates = getRates(state, alias, limits);
    refill(rates.msg, limits.msgPerMin, now);
    refill(rates.urgent, limits.urgentPerMin, now);
    refill(rates.force, limits.forcePerMin, now);
    const bucket = rates[kind];
    if (bucket.tokens < 1)
        return false;
    if (kind !== "msg" && rates.msg.tokens < 1)
        return false;
    bucket.tokens -= 1;
    if (kind !== "msg")
        rates.msg.tokens -= 1;
    return true;
}
