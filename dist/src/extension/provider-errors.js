// extension/provider-errors.ts — classify provider failures for the activity
// announcements. Mirrors pi's own retry classification (retry.js):
//   - QUOTA/BUDGET limits (FreeUsageLimitError, GoUsageLimitError,
//     insufficient_quota, monthly limit…) are PERMANENT — pi does not even
//     retry them. Retrying later cannot heal them: the account limit must
//     reset or the model must change → blocked, LONG hold.
//   - AUTH errors (401/403/invalid key) are PERMANENT → blocked.
//   - TRANSIENT throttles (429 rate limit, overloaded, 5xx) mean "retry
//     later can succeed" → rate_limited, short hold.
// The error message (assistant message stopReason="error" + errorMessage)
// is the ground truth — the provider SDK throws on HTTP errors, so the
// response-status event never fires for them.
const QUOTA_PATTERN = /FreeUsageLimitError|GoUsageLimitError|insufficient_quota|monthly usage limit|weekly usage limit|available balance|quota|billing|budget|usage limit/i;
const AUTH_PATTERN = /(^|\W)401($|\W)|(^|\W)403($|\W)|unauthorized|forbidden|invalid api key|authentication|permission denied/i;
const TRANSIENT_PATTERN = /(^|\W)429($|\W)|rate.?limit|too many requests|overloaded|(^|\W)500($|\W)|(^|\W)502($|\W)|(^|\W)503($|\W)|(^|\W)504($|\W)|temporarily unavailable|try again later|overloaded/i;
/** True when a retry later can realistically succeed. */
export function isTransientProviderError(status) {
    return status === 429 || status === 408 || status === 425 || status >= 500;
}
/** Map a provider HTTP status to the announced activity state (or undefined
 *  when the response is not an error). Used as a fallback signal only. */
export function providerErrorState(status) {
    if (status < 400)
        return undefined;
    return isTransientProviderError(status) ? "rate_limited" : "blocked";
}
/** Map a provider ERROR MESSAGE (from the failed assistant turn) to the
 *  announced activity state. Quota/auth win over transient keywords (the
 *  real 429 quota errors carry BOTH "429" and "FreeUsageLimitError"). */
export function providerErrorStateFromMessage(errorMessage) {
    if (errorMessage === "")
        return undefined;
    if (QUOTA_PATTERN.test(errorMessage))
        return "blocked";
    if (AUTH_PATTERN.test(errorMessage))
        return "blocked";
    if (TRANSIENT_PATTERN.test(errorMessage))
        return "rate_limited";
    return undefined;
}
