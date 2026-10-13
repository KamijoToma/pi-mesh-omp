/** True when a retry later can realistically succeed. */
export declare function isTransientProviderError(status: number): boolean;
/** Map a provider HTTP status to the announced activity state (or undefined
 *  when the response is not an error). Used as a fallback signal only. */
export declare function providerErrorState(status: number): "rate_limited" | "blocked" | undefined;
/** Map a provider ERROR MESSAGE (from the failed assistant turn) to the
 *  announced activity state. Quota/auth win over transient keywords (the
 *  real 429 quota errors carry BOTH "429" and "FreeUsageLimitError"). */
export declare function providerErrorStateFromMessage(errorMessage: string): "rate_limited" | "blocked" | undefined;
