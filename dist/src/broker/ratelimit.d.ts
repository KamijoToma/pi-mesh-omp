import { type BrokerState } from "./state.js";
export type RateKind = "msg" | "urgent" | "force";
export interface RateLimits {
    msgPerMin: number;
    urgentPerMin: number;
    forcePerMin: number;
}
export declare const DEFAULT_RATE_LIMITS: RateLimits;
/** Consume one token if available. `urgent`/`force` also consume a `msg` token. */
export declare function checkRate(state: BrokerState, alias: string, kind: RateKind, limits?: RateLimits, now?: number): boolean;
