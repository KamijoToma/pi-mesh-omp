import type { MeshErrorCode, MeshPriority } from "../protocol/envelope.js";
import { type RateLimits } from "./ratelimit.js";
export interface PolicyRule {
    from?: string;
    to?: string;
    room?: string;
}
export interface MeshPolicy {
    allow: PolicyRule[];
    deny: PolicyRule[];
    forceAllowedFrom: string[];
    /** When true, unauthorized force is downgraded to urgent instead of denied. */
    forceDowngrade: boolean;
    rateLimits: RateLimits;
}
export declare const DEFAULT_POLICY: MeshPolicy;
/** Load policy from disk; missing/invalid → permissive default. */
export declare function loadPolicy(stateDir?: string, env?: NodeJS.ProcessEnv): MeshPolicy;
export interface PolicyContext {
    from: string;
    to: string;
    room: string;
    priority: MeshPriority;
}
export type PolicyDecision = {
    action: "allow";
} | {
    action: "downgrade";
} | {
    action: "deny";
    code: MeshErrorCode;
};
/**
 * Evaluate policy at send time. Deny rules first, then allow list,
 * then force authorization.
 */
export declare function evaluatePolicy(policy: MeshPolicy, ctx: PolicyContext): PolicyDecision;
