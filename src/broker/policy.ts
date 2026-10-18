// broker/policy.ts — declarative governance via .mesh/policy.json.
// Evaluated at send time; no external domain imports.
import { readFileSync } from "node:fs";
import type { MeshErrorCode, MeshPriority } from "../protocol/envelope.js";
import { policyPath } from "../shared/paths.js";
import { DEFAULT_RATE_LIMITS, type RateLimits } from "./ratelimit.js";

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

export const DEFAULT_POLICY: MeshPolicy = {
  allow: [{ from: "*", to: "*", room: "*" }],
  deny: [],
  forceAllowedFrom: [],
  forceDowngrade: false,
  rateLimits: { ...DEFAULT_RATE_LIMITS },
};

/** Simple wildcard match: "*" matches all; "foo*" prefix; else exact. */
function matchPattern(pattern: string | undefined, value: string): boolean {
  if (pattern === undefined || pattern === "*") return true;
  if (pattern.endsWith("*")) return value.startsWith(pattern.slice(0, -1));
  return pattern === value;
}

function ruleMatches(rule: PolicyRule, ctx: { from: string; to: string; room: string }): boolean {
  return (
    matchPattern(rule.from, ctx.from) &&
    matchPattern(rule.to, ctx.to) &&
    matchPattern(rule.room, ctx.room)
  );
}

function parseRules(value: unknown): PolicyRule[] {
  if (!Array.isArray(value)) return [];
  const rules: PolicyRule[] = [];
  for (const item of value) {
    if (item !== null && typeof item === "object" && !Array.isArray(item)) {
      const r = item as Record<string, unknown>;
      rules.push({
        from: typeof r.from === "string" ? r.from : undefined,
        to: typeof r.to === "string" ? r.to : undefined,
        room: typeof r.room === "string" ? r.room : undefined,
      });
    }
  }
  return rules;
}

/** Load policy from disk; missing/invalid → permissive default. */
export function loadPolicy(stateDir?: string, env: NodeJS.ProcessEnv = process.env): MeshPolicy {
  let raw: unknown;
  try {
    raw = JSON.parse(readFileSync(policyPath(stateDir, env), "utf8"));
  } catch {
    return structuredClone(DEFAULT_POLICY);
  }
  if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
    return structuredClone(DEFAULT_POLICY);
  }
  const p = raw as Record<string, unknown>;
  const allow = parseRules(p.allow);
  const rl = (p.rateLimits ?? {}) as Record<string, unknown>;
  const posInt = (v: unknown, d: number): number =>
    typeof v === "number" && Number.isSafeInteger(v) && v > 0 ? v : d;
  return {
    allow: allow.length > 0 ? allow : [{ from: "*", to: "*", room: "*" }],
    deny: parseRules(p.deny),
    forceAllowedFrom: Array.isArray(p.forceAllowedFrom)
      ? p.forceAllowedFrom.filter((x): x is string => typeof x === "string")
      : [],
    forceDowngrade: p.forceDowngrade === true,
    rateLimits: {
      msgPerMin: posInt(rl.msgPerMin, DEFAULT_RATE_LIMITS.msgPerMin),
      urgentPerMin: posInt(rl.urgentPerMin, DEFAULT_RATE_LIMITS.urgentPerMin),
      forcePerMin: posInt(rl.forcePerMin, DEFAULT_RATE_LIMITS.forcePerMin),
    },
  };
}

export interface PolicyContext {
  from: string;
  to: string;
  room: string;
  priority: MeshPriority;
}

export type PolicyDecision =
  | { action: "allow" }
  | { action: "downgrade" } // force → urgent (forceDowngrade option)
  | { action: "deny"; code: MeshErrorCode };

/**
 * Evaluate policy at send time. Deny rules first, then allow list,
 * then force authorization.
 */
export function evaluatePolicy(policy: MeshPolicy, ctx: PolicyContext): PolicyDecision {
  for (const rule of policy.deny) {
    if (ruleMatches(rule, ctx)) return { action: "deny", code: "policy_denied" };
  }
  const allowed = policy.allow.some((rule) => ruleMatches(rule, ctx));
  if (!allowed) return { action: "deny", code: "policy_denied" };
  if (ctx.priority === "force" && !policy.forceAllowedFrom.includes(ctx.from)) {
    if (policy.forceDowngrade) return { action: "downgrade" };
    return { action: "deny", code: "policy_denied" };
  }
  return { action: "allow" };
}
