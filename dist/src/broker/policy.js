// broker/policy.ts — declarative governance via .mesh/policy.json.
// Evaluated at send time; no external domain imports.
import { readFileSync } from "node:fs";
import { policyPath } from "../shared/paths.js";
import { DEFAULT_RATE_LIMITS } from "./ratelimit.js";
export const DEFAULT_POLICY = {
    allow: [{ from: "*", to: "*", room: "*" }],
    deny: [],
    forceAllowedFrom: [],
    forceDowngrade: false,
    rateLimits: { ...DEFAULT_RATE_LIMITS },
};
/** Simple wildcard match: "*" matches all; "foo*" prefix; else exact. */
function matchPattern(pattern, value) {
    if (pattern === undefined || pattern === "*")
        return true;
    if (pattern.endsWith("*"))
        return value.startsWith(pattern.slice(0, -1));
    return pattern === value;
}
function ruleMatches(rule, ctx) {
    return (matchPattern(rule.from, ctx.from) &&
        matchPattern(rule.to, ctx.to) &&
        matchPattern(rule.room, ctx.room));
}
function parseRules(value) {
    if (!Array.isArray(value))
        return [];
    const rules = [];
    for (const item of value) {
        if (item !== null && typeof item === "object" && !Array.isArray(item)) {
            const r = item;
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
export function loadPolicy(stateDir, env = process.env) {
    let raw;
    try {
        raw = JSON.parse(readFileSync(policyPath(stateDir, env), "utf8"));
    }
    catch {
        return structuredClone(DEFAULT_POLICY);
    }
    if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
        return structuredClone(DEFAULT_POLICY);
    }
    const p = raw;
    const allow = parseRules(p.allow);
    const rl = (p.rateLimits ?? {});
    const posInt = (v, d) => typeof v === "number" && Number.isFinite(v) && v > 0 ? Math.floor(v) : d;
    return {
        allow: allow.length > 0 ? allow : [{ from: "*", to: "*", room: "*" }],
        deny: parseRules(p.deny),
        forceAllowedFrom: Array.isArray(p.forceAllowedFrom)
            ? p.forceAllowedFrom.filter((x) => typeof x === "string")
            : [],
        forceDowngrade: p.forceDowngrade === true,
        rateLimits: {
            msgPerMin: posInt(rl.msgPerMin, DEFAULT_RATE_LIMITS.msgPerMin),
            urgentPerMin: posInt(rl.urgentPerMin, DEFAULT_RATE_LIMITS.urgentPerMin),
            forcePerMin: posInt(rl.forcePerMin, DEFAULT_RATE_LIMITS.forcePerMin),
        },
    };
}
/**
 * Evaluate policy at send time. Deny rules first, then allow list,
 * then force authorization.
 */
export function evaluatePolicy(policy, ctx) {
    for (const rule of policy.deny) {
        if (ruleMatches(rule, ctx))
            return { action: "deny", code: "policy_denied" };
    }
    const allowed = policy.allow.some((rule) => ruleMatches(rule, ctx));
    if (!allowed)
        return { action: "deny", code: "policy_denied" };
    if (ctx.priority === "force" && !policy.forceAllowedFrom.includes(ctx.from)) {
        if (policy.forceDowngrade)
            return { action: "downgrade" };
        return { action: "deny", code: "policy_denied" };
    }
    return { action: "allow" };
}
