/** Distinct theme colors (accent + statuses + syntax palette). */
export const AGENT_COLORS = [
    "accent",
    "success",
    "warning",
    "error",
    "customMessageText",
    "syntaxString",
    "syntaxKeyword",
    "syntaxNumber",
    "syntaxType",
    "syntaxFunction",
    "syntaxComment",
    "dim",
];
/** Stable color for an alias (hash → palette index). */
export function agentColor(alias) {
    let h = 0;
    for (let i = 0; i < alias.length; i += 1) {
        h = (h * 31 + alias.charCodeAt(i)) >>> 0;
    }
    return AGENT_COLORS[h % AGENT_COLORS.length] ?? "accent";
}
/** Colorize a sender alias with its agent color. */
export function colorizeSender(alias, fg) {
    return fg(agentColor(alias), `@${alias}`);
}
/** Full alias regex (used by renderers to recolor raw lines). */
export const ALIAS_RE = /(@[a-z0-9][a-z0-9-]*)/g;
