import type { ThemeColor } from "./pi-types.js";
/** Distinct theme colors (accent + statuses + syntax palette). */
export declare const AGENT_COLORS: readonly ThemeColor[];
/** Stable color for an alias (hash → palette index). */
export declare function agentColor(alias: string): ThemeColor;
/** Colorize a sender alias with its agent color. */
export declare function colorizeSender(alias: string, fg: (color: ThemeColor, text: string) => string): string;
/** Full alias regex (used by renderers to recolor raw lines). */
export declare const ALIAS_RE: RegExp;
