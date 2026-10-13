import type { ThemeBg, ThemeColor } from "./pi-types.js";
export interface RenderTheme {
    fg(color: ThemeColor, text: string): string;
    /** background application (theme.bg) — absent in tests/headless. */
    bg?(color: ThemeBg, text: string): string;
    /** Bold helper (theme.bold) for the box label. */
    bold?(text: string): string;
    /** Raw foreground ANSI code for a theme color — the verdict lines use it
     *  to build the per-agent BACKGROUND (38→48). */
    fgAnsi?(color: ThemeColor): string;
}
/** Colorize every @alias in a line with its OWN agent color. */
export declare function colorizeAliases(line: string, theme: RenderTheme): string;
export interface MeshInboundDetails {
    kind?: string;
    count?: number;
    msgId?: string;
    from?: string;
    room?: string;
    priority?: string;
}
/** Live entry payload (mesh-live,. */
export interface LiveEntryData {
    from?: string;
    room?: string;
    priority?: string;
    body?: string;
    at?: string;
}
/**
 * reproduce pi's default custom-message Box — background
 * customMessageBg, padding X=1/Y=1 — around OUR colored content, so mesh
 * messages get the "purple frame" back while keeping per-agent colors.
 * Falls back to plain lines when the theme has no bg (tests/headless).
 */
export declare function renderBox(contentLines: string[], width: number, theme: RenderTheme, label?: string): string[];
/** Render a mesh message (simple or batch) as a boxed, colored component. */
export declare function renderMeshInbound(content: string, details: MeshInboundDetails | undefined, width: number, theme: RenderTheme): string[];
/** Live inbound entry rendered while the agent is busy. */
export declare function renderLiveEntry(data: LiveEntryData | undefined, width: number, theme: RenderTheme): string[];
/** Verdict entry payload (mesh-verdict — the colored wait_all result). */
export interface VerdictEntryData {
    head?: string;
    answers?: {
        to: string;
        response: string;
    }[];
    missing?: {
        to: string;
        msgId: string;
    }[];
}
/** ANSI background code for a theme color (38→48 on the raw foreground
 *  code; works for 256-color and truecolor sequences). */
export declare function ansiBackground(color: ThemeColor, theme: RenderTheme): string | undefined;
/** Parse an ANSI color code (`38|48;5;N` or `38|48;2;R;G;B`) into RGB —
 *  accepts foreground AND background variants (the verdict passes the
 *  background code). */
export declare function ansiToRgb(ansi: string): {
    r: number;
    g: number;
    b: number;
} | undefined;
/** Perceptual luminance 0..1 — 0.5+ means a LIGHT background. */
export declare function luminance(rgb: {
    r: number;
    g: number;
    b: number;
}): number;
/** The wait_all verdict rendered as a colored entry: header in accent on the
 *  default box background, then one line per agent with the AGENT color as
 *  text AND background. Falls back to plain lines without a theme. */
export declare function renderVerdictEntry(data: VerdictEntryData | undefined, width: number, theme: RenderTheme): string[];
