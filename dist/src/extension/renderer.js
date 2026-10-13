// extension/renderer.ts — colored rendering of mesh messages in
// the conversation, INSIDE the pi custom-message box (background
// customMessageBg — the "purple frame" of the default rendering,.
// Per-agent colors everywhere: simple messages, batches and live entries.
// No Pi imports except pi-tui helpers (wrap + visibleWidth).
import { visibleWidth, wrapTextWithAnsi } from "@mariozechner/pi-tui";
import { ALIAS_RE, agentColor } from "./colors.js";
/** Colorize every @alias in a line with its OWN agent color. */
export function colorizeAliases(line, theme) {
    return line.replace(ALIAS_RE, (m) => theme.fg(agentColor(m.slice(1)), m));
}
/**
 * reproduce pi's default custom-message Box — background
 * customMessageBg, padding X=1/Y=1 — around OUR colored content, so mesh
 * messages get the "purple frame" back while keeping per-agent colors.
 * Falls back to plain lines when the theme has no bg (tests/headless).
 */
export function renderBox(contentLines, width, theme, label = "[mesh-inbound]") {
    const bg = theme.bg;
    if (bg === undefined)
        return contentLines;
    const inner = Math.max(1, width - 2);
    const out = [];
    const emptyLine = bg("customMessageBg", " ".repeat(Math.max(0, width)));
    out.push(emptyLine); // padding Y=1 top
    // Label line (like pi's default box): [customType] bold in customMessageLabel
    const labelText = theme.bold !== undefined ? theme.bold(label) : label;
    out.push(padBg(bg, theme.fg("customMessageLabel", labelText), width));
    out.push(emptyLine); // spacer
    for (const raw of contentLines) {
        if (raw === "") {
            out.push(emptyLine);
            continue;
        }
        // wrap to the inner width (padding X=1 on both sides), then pad the rest
        for (const l of wrapTextWithAnsi(raw, inner)) {
            out.push(padBg(bg, l, width));
        }
    }
    out.push(emptyLine); // padding Y=1 bottom
    return out;
}
/** Left-pad X=1, fill the remaining width with spaces, apply the background. */
function padBg(bg, line, width) {
    const vis = visibleWidth(line);
    const padded = ` ${line}${" ".repeat(Math.max(0, width - vis - 1))}`;
    return bg("customMessageBg", padded);
}
function pushLine(out, line, width, theme) {
    if (line.trim() === "") {
        out.push("");
        return;
    }
    let colored = line;
    if (line.startsWith("↩"))
        colored = theme.fg("muted", line);
    out.push(...wrapTextWithAnsi(colorizeAliases(colored, theme), width));
}
/** Render a mesh message (simple or batch) as a boxed, colored component. */
export function renderMeshInbound(content, details, width, theme) {
    const inner = [];
    if (details?.kind === "mesh-batch" && typeof details.count === "number") {
        inner.push(theme.fg("accent", `▚ mesh batch — ${details.count} messages`));
    }
    for (const raw of content.split("\n"))
        pushLine(inner, raw, width, theme);
    return renderBox(inner, width, theme);
}
/** Live inbound entry rendered while the agent is busy. */
export function renderLiveEntry(data, width, theme) {
    if (data === undefined || data.from === undefined) {
        return renderBox([theme.fg("dim", "[mesh]")], width, theme);
    }
    const sender = colorizeSender(data.from, theme.fg);
    const room = data.room !== undefined ? theme.fg("dim", ` [${data.room}]`) : "";
    const time = data.at !== undefined ? theme.fg("dim", ` ${data.at.slice(11, 19)}`) : "";
    const body = (data.body ?? "").replace(/\s+/g, " ").trim();
    const head = `⇠ ${sender}${room}${time}`;
    const inner = [...wrapTextWithAnsi(head, width)];
    if (body.length > 0) {
        inner.push(...wrapTextWithAnsi(colorizeAliases(body, theme), width));
    }
    return renderBox(inner, width, theme, "[mesh-live]");
}
function colorizeSender(alias, fg) {
    return fg(agentColor(alias), `@${alias}`);
}
/** ANSI background code for a theme color (38→48 on the raw foreground
 *  code; works for 256-color and truecolor sequences). */
export function ansiBackground(color, theme) {
    const fg = theme.fgAnsi?.(color);
    if (fg === undefined || !fg.includes("38"))
        return undefined;
    return fg.replace("38", "48");
}
/** Standard xterm 256-color palette (base 16 + 6×6×6 cube + 24 grays). */
const XTERM_256 = (() => {
    const base = [
        [0, 0, 0], [128, 0, 0], [0, 128, 0], [128, 128, 0],
        [0, 0, 128], [128, 0, 128], [0, 128, 128], [192, 192, 192],
        [128, 128, 128], [255, 0, 0], [0, 255, 0], [255, 255, 0],
        [0, 0, 255], [255, 0, 255], [0, 255, 255], [255, 255, 255],
    ];
    const cube = [];
    for (const r of [0, 95, 135, 175, 215, 255]) {
        for (const g of [0, 95, 135, 175, 215, 255]) {
            for (const b of [0, 95, 135, 175, 215, 255])
                cube.push([r, g, b]);
        }
    }
    const grays = [];
    for (let i = 0; i < 24; i += 1) {
        const v = 8 + 10 * i;
        grays.push([v, v, v]);
    }
    return [...base, ...cube, ...grays];
})();
/** Parse an ANSI color code (`38|48;5;N` or `38|48;2;R;G;B`) into RGB —
 *  accepts foreground AND background variants (the verdict passes the
 *  background code). */
export function ansiToRgb(ansi) {
    const m256 = /\x1b\[(?:38|48);5;(\d{1,3})m/.exec(ansi);
    if (m256 !== null) {
        const idx = Number(m256[1]);
        const c = XTERM_256[idx];
        if (c === undefined)
            return undefined;
        return { r: c[0], g: c[1], b: c[2] };
    }
    const mTrue = /\x1b\[(?:38|48);2;(\d{1,3});(\d{1,3});(\d{1,3})m/.exec(ansi);
    if (mTrue !== null) {
        return { r: Number(mTrue[1]), g: Number(mTrue[2]), b: Number(mTrue[3]) };
    }
    return undefined;
}
/** Perceptual luminance 0..1 — 0.5+ means a LIGHT background. */
export function luminance(rgb) {
    return (0.299 * rgb.r + 0.587 * rgb.g + 0.114 * rgb.b) / 255;
}
/** One verdict entry: text wrapped to `width`, EVERY line tinted in the
 *  agent's fg color with FULL-WIDTH background (padded to `width`).
 *  Wrapping (not hard truncation) is mandatory: CJK chars are double-width,
 *  so a char-count cap alone can produce lines wider than the terminal —
 *  which crashes pi's renderer (uncaughtException in doRender). */
function verdictLines(marker, to, text, width, theme) {
    const color = agentColor(to);
    const full = `${marker} @${to}: ${text}`;
    const parts = wrapTextWithAnsi(full, Math.max(1, width));
    const bg = ansiBackground(color, theme);
    // without a theme the agent color is used for the text (plain background)
    if (bg === undefined)
        return parts.map((part) => theme.fg(color, part));
    // adaptive contrast: dark text on LIGHT agent backgrounds, light text on
    // DARK ones — always readable
    const rgb = ansiToRgb(bg);
    const fgCode = rgb !== undefined && luminance(rgb) > 0.5
        ? "\x1b[38;5;232m" // near-black
        : "\x1b[38;5;255m"; // near-white
    return parts.map((part) => {
        const vis = visibleWidth(part);
        return `${bg}${fgCode}${part}${" ".repeat(Math.max(0, width - vis))}\x1b[39m\x1b[49m`;
    });
}
/** The wait_all verdict rendered as a colored entry: header in accent on the
 *  default box background, then one line per agent with the AGENT color as
 *  text AND background. Falls back to plain lines without a theme. */
export function renderVerdictEntry(data, width, theme) {
    const head = data?.head ?? "wait_all";
    const out = [];
    const pad = (l) => `${l}${" ".repeat(Math.max(0, width - visibleWidth(l)))}`;
    // header on the neutral box background, with an empty line ABOVE (the
    // entry must not touch the previous content) and BELOW (before the lines)
    const bg = theme.bg;
    if (bg !== undefined) {
        out.push(bg("customMessageBg", " ".repeat(Math.max(0, width))));
        out.push(bg("customMessageBg", pad(theme.fg("accent", `▚ ${head}`))));
        out.push(bg("customMessageBg", " ".repeat(Math.max(0, width))));
    }
    else {
        out.push("");
        out.push(theme.fg("accent", head));
    }
    // one line per agent, each separated by an empty neutral line (breathing
    // room between the colored blocks)
    const entries = [];
    for (const a of data?.answers ?? []) {
        entries.push(...verdictLines("✓", a.to, a.response.replace(/\s+/g, " ").trim().slice(0, 240), width, theme));
    }
    for (const m of data?.missing ?? []) {
        entries.push(...verdictLines("✗", m.to, `NOT ANSWERED (${m.msgId.slice(0, 18)})`, width, theme));
    }
    for (let i = 0; i < entries.length; i += 1) {
        if (i > 0 && bg !== undefined) {
            out.push(bg("customMessageBg", " ".repeat(Math.max(0, width))));
        }
        else if (i > 0) {
            out.push("");
        }
        out.push(entries[i]);
    }
    // trailing empty line so the last colored block never touches the content
    // that follows
    if (bg !== undefined) {
        out.push(bg("customMessageBg", " ".repeat(Math.max(0, width))));
    }
    else {
        out.push("");
    }
    return out;
}
