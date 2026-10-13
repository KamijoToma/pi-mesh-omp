export declare function setColorOverride(value: boolean | null): void;
/** True when colors may be emitted (TTY, no NO_COLOR, not TERM=dumb). */
export declare function useColor(noColorFlag?: boolean): boolean;
export declare function say(line?: string): void;
export declare function sayErr(line?: string): void;
/** One NDJSON line for --json mode (never pretty-printed: scripts). */
export declare function jsonLine(value: unknown): void;
export declare function dim(text: string, color?: boolean): string;
export declare function bold(text: string, color?: boolean): string;
/** Stable color for an alias — same hash as the extension agentColor
 * (per-surface stability); the ANSI palette differs from the pi theme. */
export declare function colorFor(alias: string): number;
/** Colorize `@alias` with its stable color (no-op without color). */
export declare function colorizeSender(alias: string, color: boolean): string;
/** sha256 hex prefix — ids/hashes in dim output (never bodies). */
export declare function shortHash(value: string, chars?: number): string;
