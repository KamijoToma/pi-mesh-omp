export type ArgKind = "flag" | "value" | "repeat";
export interface ArgSpec {
    /** long name without leading dashes (e.g. "room") */
    name: string;
    kind: ArgKind;
    /** short single-letter alias without the dash (e.g. "R") */
    short?: string;
    /** one-line help shown by `mesh help <cmd>` */
    help?: string;
    /** placeholder for the value in usage lines (e.g. "MS") */
    meta?: string;
}
export interface ParsedArgs {
    /** names of flags present */
    flags: Set<string>;
    /** option name → value (last wins) */
    values: Map<string, string>;
    /** option name → all values (repeat kind) */
    repeats: Map<string, string[]>;
    /** positional operands, in order */
    positionals: string[];
}
export type ParseResult = {
    ok: true;
    parsed: ParsedArgs;
    help: boolean;
} | {
    ok: false;
    error: string;
};
/** Parse argv against specs. Never throws — errors are honest strings. */
export declare function parseArgs(argv: string[], specs: ArgSpec[]): ParseResult;
/** Render the usage fragment for a spec list: [--room R] [--await] ... */
export declare function specUsage(specs: ArgSpec[]): string;
