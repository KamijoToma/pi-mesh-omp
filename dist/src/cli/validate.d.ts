export type Validation = {
    ok: true;
} | {
    ok: false;
    error: string;
};
export declare function validateAlias(alias: string): Validation;
export declare function validateRoom(room: string): Validation;
export declare function validateTimeoutMs(raw: string): Validation;
/** Body: 1..MAX_BODY_BYTES bytes — empty or oversized is refused loudly,
 * never silently truncated (the broker would reject the frame anyway). */
export declare function validateBody(body: string): Validation;
/** Comma-separated repo-relative refs (≤ MAX_REFS, each ≤ MAX_REF_CHARS). */
export declare function validateRefsCsv(raw: string): Validation;
/** Comma-separated reply targets (≤ MAX_REPLY_TARGETS, each a valid alias). */
export declare function validateReplyToCsv(raw: string): Validation;
/** Split a validated CSV into its trimmed parts. */
export declare function csvParts(raw: string): string[];
