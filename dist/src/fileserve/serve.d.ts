export declare const FILE_SERVE_ROUTE = "/mesh-file";
/** After the ONE completed download, the listener lingers this long so a
 *  raced second request (keep-alive pipelining, retry) gets an honest 409
 *  instead of a raw socket close — then shuts down. */
export declare const FILE_SERVE_GRACE_MS = 1000;
export interface FileServeHandle {
    /** announcement URL path (route + token) — combine with the offer host. */
    tokenPath: string;
    /** one-shot notice body for mesh_send (no secrets beyond reachability). */
    announcement: string;
    /** Structured fields — callers put these into tool-result details. */
    fields: {
        name: string;
        size: number;
        sha256: string;
        ttlMs: number;
        /** bind address as offered to peers (never 0.0.0.0 in the URL). */
        host: string;
        port: number;
        url: string;
    };
    close: () => void;
    /** resolves when the file was downloaded once, or rejects on TTL/abort. */
    onceServed: Promise<"downloaded" | "expired">;
}
export declare class FileServeError extends Error {
    readonly code: string;
    readonly name = "FileServeError";
    constructor(code: string, message: string);
}
declare function isFileServeError(error: unknown): error is FileServeError;
/** Clean public name: basename only, no traversal, control chars out. */
export declare function sanitizeServeName(path: string): string;
export interface FileServeOptions {
    /** Absolute path of the file to serve. */
    path: string;
    /** Addresses peers should use. Candidates are probed in order; the
     *  first one the OS reports as bound-reachable wins. `tailscale status`
     *  self-addresses typically come first on tailnets. */
    hostCandidates: string[];
    ttlMs?: number;
    signal?: AbortSignal;
}
/**
 * Serve ONE file for download at most once, guarded by a one-time
 * 128-bit token, until the TTL closes the listener. Streaming only:
 * the file is never buffered or spooled by this module.
 */
export declare function serveFileOnce(options: FileServeOptions): Promise<FileServeHandle>;
export { isFileServeError };
