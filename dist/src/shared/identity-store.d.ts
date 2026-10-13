import { type FileReservation } from "../protocol/envelope.js";
export declare const IDENTITY_FILE_NAME = "identity.json";
export declare const IDENTITY_PREFIX = "identity-";
export declare const IDENTITY_PENDING_NAME = "identity-pending.json";
export declare const IDENTITY_VERSION = 1;
/** Persisted reservations older than this are NOT re-declared at hello. */
export declare const RESERVATION_TTL_MS = 86400000;
/** a /mesh new handoff is only valid for this long. */
export declare const PENDING_TTL_MS = 900000;
export interface PersistedIdentity {
    version: typeof IDENTITY_VERSION;
    sessionId: string;
    alias: string;
    rooms: string[];
    reservations: FileReservation[];
    updatedAt: string;
}
/** Snapshot the current client state into a persistable identity. */
export declare function identityFromClient(sessionId: string, client: {
    alias: string;
    rooms: readonly string[];
    reservations: readonly FileReservation[];
}): PersistedIdentity;
export declare class MeshIdentity {
    private readonly stateDir;
    constructor(stateDir: string);
    /** Per-session file: <stateDir>/identity-<sessionId>.json */
    private pathFor;
    private get legacyPath();
    private parse;
    /**
    * Load the identity belonging to `sessionId`. Returns null when absent,
    * from another session, malformed, or on an older version — callers then
    * start fresh (random alias, default rooms, no reservations).
    */
    load(sessionId: string): PersistedIdentity | null;
    /**
    * factory-reset this session's identity — /mesh reset behaves like
    * /new (fresh alias, default rooms, no reservations) while staying in the
    * same pi session. Best effort — never throws.
    */
    reset(sessionId: string): void;
    private get pendingPath();
    /**
    * Stage the current identity for the NEXT session (created by /mesh new)
    * alias + rooms + reservations (and optionally a compact history) travel
    * through identity-pending.json, consumed by the next session_start.
    */
    savePending(identity: PersistedIdentity, history?: string[]): void;
    /**
    * Take the staged identity (if fresh enough) and clear the file.
    * Returns { identity, history } or null. The history is the compact
    * transferred conversation --history flag).
    */
    consumePending(now?: number): {
        identity: PersistedIdentity;
        history?: string[];
    } | null;
    /** Persist atomically (tmp + rename). Best effort — never throws. */
    save(identity: PersistedIdentity): void;
    /** List every persisted identity in this stateDir (sessions command /
     *  adoption targets). Corrupt or unusable files are SKIPPED and REPORTED
     *  in `skipped` — a store layer does not print; the caller decides how to
     *  surface the loss. READ-ONLY: nothing is written, moved or deleted. */
    list(): {
        identities: ListedIdentity[];
        skipped: string[];
    };
}
export interface ListedIdentity extends PersistedIdentity {
    sessionId: string;
}
/** Path of the per-session identity file (exported for tests). */
export declare function identityPath(stateDir: string, sessionId: string): string;
export declare function identityFileExists(stateDir: string, sessionId: string): boolean;
