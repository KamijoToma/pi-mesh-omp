export declare const LEDGER_SCHEMA = "mesh.ledger.v1";
export type LedgerEventName = "sent" | "delivered" | "queued_offline" | "reply" | "expired" | "interrupted" | "blocked" | "error" | "inbound" | "reserved" | "released" | "dropped_offline";
export interface LedgerRecord {
    schema: typeof LEDGER_SCHEMA;
    event: LedgerEventName;
    id?: string;
    from?: string;
    to?: string;
    room?: string;
    priority?: string;
    bodyHash?: string;
    reasonHash?: string;
    refs?: string[];
    code?: string;
    ts: string;
    bodyStored: false;
}
/** Fields the caller supplies; schema/ts/bodyStored are stamped by the ledger. */
export type LedgerInput = Omit<LedgerRecord, "schema" | "ts" | "bodyStored">;
export declare class MeshLedger {
    readonly stateDir: string;
    private readonly maxBytes;
    constructor(stateDir: string, maxBytes?: number);
    get path(): string;
    /**
    * Append one record. Fail-closed: recursive scan of forbidden persisted
    * keys BEFORE writing — a violation throws and zero bytes are appended.
    */
    append(input: LedgerInput): LedgerRecord;
    /**
    * read the last `limit` records, optionally filtered (hash-only — the
    * ledger never contains bodies,. For agents: durable mesh history.
    */
    read(limit?: number, filter?: {
        from?: string;
        to?: string;
        room?: string;
        event?: string;
    }): LedgerRecord[];
    /** Simple rotation: when ledger.jsonl exceeds maxBytes → ledger-<date>.jsonl.1. */
    private rotateIfNeeded;
}
