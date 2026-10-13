export interface LedgerFilters {
    limit: number;
    from?: string;
    to?: string;
    room?: string;
    event?: string;
}
/** mesh tail: last N lines of the ledger (no follow — that is `tail -f`). */
export declare function cmdTail(limitRaw: string | undefined): Promise<number>;
/** mesh ledger: filtered history over current + rotated files. */
export declare function cmdLedger(filters: LedgerFilters, asJson: boolean): Promise<number>;
export declare const LEDGER_USAGE = "usage: mesh ledger [--limit N] [--from A] [--to A] [--room R] [--event E] [--json]";
