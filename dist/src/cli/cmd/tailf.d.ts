export declare const TAILF_USAGE = "usage: mesh tail -f [--limit N]";
/** Follow the ledger: print the last `limit` lines, then stream new ones
 * until SIGINT. Bodies are never present in the ledger (hash-only). */
export declare function cmdTailFollow(limitRaw: string | undefined): Promise<number>;
