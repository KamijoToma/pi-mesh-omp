export declare const RESERVE_USAGE = "usage: mesh reserve <path>... [--reason R] [--hold MS]";
export declare const RELEASE_USAGE = "usage: mesh release [<pattern>...] [--all]";
export interface ReserveArgs {
    paths: string[];
    reason: string | undefined;
    holdMs: number | undefined;
}
export declare function cmdReserve(args: ReserveArgs): Promise<number>;
/** One-shot release: honest no-op — a fresh process holds NO claims (they
 * die with their connection). Real releases happen in the SAME process
 * (reserve --hold → Ctrl-C) or a persistent peer (attach, Phase 5). */
export declare function cmdRelease(patterns: string[], all: boolean): Promise<number>;
