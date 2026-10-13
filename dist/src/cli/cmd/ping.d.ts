export declare const PING_USAGE = "usage: mesh ping <alias> [--timeout MS]";
export declare function cmdPing(alias: string, timeoutRaw: string | undefined): Promise<number>;
