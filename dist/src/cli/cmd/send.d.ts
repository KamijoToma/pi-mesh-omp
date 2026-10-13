import type { SendResult } from "../../client/client.js";
/** Single source of truth for the send usage line (dispatcher + parser). */
export declare const SEND_USAGE = "usage: mesh send [alias] <text\u2026> [--room R] [--priority P] [--reason R] [--refs A,B] [--reply-to A,B] [--broadcast] [--await | --launch] [--timeout MS] [--alias A] [--require-online]";
export interface SendArgs {
    to: string | undefined;
    text: string;
    room?: string;
    priority: string | undefined;
    reason: string | undefined;
    interrupt: boolean;
    refsCsv: string | undefined;
    replyToCsv: string | undefined;
    broadcast: boolean;
    awaitReply: boolean;
    launch: boolean;
    timeoutMs: string | undefined;
    alias: string | undefined;
    requireOnline: boolean;
}
export declare function printResult(r: SendResult): void;
/** Honest exit code for a SendResult (D5 contract). */
export declare function exitCodeFor(r: SendResult, requireOnline: boolean): number;
export declare function parseSendArgs(positionals: string[], opts: Omit<SendArgs, "to" | "text">): {
    ok: true;
    args: SendArgs;
} | {
    ok: false;
    error: string;
};
/** Read the whole stdin as the message body, capped at MAX_BODY_BYTES + 1
 * bytes so an oversized body is DETECTED (exit 2), never truncated. */
export declare function readStdinBody(): Promise<string>;
export declare function cmdSend(args: SendArgs): Promise<number>;
