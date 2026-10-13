export declare const REPLY_USAGE = "usage: mesh reply <msgId> <text\u2026> (--to <alias> | --reply-all) --room <room> [--refs A,B]";
export interface ReplyArgs {
    msgId: string;
    text: string;
    to: string | undefined;
    room: string | undefined;
    refsCsv: string | undefined;
    replyAll: boolean;
}
export declare function cmdReply(args: ReplyArgs): Promise<number>;
