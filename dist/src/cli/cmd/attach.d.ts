export declare const ATTACH_USAGE = "usage: mesh attach [alias] [--session ID] [--room R] [--json] [--no-read]";
export interface AttachOpts {
    alias: string | undefined;
    sessionId: string | undefined;
    room: string | undefined;
    asJson: boolean;
    noRead: boolean;
}
export declare function cmdAttach(opts: AttachOpts): Promise<number>;
