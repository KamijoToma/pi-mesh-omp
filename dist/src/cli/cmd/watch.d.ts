export declare const WATCH_USAGE = "usage: mesh watch [room] [--alias A] [--json]";
export interface WatchOpts {
    room: string | undefined;
    alias: string | undefined;
    asJson: boolean;
}
export declare function cmdWatch(opts: WatchOpts): Promise<number>;
