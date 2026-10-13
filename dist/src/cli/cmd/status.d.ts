export declare const STATUS_USAGE = "usage: mesh status [room] [--all] [--reservations] [--json]  (alias: peers)";
export interface StatusOpts {
    room: string | undefined;
    all: boolean;
    reservations: boolean;
    json: boolean;
}
export declare function cmdStatus(opts: StatusOpts): Promise<number>;
