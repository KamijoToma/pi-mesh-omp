import { type TailnetCommand } from "../../tailscale/commands.js";
export declare const TAILSCALE_USAGE = "usage: mesh tailscale status|discover|doctor [--json]\n       mesh tailscale join <mesh-id> [--service svc:NAME | --host HOST | --tag tag:NAME] [--port PORT] [--json]";
export type ParsedTailnetArgs = {
    ok: true;
    help: boolean;
    json: boolean;
    command: TailnetCommand;
} | {
    ok: false;
    error: string;
};
export declare function parseTailnetArgs(argv: string[]): ParsedTailnetArgs;
export declare function cmdTailscale(argv: string[]): Promise<number>;
