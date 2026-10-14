import type { MeshHud } from "./hud.js";
import type { ExtensionAPI } from "./pi-types.js";
import { type GetRuntime, type MeshRuntime } from "./tools.js";
/**
 * Parse `/mesh join <room> [as <alias>] [observer]` args.
 * Pure, exported for tests.
 */
export declare function parseJoinArgs(args: string[]): {
    room?: string;
    asAlias?: string;
    observer: boolean;
};
/** `/mesh stale` — pure report builder: every reservation currently
 *  blocking peers, with its age vs the TTL (measured: 10 leaked claims,
 *  some blocking edits for 5+ h). Exported for tests. */
export declare function buildStaleReport(reservationsByPeer: ReadonlyMap<string, readonly {
    pattern: string;
    reason?: string;
    since?: string;
}[]>, selfAlias: string, ttlMs: number, activityOf: (alias: string) => {
    state: string;
} | undefined, now?: number): string[];
export declare function registerCommands(pi: ExtensionAPI, getRuntime: GetRuntime, onChanged?: () => void, // HUD refresh after join/switch/leave/log toggles
getHud?: () => MeshHud | null, resetActivity?: (rt: MeshRuntime) => void): void;
