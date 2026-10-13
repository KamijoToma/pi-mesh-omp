import { type StatusSnapshot } from "../client/client.js";
import type { MeshFrame } from "../protocol/envelope.js";
import type { SessionContext } from "./pi-types.js";
import type { GetRuntime } from "./tools.js";
export declare const HUD_WIDGET_ID = "mesh-hud";
export declare const HUD_STATUS_ID = "mesh";
export declare const HUD_ACTIVITY_WINDOW_MS = 90000;
export declare const HUD_PEER_MAX = 5;
export declare const HUD_PREVIEW_MAX = 60;
/** Presence-triggered status refreshes: at most 1/s, trailing. */
export declare const HUD_STATUS_REFRESH_MIN_MS = 1000;
/**
 * the rooms the HUD must display — the SELF peer's rooms from the
 * broker snapshot (authoritative), falling back to the local joinedRooms
 * before the first snapshot arrives. The snapshot's top-level `rooms` field
 * is the broker-WIDE list and must never be shown as "my rooms".
 */
export declare function selfRooms(snapshot: StatusSnapshot | null, selfAlias: string, fallback: readonly string[]): string[];
/**
 * peers VISIBLE from this session — those sharing at least one room
 * with the self (the mesh messaging rule). The raw snapshot lists every
 * peer of the broker (all rooms): a session alone in "voice" must not see
 * the cs-room agents as its peers.
 */
export declare function visiblePeers(snapshot: StatusSnapshot | null, selfAlias: string, selfRoomList: readonly string[]): string[];
export type HudActivity = {
    kind: "inbound";
    from: string;
    room: string;
    preview: string;
    ts: number;
} | {
    kind: "expired";
    msgId: string;
    ts: number;
};
/** Plain-data input to the pure renderer (inject `now` for tests). */
export interface HudState {
    connected: boolean;
    connecting: boolean;
    alias: string;
    rooms: string[];
    peers: string[];
    pending: number;
    transcriptOn: boolean;
    ledgerFailures: number;
    transcriptFailures: number;
    injectionFailures: number;
    activity?: HudActivity;
    now: number;
}
/**
 * PURE renderer — no ANSI, no Pi API. 1 line normally, 2 when a recent
 * activity (< 90 s) exists. L1: connection dot (● online / ◐ connecting /
 * ○ offline), alias, rooms, peers (max 5 + overflow), pend (omit at 0),
 * tx on/off, failure counters (only when > 0).
 */
export declare function renderHudLines(state: HudState): [string, ...string[]];
/** Compact footer status text. */
export declare function hudStatusText(state: HudState): string;
/**
 * MeshHud — owns HUD state (last status snapshot, last activity) and pushes
 * it to the TUI. refresh eagerly computes the colorized string[] (via
 * ctx.ui.theme, falling back to plain lines when unavailable) and calls
 * setWidget with the SAFE array form only — NEVER a factory: pi uses a
 * factory's return value directly as a component, so returning string[]
 * crashes the TUI (child.render is not a function).
 */
export declare class MeshHud {
    private readonly deps;
    private ctx;
    private snapshot;
    private activity;
    private connecting;
    private statusTimer;
    private lastStatusFetchAt;
    constructor(deps: {
        getRuntime: GetRuntime;
    });
    attach(ctx: SessionContext): void;
    /** session_shutdown: clear BOTH widget and status. */
    detach(): void;
    setConnecting(on: boolean): void;
    /** 'closed': mark offline + clear peers (client auto-reconnects → ◐). */
    onClosed(): void;
    /** Inbound frame: L2 activity preview — transient memory only, never persisted. */
    noteInbound(frame: MeshFrame): void;
    noteExpired(msgId: string): void;
    /** Presence-driven status refresh: at most 1/s, trailing. */
    scheduleStatusRefresh(): void;
    /** Local change (join/leave/log toggle): repaint + resync snapshot. */
    onLocalChange(): void;
    /** Fire-and-forget status → snapshot. NEVER blocks session lifecycle. */
    fetchStatus(): void;
    refresh(): void;
    /** Eager colorization at refresh time; plain lines when no theme (headless).
    *  known peer aliases are recolored with their per-agent color. */
    private colorize;
    private buildState;
}
/**
 * recolor exact alias tokens in a line — "agent-1" must NOT recolor
 * inside "agent-10" (word boundaries = the alias charset). Pure, exported
 * for tests.
 */
export declare function colorizePeerAliases(line: string, peers: {
    alias: string;
}[], fg: (color: string, text: string) => string): string;
