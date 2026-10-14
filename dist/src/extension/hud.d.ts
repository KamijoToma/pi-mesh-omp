import type { StatusSnapshot } from "../client/client.js";
import type { SessionContext, UiTheme } from "./pi-types.js";
import type { GetRuntime } from "./tools.js";
export declare const HUD_WIDGET_ID = "mesh-hud";
/** Presence-triggered status refreshes: at most 1/s, trailing. */
export declare const HUD_STATUS_REFRESH_MIN_MS = 1000;
/**
 * the rooms the HUD must display — the SELF peer's rooms from the
 * broker snapshot (authoritative), falling back to the local joinedRooms
 * before the first snapshot arrives. The snapshot's top-level `rooms` field
 * is the broker-WIDE list and must never be shown as "my rooms".
 */
export declare function selfRooms(snapshot: StatusSnapshot | null, selfAlias: string, fallback: readonly string[]): string[];
/** Count other peers sharing a room with this session, not the whole broker. */
export declare function visiblePeerCount(snapshot: StatusSnapshot | null, selfAlias: string, selfRoomList: readonly string[]): number;
/** Plain-data input; without a theme the renderer returns plain text. */
export interface HudState {
    connected: boolean;
    connecting: boolean;
    alias: string;
    rooms: string[];
    peerCount: number;
    pending: number;
    inbox: number;
    transcriptOn: boolean;
    ledgerFailures: number;
    transcriptFailures: number;
    injectionFailures: number;
}
/** Important state comes first so right-side truncation loses details, not alerts. */
export declare function renderHudLines(state: HudState, theme?: UiTheme): [string];
/** Owns the status snapshot and renders a real, non-wrapping TUI component. */
export declare class MeshHud {
    private readonly deps;
    private ctx;
    private snapshot;
    private connecting;
    private statusTimer;
    private lastStatusFetchAt;
    constructor(deps: {
        getRuntime: GetRuntime;
    });
    attach(ctx: SessionContext): void;
    /** session_shutdown: remove the row and cancel pending snapshot refreshes. */
    detach(): void;
    setConnecting(on: boolean): void;
    /** 'closed': mark offline + clear peers (client auto-reconnects → ◐). */
    onClosed(): void;
    /** Presence-driven status refresh: at most 1/s, trailing. */
    scheduleStatusRefresh(): void;
    /** Local change (join/leave/log toggle): repaint + resync snapshot. */
    onLocalChange(): void;
    /** Fire-and-forget status → snapshot. NEVER blocks session lifecycle. */
    fetchStatus(): void;
    refresh(): void;
    private buildState;
}
