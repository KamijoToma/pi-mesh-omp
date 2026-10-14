// extension/hud.ts — one compact mesh status row above the editor.
import { truncateToWidth } from "@mariozechner/pi-tui";
import type { StatusSnapshot } from "../client/client.js";
import { DEFAULT_ROOM } from "../shared/config.js";
import { agentColor } from "./colors.js";
import type { SessionContext, UiTheme } from "./pi-types.js";
import type { GetRuntime } from "./tools.js";

export const HUD_WIDGET_ID = "mesh-hud";
/** Presence-triggered status refreshes: at most 1/s, trailing. */
export const HUD_STATUS_REFRESH_MIN_MS = 1_000;

/**
 * the rooms the HUD must display — the SELF peer's rooms from the
 * broker snapshot (authoritative), falling back to the local joinedRooms
 * before the first snapshot arrives. The snapshot's top-level `rooms` field
 * is the broker-WIDE list and must never be shown as "my rooms".
 */
export function selfRooms(
  snapshot: StatusSnapshot | null,
  selfAlias: string,
  fallback: readonly string[],
): string[] {
  const peer = snapshot?.peers.find((p) => p.alias === selfAlias);
  if (peer !== undefined) return peer.rooms;
  return [...fallback];
}

/** Count other peers sharing a room with this session, not the whole broker. */
export function visiblePeerCount(
  snapshot: StatusSnapshot | null,
  selfAlias: string,
  selfRoomList: readonly string[],
): number {
  let count = 0;
  for (const peer of snapshot?.peers ?? []) {
    if (peer.alias === selfAlias) continue;
    for (const room of peer.rooms) {
      if (selfRoomList.includes(room)) {
        count += 1;
        break;
      }
    }
  }
  return count;
}

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
export function renderHudLines(state: HudState, theme?: UiTheme): [string] {
  const color = state.connected ? "success" : state.connecting ? "warning" : "muted";
  const dot = state.connected ? "●" : state.connecting ? "◐" : "○";
  let line = `mesh ${theme?.fg(color, dot) ?? dot}`;
  const failures = state.ledgerFailures + state.transcriptFailures + state.injectionFailures;
  if (failures > 0) {
    const alert = `!err:${failures}`;
    line += ` ${theme?.fg("error", alert) ?? alert}`;
  }
  line += state.connected ? ` peers:${state.peerCount}` : state.connecting ? " connecting" : " offline";
  if (state.pending > 0) line += ` · wait:${state.pending}`;
  if (state.inbox > 0) line += ` · inbox:${state.inbox}`;
  line += ` · ${theme?.fg(agentColor(state.alias), state.alias) ?? state.alias}`;
  if (state.rooms.length === 0) {
    line += " · no-room";
  } else if (state.rooms.length !== 1 || state.rooms[0] !== DEFAULT_ROOM) {
    const room = state.rooms.find((r) => r !== DEFAULT_ROOM) ?? state.rooms[0];
    line += ` · @${room}${state.rooms.length > 1 ? `+${state.rooms.length - 1}` : ""}`;
  }
  if (state.transcriptOn) line += " · log:on";
  return [line];
}

/** Owns the status snapshot and renders a real, non-wrapping TUI component. */
export class MeshHud {
  private ctx: SessionContext | null = null;
  private snapshot: StatusSnapshot | null = null;
  private connecting = false;
  private statusTimer: NodeJS.Timeout | null = null;
  private lastStatusFetchAt = 0;

  constructor(private readonly deps: { getRuntime: GetRuntime }) {}

  attach(ctx: SessionContext): void {
    this.ctx = ctx;
    // Hot reload may leave the old footer entries behind.
    ctx.ui.setStatus("mesh", undefined);
    ctx.ui.setStatus("mesh-inbox", undefined);
    this.refresh();
  }

  /** session_shutdown: remove the row and cancel pending snapshot refreshes. */
  detach(): void {
    const ctx = this.ctx;
    this.ctx = null;
    if (this.statusTimer !== null) {
      clearTimeout(this.statusTimer);
      this.statusTimer = null;
    }
    if (ctx !== null) {
      ctx.ui.setWidget(HUD_WIDGET_ID, undefined);
    }
  }

  setConnecting(on: boolean): void {
    this.connecting = on;
    this.refresh();
  }

  /** 'closed': mark offline + clear peers (client auto-reconnects → ◐). */
  onClosed(): void {
    this.snapshot = null;
    this.connecting = true;
    this.refresh();
  }

  /** Presence-driven status refresh: at most 1/s, trailing. */
  scheduleStatusRefresh(): void {
    if (this.statusTimer !== null) return; // trailing refresh already queued
    const wait = Math.max(0, HUD_STATUS_REFRESH_MIN_MS - (Date.now() - this.lastStatusFetchAt));
    this.statusTimer = setTimeout(() => {
      this.statusTimer = null;
      this.fetchStatus();
    }, wait);
    this.statusTimer.unref();
  }

  /** Local change (join/leave/log toggle): repaint + resync snapshot. */
  onLocalChange(): void {
    this.refresh();
    this.scheduleStatusRefresh();
  }

  /** Fire-and-forget status → snapshot. NEVER blocks session lifecycle. */
  fetchStatus(): void {
    const rt = this.deps.getRuntime();
    if (rt === null || !rt.client.isOnline()) {
      this.refresh();
      return;
    }
    this.lastStatusFetchAt = Date.now();
    rt.client
      .status()
      .then((snap) => {
        this.snapshot = snap;
        this.refresh();
      })
      .catch(() => {});
  }

  refresh(): void {
    const ctx = this.ctx;
    if (ctx === null) return;
    const state = this.buildState();
    const lines = renderHudLines(state, ctx.ui.theme);
    if (ctx.mode !== undefined && ctx.mode !== "tui") {
      ctx.ui.setWidget(HUD_WIDGET_ID, lines);
      return;
    }
    const line = ` ${lines[0]}`;
    ctx.ui.setWidget(HUD_WIDGET_ID, () => ({
      render: (width: number) => [truncateToWidth(line, width)],
      invalidate: () => {},
    }));
  }

  private buildState(): HudState {
    const rt = this.deps.getRuntime();
    const self = rt?.client.alias ?? "";
    const rooms = selfRooms(this.snapshot, self, rt?.client.rooms ?? []);
    return {
      connected: rt?.client.isOnline() === true,
      connecting: this.connecting,
      alias: self !== "" ? self : "?",
      rooms,
      peerCount: visiblePeerCount(this.snapshot, self, rooms),
      pending: rt?.client.pendingCount ?? 0,
      inbox: rt?.deferredInbox?.count ?? 0,
      transcriptOn: rt?.transcript.isEnabled() === true,
      ledgerFailures: rt?.ledgerFailures ?? 0,
      transcriptFailures: rt?.transcriptFailures ?? 0,
      injectionFailures: rt?.injectionFailures ?? 0,
    };
  }
}

