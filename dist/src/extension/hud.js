// extension/hud.ts — mesh HUD: a live footer-widget ABOVE the Pi editor.
// renderHudLines is a PURE function (no ANSI, no I/O) → fully unit-testable
// without a TUI. MeshHud owns state + ctx.ui.setWidget/setStatus wiring.
// Body previews are transient memory only — never persisted.
import { computePeerStatus } from "../client/client.js";
import { agentColor } from "./colors.js";
export const HUD_WIDGET_ID = "mesh-hud";
export const HUD_STATUS_ID = "mesh";
export const HUD_ACTIVITY_WINDOW_MS = 90_000;
export const HUD_PEER_MAX = 5;
export const HUD_PREVIEW_MAX = 60;
/** Presence-triggered status refreshes: at most 1/s, trailing. */
export const HUD_STATUS_REFRESH_MIN_MS = 1_000;
/**
 * the rooms the HUD must display — the SELF peer's rooms from the
 * broker snapshot (authoritative), falling back to the local joinedRooms
 * before the first snapshot arrives. The snapshot's top-level `rooms` field
 * is the broker-WIDE list and must never be shown as "my rooms".
 */
export function selfRooms(snapshot, selfAlias, fallback) {
    const peer = snapshot?.peers.find((p) => p.alias === selfAlias);
    if (peer !== undefined)
        return peer.rooms;
    return [...fallback];
}
/**
 * peers VISIBLE from this session — those sharing at least one room
 * with the self (the mesh messaging rule). The raw snapshot lists every
 * peer of the broker (all rooms): a session alone in "voice" must not see
 * the cs-room agents as its peers.
 */
export function visiblePeers(snapshot, selfAlias, selfRoomList) {
    if (snapshot === null)
        return [];
    const mine = new Set(selfRoomList);
    // no rooms → no visibility at all (the session cannot talk to anyone)
    if (mine.size === 0)
        return [];
    return snapshot.peers
        .filter((p) => p.alias !== selfAlias && p.rooms.some((r) => mine.has(r)))
        .map((p) => p.alias);
}
function oneLine(s, max) {
    const flat = s.replace(/\s+/g, " ").trim();
    return flat.length > max ? `${flat.slice(0, max - 1)}…` : flat;
}
function renderActivity(a) {
    if (a.kind === "expired")
        return `✗ expired ${a.msgId}`;
    const time = new Date(a.ts).toTimeString().slice(0, 8); // HH:MM:SS local
    const room = a.room !== "" ? ` [${a.room}]` : "";
    return `↩ @${a.from}${room}: ${oneLine(a.preview, HUD_PREVIEW_MAX)} ${time}`;
}
/**
 * PURE renderer — no ANSI, no Pi API. 1 line normally, 2 when a recent
 * activity (< 90 s) exists. L1: connection dot (● online / ◐ connecting /
 * ○ offline), alias, rooms, peers (max 5 + overflow), pend (omit at 0),
 * tx on/off, failure counters (only when > 0).
 */
export function renderHudLines(state) {
    const dot = state.connected ? "●" : state.connecting ? "◐" : "○";
    let head = `mesh ${dot} ${state.alias}`;
    if (state.rooms.length > 0)
        head += ` @${state.rooms.join(",")}`;
    const segs = [head];
    if (state.peers.length > 0) {
        const shown = state.peers.slice(0, HUD_PEER_MAX);
        const extra = state.peers.length - shown.length;
        segs.push(`peers: ${shown.join(",")}${extra > 0 ? `(+${extra})` : ""}`);
    }
    if (state.pending > 0)
        segs.push(`pend:${state.pending}`);
    segs.push(`tx:${state.transcriptOn ? "on" : "off"}`);
    if (state.ledgerFailures > 0)
        segs.push(`!ledger:${state.ledgerFailures}`);
    if (state.transcriptFailures > 0)
        segs.push(`!transcript:${state.transcriptFailures}`);
    if (state.injectionFailures > 0)
        segs.push(`!inject:${state.injectionFailures}`);
    const lines = [segs.join(" · ")];
    const a = state.activity;
    if (a !== undefined && state.now - a.ts < HUD_ACTIVITY_WINDOW_MS) {
        lines.push(renderActivity(a));
    }
    return lines;
}
/** Compact footer status text. */
export function hudStatusText(state) {
    if (state.connected)
        return `mesh:${state.peers.length}`;
    if (state.connecting)
        return "mesh:…";
    return "mesh:off";
}
/**
 * MeshHud — owns HUD state (last status snapshot, last activity) and pushes
 * it to the TUI. refresh eagerly computes the colorized string[] (via
 * ctx.ui.theme, falling back to plain lines when unavailable) and calls
 * setWidget with the SAFE array form only — NEVER a factory: pi uses a
 * factory's return value directly as a component, so returning string[]
 * crashes the TUI (child.render is not a function).
 */
export class MeshHud {
    deps;
    ctx = null;
    snapshot = null;
    activity = null;
    connecting = false;
    statusTimer = null;
    lastStatusFetchAt = 0;
    constructor(deps) {
        this.deps = deps;
    }
    attach(ctx) {
        this.ctx = ctx;
        this.refresh();
    }
    /** session_shutdown: clear BOTH widget and status. */
    detach() {
        const ctx = this.ctx;
        this.ctx = null;
        if (this.statusTimer !== null) {
            clearTimeout(this.statusTimer);
            this.statusTimer = null;
        }
        if (ctx !== null) {
            ctx.ui.setWidget(HUD_WIDGET_ID, undefined);
            ctx.ui.setStatus(HUD_STATUS_ID, undefined);
        }
    }
    setConnecting(on) {
        this.connecting = on;
        this.refresh();
    }
    /** 'closed': mark offline + clear peers (client auto-reconnects → ◐). */
    onClosed() {
        this.snapshot = null;
        this.connecting = true;
        this.refresh();
    }
    /** Inbound frame: L2 activity preview — transient memory only, never persisted. */
    noteInbound(frame) {
        this.activity = {
            kind: "inbound",
            from: frame.from ?? "?",
            room: frame.room ?? "",
            preview: frame.body ?? "",
            ts: Date.now(),
        };
        this.refresh();
    }
    noteExpired(msgId) {
        this.activity = { kind: "expired", msgId, ts: Date.now() };
        this.refresh();
    }
    /** Presence-driven status refresh: at most 1/s, trailing. */
    scheduleStatusRefresh() {
        if (this.statusTimer !== null)
            return; // trailing refresh already queued
        const wait = Math.max(0, HUD_STATUS_REFRESH_MIN_MS - (Date.now() - this.lastStatusFetchAt));
        this.statusTimer = setTimeout(() => {
            this.statusTimer = null;
            this.fetchStatus();
        }, wait);
        this.statusTimer.unref();
    }
    /** Local change (join/leave/log toggle): repaint + resync snapshot. */
    onLocalChange() {
        this.refresh();
        this.scheduleStatusRefresh();
    }
    /** Fire-and-forget status → snapshot. NEVER blocks session lifecycle. */
    fetchStatus() {
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
            .catch(() => { });
    }
    refresh() {
        const ctx = this.ctx;
        if (ctx === null)
            return;
        const state = this.buildState();
        ctx.ui.setWidget(HUD_WIDGET_ID, this.colorize(renderHudLines(state), state));
        ctx.ui.setStatus(HUD_STATUS_ID, hudStatusText(state));
    }
    /** Eager colorization at refresh time; plain lines when no theme (headless).
    *  known peer aliases are recolored with their per-agent color. */
    colorize(lines, state) {
        const theme = this.ctx?.ui.theme;
        if (theme === undefined)
            return lines;
        const color = state.connected ? "success" : state.connecting ? "warning" : "muted";
        const peers = this.snapshot?.peers ?? [];
        return lines.map((line, i) => {
            const l = colorizePeerAliases(line, peers, (c, t) => theme.fg(c, t));
            return i === 0 ? theme.fg(color, l) : theme.fg("muted", l);
        });
    }
    buildState() {
        const rt = this.deps.getRuntime();
        const self = rt?.client.alias ?? "";
        const rooms = selfRooms(this.snapshot, self, rt?.client.rooms ?? []);
        return {
            connected: rt?.client.isOnline() === true,
            connecting: this.connecting,
            alias: self !== "" ? self : "?",
            // the status snapshot's `rooms` is the broker-wide room list, NOT
            // this session's rooms — the HUD must show the SELF peer's rooms (the
            // broker's truth), falling back to the local joinedRooms before the
            // first snapshot arrives. (Bug: a new session in room "voice" showed
            // "@cs-room,voice" because it listed every room of the mesh.)
            rooms,
            // only peers sharing a room with this session are visible.
            // Phase 3: announced turn state first (● working / ○ idle), stuck
            // (idle long with reservations) wins over idle, heuristic fallback.
            peers: visiblePeers(this.snapshot, self, rooms).map((alias) => {
                const p = this.snapshot?.peers.find((x) => x.alias === alias);
                if (p === undefined)
                    return alias;
                // D40: ⌁ + last IP octet marks peers running on ANOTHER machine
                // (full origin in mesh_status / /mesh status).
                const remote = p.via !== undefined ? `⌁${p.via.split(":").pop() ?? "?"}` : "";
                const act = computePeerStatus(p.lastSeenAt, (p.reservations?.length ?? 0) > 0, rt?.client.activityIdleMs ?? 0, rt?.client.activityStuckMs ?? 0);
                if (act.status === "stuck")
                    return `${alias}✕${remote}`;
                if (p.activity !== undefined) {
                    return p.activity.state === "busy" ? `${alias}●${remote}` : `${alias}○${remote}`;
                }
                return act.status === "idle" ? `${alias}○${remote}` : `${alias}${remote}`;
            }),
            pending: rt?.client.pendingCount ?? 0,
            transcriptOn: rt?.transcript.isEnabled() === true,
            ledgerFailures: rt?.ledgerFailures ?? 0,
            transcriptFailures: rt?.transcriptFailures ?? 0,
            injectionFailures: rt?.injectionFailures ?? 0,
            activity: this.activity ?? undefined,
            now: Date.now(),
        };
    }
}
/** Escape an alias for use inside a RegExp. */
function escapeRe(s) {
    return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
/**
 * recolor exact alias tokens in a line — "agent-1" must NOT recolor
 * inside "agent-10" (word boundaries = the alias charset). Pure, exported
 * for tests.
 */
export function colorizePeerAliases(line, peers, fg) {
    let l = line;
    for (const p of peers) {
        const re = new RegExp(`(?<![a-z0-9-])${escapeRe(p.alias)}(?![a-z0-9-])`, "g");
        l = l.replace(re, (m) => fg(agentColor(p.alias), m));
    }
    return l;
}
