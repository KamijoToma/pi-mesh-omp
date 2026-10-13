// extension/commands.ts — /mesh command. All output via ctx.ui.notify.
import * as fs from "node:fs";
import { MeshClient } from "../client/client.js";
import { loadConfig } from "../shared/config.js";
import { brokerLockPath, brokerSocketPath } from "../shared/paths.js";
import { MESH_VERSION } from "../shared/version.js";
import { attachClientListeners, updateSessionName } from "./attach.js";
import { identityFromClient } from "../shared/identity-store.js";
import { isOmpHost, reportConnectionError } from "./host.js";
const HELP_TEXT = [
    "/mesh status [room]   — broker snapshot (online peers, rooms)",
    "/mesh join <room> [as <alias>] [observer]",
    "/mesh leave <room>",
    "/mesh alias [<new-alias>] — show, or change this session's alias live",
    "/mesh new [--history]  — fresh pi session like /new, mesh identity (alias, rooms,",
    "                         reservations) handed over; --history carries the last frames",
    "/mesh reset            — factory-reset the mesh identity (like /new: fresh alias,",
    "                         default rooms, no reservations) WITHOUT leaving this session",
    "/mesh log [on|off]    — opt-in transcript (redacted bodies)",
    "/mesh ping <alias>    — send a one-shot ping message",
    "/mesh stale           — reservations held by peers, with age (TTL insight)",
    "/mesh broker          — socket path, lock pid, session state",
    "/mesh inbox [flush]   — list deferred broadcasts, or deliver them now",
    "/mesh help",
].join("\n");
function notify(ctx, message) {
    ctx.ui.notify(message, "info");
}
/** persist alias/rooms after a mutation (join/leave/rename). */
function persistIdentity(rt) {
    try {
        rt.identity.save(identityFromClient(rt.sessionId, rt.client));
    }
    catch {
        // best effort
    }
}
/**
 * Parse `/mesh join <room> [as <alias>] [observer]` args.
 * Pure, exported for tests.
 */
export function parseJoinArgs(args) {
    const rest = args.filter((a) => a !== "observer");
    const observer = args.includes("observer");
    const asIdx = rest.indexOf("as");
    if (asIdx === -1)
        return { room: rest[0], observer };
    const asAlias = rest[asIdx + 1];
    if (asIdx === 0)
        return { asAlias, observer };
    return { room: rest[0], asAlias, observer };
}
function pidAlive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    }
    catch {
        return false;
    }
}
async function cmdStatus(rt, ctx, pi, room) {
    if (!rt.client.isOnline()) {
        try {
            await rt.client.connect();
        }
        catch (error) {
            reportConnectionError(pi, ctx, error);
            return;
        }
    }
    const snap = await rt.client.status(room);
    if (snap.peers.length === 0) {
        notify(ctx, `mesh: no online peers${room !== undefined ? ` in room ${room}` : ""}`);
        return;
    }
    const lines = snap.peers.map((p) => `@${p.alias} rooms=${p.rooms.join(",")}${p.since !== undefined ? ` since=${p.since}` : ""}${p.via !== undefined ? ` via=${p.via} ⟵ other machine` : ""}`);
    notify(ctx, `mesh peers (${snap.peers.length}):\n${lines.join("\n")}\nrooms: ${snap.rooms.join(", ") || "(none)"}`);
}
async function cmdJoin(rt, ctx, room, observer, asAlias, pi) {
    // `/mesh join <room> as <alias>`: rename first (re-hello), then join.
    // A rename failure NEVER aborts the join — the session keeps its current
    // alias and still joins the room (same_alias is a no-op success).
    if (asAlias !== undefined) {
        const renamed = await rt.client.rename(asAlias);
        if (renamed.ok) {
            if (renamed.unchanged !== true) {
                notify(ctx, `mesh: alias changed @${renamed.alias}`);
                persistIdentity(rt);
            }
        }
        else {
            notify(ctx, `mesh: rename to "${asAlias}" failed: ${renamed.reason} — joining as @${rt.client.alias}`);
        }
    }
    if (room === undefined) {
        notify(ctx, "usage: /mesh join <room> [as <alias>] [observer]");
        return;
    }
    const role = observer ? "observer" : "member";
    try {
        await rt.client.join(room, role);
        notify(ctx, `mesh: joined ${room} as ${role}`);
        persistIdentity(rt);
        updateSessionName(pi, rt);
    }
    catch (err) {
        notify(ctx, `mesh: join failed: ${err instanceof Error ? err.message : String(err)}`);
    }
}
async function cmdLeave(rt, ctx, room, pi) {
    if (room === undefined) {
        notify(ctx, "usage: /mesh leave <room>");
        return;
    }
    try {
        await rt.client.leave(room);
        notify(ctx, `mesh: left ${room}`);
        persistIdentity(rt);
        updateSessionName(pi, rt);
    }
    catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg === "not_member") {
            // Honest + friendly: the broker does not know us in this room. The
            // local joinedRooms set is resynced by client.leave so a later
            // reconnect does not try to rejoin it.
            notify(ctx, `mesh: not in room "${room}" — nothing to leave`);
        }
        else {
            notify(ctx, `mesh: leave failed: ${msg}`);
        }
    }
}
async function cmdAlias(rt, ctx, alias, pi) {
    if (alias === undefined) {
        notify(ctx, `mesh alias: @${rt.client.alias}`);
        return;
    }
    const renamed = await rt.client.rename(alias);
    if (renamed.ok) {
        if (renamed.unchanged !== true) {
            notify(ctx, `mesh: alias changed @${renamed.alias}`);
            persistIdentity(rt);
            updateSessionName(pi, rt);
        }
        else {
            notify(ctx, `mesh alias: @${rt.client.alias}`);
        }
    }
    else {
        let hint = "";
        if (renamed.reason === "alias_taken") {
            // tell the user WHO holds the alias — a live peer keeps it until it
            // disconnects, so a rename can never steal it.
            try {
                const snap = await rt.client.status();
                const holder = snap.peers.find((p) => p.alias === alias);
                if (holder !== undefined) {
                    hint = ` — @${holder.alias} is connected${holder.since !== undefined ? ` since ${holder.since}` : ""}`;
                }
            }
            catch {
                // best effort — the hint must never break the error path
            }
        }
        notify(ctx, `mesh: rename to "${alias}" failed: ${renamed.reason}${hint}`);
    }
}
async function cmdNew(rt, ctx, withHistory, pi, getRuntime) {
    if (typeof ctx.newSession !== "function") {
        notify(ctx, "mesh: /mesh new requires a session command context (newSession unavailable)");
        return;
    }
    const history = withHistory
        ? rt.client.transcript.slice(-30).map((frame) => `${frame.ts.slice(11, 19)} ${frame.type} ${frame.from ?? "?"}→${frame.to ?? "*"}: ${(frame.body ?? "").slice(0, 120)}`)
        : undefined;
    rt.identity.savePending(identityFromClient(rt.sessionId, rt.client), history);
    notify(ctx, `mesh: opening a new session — @${rt.client.alias} handed off (rooms/reservations kept)`);
    try {
        // Pi's replaced command context is stale: its native withSession supplies the fresh one.
        // Omp emits session_switch and supplies the fresh context through that public event.
        const result = isOmpHost(pi)
            ? await ctx.newSession()
            : await ctx.newSession({
                withSession: async (newCtx) => {
                    newCtx.ui.notify("mesh: identity handoff received — same alias/rooms/reservations", "info");
                },
            });
        if (result.cancelled) {
            rt.identity.consumePending();
            notify(ctx, "mesh: new session cancelled — current identity unchanged");
        }
        else if (isOmpHost(pi)) {
            getRuntime()?.ctx?.ui.notify("mesh: identity handoff received — same alias/rooms/reservations", "info");
        }
    }
    catch (error) {
        rt.identity.consumePending();
        throw error;
    }
}
async function cmdReset(rt, ctx, pi, getHud, getRuntime, resetActivity) {
    // like /new (fresh alias, default rooms, no reservations) but stays in
    // this pi session, like /reload does for identity preservation.
    const oldAlias = rt.client.alias;
    rt.markDetached?.();
    rt.stopHold?.();
    // 1. leave the mesh cleanly (alias/rooms/reservations purged at the broker)
    await rt.client.close().catch(() => { });
    if (getRuntime() !== rt)
        return;
    // 2. factory-reset the persisted identity for this session
    rt.identity.reset(rt.sessionId);
    // 3. spawn a fresh client in-place (same runtime object, re-attached)
    const config = loadConfig(rt.stateDir);
    const fresh = new MeshClient({
        alias: config.alias, // explicit config.alias wins; otherwise random
        rooms: config.rooms,
        runtimeDir: rt.runtimeDir,
        config,
    });
    rt.client = fresh;
    attachClientListeners(pi, rt, getHud, ctx, fresh, (r) => {
        try {
            r.identity.save(identityFromClient(r.sessionId, r.client));
        }
        catch {
            // best effort
        }
    });
    resetActivity?.(rt);
    try {
        await fresh.connect();
        if (getRuntime() !== rt || rt.client !== fresh) {
            await fresh.close().catch(() => { });
            return;
        }
        updateSessionName(pi, rt);
        notify(ctx, `mesh: identity reset — was @${oldAlias}, now @${fresh.alias} (rooms: ${fresh.rooms.join(",") || "default"})`);
    }
    catch (error) {
        if (getRuntime() === rt && rt.client === fresh)
            reportConnectionError(pi, rt.ctx, error);
    }
}
function cmdLog(rt, ctx, arg) {
    if (arg === "on") {
        rt.transcript.setEnabled(true);
        notify(ctx, "mesh: transcript ON (bodies redacted, retention applies)");
    }
    else if (arg === "off") {
        rt.transcript.setEnabled(false);
        notify(ctx, "mesh: transcript OFF");
    }
    else {
        notify(ctx, `mesh: transcript is ${rt.transcript.isEnabled() ? "ON" : "OFF"}`);
    }
}
async function cmdPing(rt, ctx, alias) {
    if (alias === undefined) {
        notify(ctx, "usage: /mesh ping <alias>");
        return;
    }
    const res = await rt.client.send({ to: alias, message: "ping" });
    switch (res.status) {
        case "delivered":
            notify(ctx, `mesh: pong-path ok — delivered ${res.msgId}`);
            break;
        case "queued_offline":
            notify(ctx, `mesh: @${alias} offline — queued ${res.msgId}`);
            break;
        default:
            notify(ctx, `mesh: ping ${res.status}${"reason" in res ? `: ${res.reason}` : ""}`);
    }
}
/** `/mesh stale` — pure report builder: every reservation currently
 *  blocking peers, with its age vs the TTL (measured: 10 leaked claims,
 *  some blocking edits for 5+ h). Exported for tests. */
export function buildStaleReport(reservationsByPeer, selfAlias, ttlMs, activityOf, now = Date.now()) {
    const rows = [];
    for (const [alias, reservations] of reservationsByPeer) {
        if (alias === selfAlias)
            continue;
        for (const r of reservations) {
            const t = r.since !== undefined ? Date.parse(r.since) : Number.NaN;
            const ageH = Number.isNaN(t) ? "?" : `${((now - t) / 3_600_000).toFixed(1)}h`;
            const expired = ttlMs > 0 && !Number.isNaN(t) && now - t > ttlMs ? " (EXPIRED — ignorable)" : "";
            const act = activityOf(alias);
            const state = act === undefined ? "" : act.state === "busy" ? " · busy" : ` · ${act.state}`;
            rows.push(`  @${alias} · ${r.pattern} · held ${ageH}${state}${expired}`);
        }
    }
    return rows;
}
function cmdStale(rt, ctx) {
    const rows = buildStaleReport(rt.client.peerReservationMap, rt.client.alias, rt.client.reservationTtlMs, (a) => rt.client.activityOf(a));
    if (rows.length === 0) {
        notify(ctx, "mesh: no peer reservations (nothing stale)");
        return;
    }
    const ttlMs = rt.client.reservationTtlMs;
    notify(ctx, `mesh reservations held by peers (TTL ${ttlMs > 0 ? `${(ttlMs / 3_600_000).toFixed(0)} h` : "off"}):\n${rows.join("\n")}`);
}
/** Phase 3: session file size + compaction count (context pressure hint). */
function sessionHealth(rt) {
    const file = rt.ctx?.sessionManager?.getSessionFile?.();
    if (file === undefined || file === "")
        return null;
    let sizeMb = 0;
    let compactions = 0;
    try {
        sizeMb = fs.statSync(file).size / 1e6;
        // count compaction entries — streaming scan, bounded to the first MBs
        const fd = fs.openSync(file, "r");
        try {
            const CHUNK = 256 * 1024;
            const buf = Buffer.alloc(CHUNK);
            let pos = 0;
            let carry = "";
            let scanned = 0;
            for (;;) {
                const n = fs.readSync(fd, buf, 0, CHUNK, pos);
                if (n <= 0)
                    break;
                pos += n;
                scanned += n;
                const text = carry + buf.subarray(0, n).toString("utf8");
                const lines = text.split("\n");
                carry = lines.pop() ?? "";
                for (const line of lines) {
                    if (line.includes('"compaction"'))
                        compactions += 1;
                }
                if (scanned > 4 * 1024 * 1024)
                    break; // good enough; never block
            }
        }
        finally {
            fs.closeSync(fd);
        }
    }
    catch {
        return null;
    }
    return { sizeMb, compactions, path: file };
}
function cmdBroker(rt, ctx) {
    const sock = brokerSocketPath(rt.runtimeDir);
    let lockInfo = "absent";
    try {
        const pid = Number(fs.readFileSync(brokerLockPath(rt.runtimeDir), "utf8").trim());
        if (Number.isFinite(pid))
            lockInfo = `pid=${pid} alive=${pidAlive(pid)}`;
    }
    catch {
        // no lock file
    }
    const uptimeS = Math.floor((Date.now() - rt.startedAt) / 1000);
    const lines = [
        `mesh broker:`,
        `  version: ${MESH_VERSION}`,
        `  socket: ${sock}`,
        `  lock: ${lockInfo}`,
        `  online: ${rt.client.isOnline()}`,
        `  session uptime: ${uptimeS}s`,
        `  inbound failures: ledger=${rt.ledgerFailures} transcript=${rt.transcriptFailures} injection=${rt.injectionFailures}`,
    ];
    // Phase 3: context pressure — a huge session means frequent compactions
    const health = sessionHealth(rt);
    if (health !== null) {
        lines.push(`  session file: ${health.sizeMb.toFixed(1)} MB · ${health.compactions} compaction(s)`);
        if (health.sizeMb > 15) {
            lines.push(`  hint: /mesh new recommended (context pressure — identity is handed over)`);
        }
    }
    notify(ctx, lines.join("\n"));
}
export function registerCommands(pi, getRuntime, onChanged, // HUD refresh after join/leave/log toggles
getHud = () => null, resetActivity) {
    pi.registerCommand("mesh", {
        description: "mesh inter-agent coms: status, join/leave, alias, log, ping, broker",
        handler: async (args, ctx) => {
            const rt = getRuntime();
            if (rt === null) {
                notify(ctx, "mesh: session not started");
                return;
            }
            rt.ctx = ctx;
            const [sub, ...rest] = args.trim().split(/\s+/).filter((s) => s.length > 0);
            switch (sub) {
                case "inbox":
                    if (rest.length === 0) {
                        notify(ctx, rt.deferredInbox?.list() ?? "mesh: deferred inbox empty");
                    }
                    else if (rest.length === 1 && rest[0] === "flush") {
                        const count = rt.deferredInbox?.flush() ?? 0;
                        notify(ctx, `mesh: flushed ${count} deferred broadcast(s)` +
                            ((rt.deferredInbox?.count ?? 0) > 0 ? " (remaining messages already queued for the next prompt)" : ""));
                    }
                    else {
                        notify(ctx, "usage: /mesh inbox [flush]");
                    }
                    break;
                case "status":
                    await cmdStatus(rt, ctx, pi, rest[0]);
                    break;
                case "join": {
                    // `/mesh join <room> [as <alias>] [observer]`
                    const parsed = parseJoinArgs(rest);
                    await cmdJoin(rt, ctx, parsed.room, parsed.observer, parsed.asAlias, pi);
                    onChanged?.();
                    break;
                }
                case "leave":
                    await cmdLeave(rt, ctx, rest[0], pi);
                    onChanged?.();
                    break;
                case "alias":
                    await cmdAlias(rt, ctx, rest[0], pi);
                    onChanged?.();
                    break;
                case "log":
                    cmdLog(rt, ctx, rest[0]);
                    onChanged?.();
                    break;
                case "ping":
                    await cmdPing(rt, ctx, rest[0]);
                    break;
                case "stale":
                    cmdStale(rt, ctx);
                    break;
                case "broker":
                    cmdBroker(rt, ctx);
                    break;
                case "new":
                    await cmdNew(rt, ctx, rest.includes("--history") || rest.includes("history"), pi, getRuntime);
                    break;
                case "reset":
                    await cmdReset(rt, ctx, pi, getHud, getRuntime, resetActivity);
                    onChanged?.();
                    break;
                case "help":
                case undefined:
                    notify(ctx, HELP_TEXT);
                    break;
                default:
                    notify(ctx, `mesh: unknown subcommand '${sub}'\n${HELP_TEXT}`);
            }
        },
    });
}
