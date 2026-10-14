// broker/broker.ts — the mesh broker server: frame dispatch, rooms,
// mailbox, rate limits, policy. Stateless in-memory (rooms/mailbox are
// re-declared by clients at hello).
// Run directly (`node dist/src/broker/broker.js`) or via startBroker({config, policy}) in tests.
import { randomUUID } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import net from "node:net";
import tls from "node:tls";
import { fileURLToPath } from "node:url";
import { buildFrame, parseFrameLine, } from "../protocol/envelope.js";
import { encodeFrame, FrameDecoder, FrameSizeError, makeMsgId, sha256 } from "../protocol/frames.js";
import { DEFAULT_MAX_FRAME_BYTES, DEFAULT_ROOM, isBrokerId, isMeshId, parseEndpoint, HELLO_TIMEOUT_MS, MAILBOX_PURGE_INTERVAL_MS, SWEEP_INTERVAL_MS, WRITE_TIMEOUT_MS, loadConfig, } from "../shared/config.js";
import { brokerLockPath, brokerSocketPath, runtimeDir, stateDir } from "../shared/paths.js";
import { loadOrCreateBrokerId } from "../shared/broker-identity.js";
import { enqueueMailbox, flushMailbox, purgeAllExpired } from "./mailbox.js";
import { evaluatePolicy, loadPolicy } from "./policy.js";
import { checkRate } from "./ratelimit.js";
import { announceOnline, broadcastToRoom, detachFromRooms, joinRoom, leaveRoom, peersSnapshot, presenceFrame, } from "./rooms.js";
import { BrokerState } from "./state.js";
/** Bounded write (5 s). cb(false) on timeout/error; socket destroyed. */
function writeFrame(socket, frame, maxBytes, cb) {
    let settled = false;
    const finish = (ok) => {
        if (settled)
            return;
        settled = true;
        clearTimeout(timer);
        cb(ok);
    };
    const timer = setTimeout(() => {
        socket.destroy();
        finish(false);
    }, WRITE_TIMEOUT_MS);
    timer.unref();
    let buf;
    try {
        buf = encodeFrame(frame, maxBytes);
    }
    catch {
        clearTimeout(timer);
        settled = true;
        cb(false);
        return;
    }
    if (socket.destroyed || !socket.writable) {
        finish(false);
        return;
    }
    socket.write(buf, (err) => finish(!err));
}
export function createBroker(options) {
    const { config, policy } = options;
    const meshId = config.meshId ?? "default";
    const brokerId = config.brokerId ?? randomUUID();
    if (!isMeshId(meshId) || !isBrokerId(brokerId)) {
        throw new Error("broker_identity_invalid: invalid meshId or brokerId");
    }
    // Library brokers are ephemeral unless the caller supplies an ID. Only
    // standalone main reads/writes the state directory.
    const brokerInfo = { meshId, brokerId, bootId: randomUUID() };
    const sockPath = options.socketPath ?? brokerSocketPath();
    const state = new BrokerState();
    const tcp = options.tcpListen;
    // D39: broker-side lifecycle debug (MESH_DEBUG=1) — destroys, write
    // failures, close reasons. NEVER bodies.
    const dbg = (line) => {
        if (config.debug !== true)
            return;
        try {
            mkdirSync(stateDir(), { recursive: true });
            appendFileSync(`${stateDir()}/broker-debug.log`, `${new Date().toISOString()} ${line}\n`);
        }
        catch {
            // best effort
        }
    };
    // sockets that arrived over TCP/TLS — these require the shared token at
    // hello; local unix-socket connections stay tokenless (filesystem perms).
    const tokenSockets = new WeakSet();
    const sendTo = (peer, frame) => {
        writeFrame(peer.socket, frame, config.maxFrameBytes, (ok) => {
            // identity guard — only close if this exact record is still current
            // (the alias may have re-hello'd onto a new socket since the write).
            if (!ok && state.peers.get(peer.alias) === peer) {
                dbg(`sendTo: write FAILED to ${peer.alias} (${frame.type}) → closePeer`);
                closePeer(state, peer.alias, "write_failure");
            }
        });
    };
    const sendError = (socket, code, id) => {
        writeFrame(socket, buildFrame({ type: "error", code, id }), config.maxFrameBytes, () => { });
    };
    const sendAck = (peer, id, status, interruptStatus, deliveredCount, totalCount) => {
        sendTo(peer, buildFrame({ type: "ack", id, status, interruptStatus, deliveredCount, totalCount }));
    };
    /** Honest drop receipts: tell each ORIGINAL sender (when online) that
     *  their message left the recipient's offline mailbox — TTL expiry or
     *  cap eviction. The ack id carries the original msg id so the client
     *  can correlate and settle any live awaitReply mission immediately;
     *  `to` names the mailbox the message was bound for. Offline senders
     *  get nothing here (a notice would itself need a mailbox) — the
     *  status_res counter still records every drop. */
    const notifyDropped = (dropped) => {
        for (const { frame } of dropped) {
            const sender = frame.from !== undefined ? state.peers.get(frame.from) : undefined;
            if (sender === undefined || !sender.helloDone || sender.socket.destroyed)
                continue;
            sendTo(sender, buildFrame({ type: "ack", id: frame.id, status: "dropped_offline", to: frame.to }));
        }
    };
    /** replyAll — fan the answer out to every ONLINE room member (except the
    *  sender). Replies are never mailboxed (same policy as unicast replies). */
    const routeReplyAll = (from, frame) => {
        const room = frame.room ?? DEFAULT_ROOM;
        const members = state.rooms.get(room);
        if (members === undefined || members.size <= 1) {
            sendAck(from, frame.id, "dropped_offline");
            return;
        }
        const writes = [];
        let total = 0;
        for (const alias of members) {
            if (alias === from.alias)
                continue;
            const t = state.peers.get(alias);
            if (t !== undefined && t.helloDone && !t.socket.destroyed) {
                total += 1;
                const target = t;
                writes.push(new Promise((resolve) => {
                    writeFrame(target.socket, frame, config.maxFrameBytes, (ok) => {
                        if (ok) {
                            state.stats.relayed += 1;
                            resolve(true);
                        }
                        else {
                            if (state.peers.get(target.alias) === target)
                                closePeer(state, target.alias, "write_failure");
                            resolve(false);
                        }
                    });
                }));
            }
        }
        void Promise.all(writes).then((results) => {
            const delivered = results.filter(Boolean).length;
            if (delivered > 0)
                sendAck(from, frame.id, "delivered", undefined, delivered, total);
            else
                sendAck(from, frame.id, "dropped_offline");
        });
    };
    /** replyTargets — fan the answer out to the designated aliases (online
    *  only, like every reply). Ack carries deliveredCount/totalCount. */
    const routeReplyTargets = (from, frame) => {
        const targets = frame.replyTargets ?? [];
        const writes = [];
        let total = 0;
        for (const alias of targets) {
            if (alias === from.alias)
                continue;
            const t = state.peers.get(alias);
            if (t !== undefined && t.helloDone && !t.socket.destroyed) {
                total += 1;
                const target = t;
                writes.push(new Promise((resolve) => {
                    writeFrame(target.socket, frame, config.maxFrameBytes, (ok) => {
                        if (ok) {
                            state.stats.relayed += 1;
                            resolve(true);
                        }
                        else {
                            if (state.peers.get(target.alias) === target)
                                closePeer(state, target.alias, "write_failure");
                            resolve(false);
                        }
                    });
                }));
            }
        }
        void Promise.all(writes).then((results) => {
            const delivered = results.filter(Boolean).length;
            if (delivered > 0)
                sendAck(from, frame.id, "delivered", undefined, delivered, total);
            else
                sendAck(from, frame.id, "dropped_offline");
        });
    };
    // ---- handlers ----
    const handleHello = (socket, frame) => {
        const alias = frame.from;
        if (alias === undefined) {
            sendError(socket, "invalid_alias", frame.id);
            socket.destroy();
            return;
        }
        // tcp/tls CONNECTIONS require the shared token (sha256 match); local
        // unix-socket connections are tokenless (protected by file perms).
        if (tokenSockets.has(socket)) {
            const expected = config.brokerToken !== undefined ? sha256(config.brokerToken) : undefined;
            if (expected === undefined || frame.token !== expected) {
                sendError(socket, "invalid_token", frame.id);
                socket.destroy();
                return;
            }
        }
        if (frame.meshId !== undefined && frame.meshId !== brokerInfo.meshId) {
            sendError(socket, "mesh_mismatch", frame.id);
            socket.destroy();
            return;
        }
        const existing = state.peers.get(alias);
        if (existing && existing.socket !== socket && !existing.socket.destroyed) {
            sendError(socket, "alias_taken", frame.id); // same-tick refusal
            socket.destroy();
            return;
        }
        if (existing)
            state.peers.delete(alias);
        const peer = {
            alias,
            socket,
            rooms: new Map(),
            connectedAt: Date.now(),
            lastSeenAt: Date.now(),
            helloDone: true,
            reservations: frame.reservations ?? [],
            // M1: the sender's extension version (hello) — shown in snapshots
            clientVersion: typeof frame.clientVersion === "string" && frame.clientVersion.length > 0
                ? frame.clientVersion.slice(0, 64)
                : undefined,
            // D40: connection origin — tcp/tls sockets carry the remote IP (shown in
            // HUD/status so everyone sees WHICH machine a peer runs on); unix-socket
            // peers are broker-local (via stays undefined).
            via: tokenSockets.has(socket)
                ? `${tcp?.tls === true ? "tls" : "tcp"}:${(socket.remoteAddress ?? "?").replace(/^::ffff:/, "")}`
                : undefined,
        };
        state.peers.set(alias, peer);
        state.knownAliases.set(alias, Date.now()); // (re)stamp on every hello
        // rooms — the hello carries the client's EXACT room list. When the
        // field is present (even empty), it is authoritative: a client that
        // left "default" must NOT be re-joined to it on every reconnect (that
        // was the bug: "default" kept coming back after /mesh leave default).
        // Only a legacy hello without a rooms field gets the default room.
        const requested = frame.rooms !== undefined
            ? new Set(frame.rooms)
            : new Set([DEFAULT_ROOM]);
        for (const roomId of requested) {
            const role = frame.role ?? "member";
            const res = joinRoom(state, config, peer, roomId, role);
            if (!res.ok) {
                sendError(socket, res.code, frame.id);
                detachFromRooms(state, peer);
                state.peers.delete(alias);
                socket.destroy();
                return;
            }
        }
        const { frames: mailboxFrames, dropped: mailboxDropped } = flushMailbox(state, config, alias);
        if (mailboxDropped.length > 0)
            notifyDropped(mailboxDropped);
        sendTo(peer, buildFrame({
            type: "welcome",
            id: frame.id,
            from: alias,
            rooms: [...peer.rooms.keys()],
            peers: peersSnapshot(state),
            mailboxCount: mailboxFrames.length,
            broker: brokerInfo,
        }));
        for (const mf of mailboxFrames)
            sendTo(peer, mf); // mailbox frames right after welcome
        announceOnline(state, peer, sendTo);
        // existing peers learn the newcomer's reservations (like presence)
        if (peer.reservations.length > 0)
            broadcastReservations(state, peer, sendTo);
    };
    const routeMsg = (from, frame) => {
        const to = frame.to;
        const room = frame.room ?? DEFAULT_ROOM;
        // broadcast → fan out to every room member (except the sender).
        if (frame.broadcast === true) {
            if (!from.rooms.has(room)) {
                state.stats.refused += 1;
                sendError(from.socket, "not_member", frame.id);
                return;
            }
            if (from.rooms.get(room) === "observer") {
                state.stats.refused += 1;
                sendError(from.socket, "observer_readonly", frame.id);
                return;
            }
            const members = state.rooms.get(room);
            if (members === undefined || members.size <= 1) {
                state.stats.refused += 1;
                sendError(from.socket, "peer_not_found", frame.id);
                return;
            }
            const priority = frame.priority ?? "normal";
            const kind = priority === "force" ? "force" : priority === "urgent" ? "urgent" : "msg";
            if (!checkRate(state, from.alias, kind, policy.rateLimits)) {
                state.stats.refused += 1;
                sendError(from.socket, "rate_limited", frame.id);
                return;
            }
            const decision = evaluatePolicy(policy, { from: from.alias, to: "*", room, priority });
            if (decision.action === "deny") {
                state.stats.refused += 1;
                sendError(from.socket, decision.code, frame.id);
                return;
            }
            // deliver to every member: online → socket, offline-known → mailbox
            const writes = [];
            let total = 0;
            let queued = 0;
            for (const alias of members) {
                if (alias === from.alias)
                    continue;
                total += 1;
                const t = state.peers.get(alias);
                if (t !== undefined && t.helloDone && !t.socket.destroyed) {
                    const target = t;
                    writes.push(new Promise((resolve) => {
                        writeFrame(target.socket, frame, config.maxFrameBytes, (ok) => {
                            if (ok) {
                                resolve(true);
                            }
                            else {
                                // identity guard — a re-hello may have replaced the record.
                                if (state.peers.get(target.alias) === target)
                                    closePeer(state, target.alias, "write_failure");
                                notifyDropped(enqueueMailbox(state, config, target.alias, frame));
                                queued += 1;
                                resolve(false);
                            }
                        });
                    }));
                }
                else if (state.knownAliases.has(alias)) {
                    notifyDropped(enqueueMailbox(state, config, alias, frame));
                    queued += 1;
                }
            }
            void Promise.all(writes).then((results) => {
                const delivered = results.filter(Boolean).length;
                state.stats.relayed += delivered;
                const status = delivered > 0 ? "delivered" : "queued_offline";
                sendAck(from, frame.id, status, undefined, delivered, total);
            });
            return;
        }
        if (to === undefined) {
            state.stats.refused += 1;
            sendError(from.socket, "invalid_frame", frame.id);
            return;
        }
        // room shared between emitter and recipient
        if (!from.rooms.has(room)) {
            state.stats.refused += 1;
            sendError(from.socket, "not_member", frame.id);
            return;
        }
        if (from.rooms.get(room) === "observer") {
            state.stats.refused += 1;
            sendError(from.socket, "observer_readonly", frame.id);
            return;
        }
        const target = state.peers.get(to);
        const knownOnline = target !== undefined && target.helloDone && !target.socket.destroyed;
        if (!knownOnline && !state.knownAliases.has(to)) {
            state.stats.refused += 1;
            sendError(from.socket, "peer_not_found", frame.id);
            return;
        }
        if (knownOnline && target && !target.rooms.has(room)) {
            state.stats.refused += 1;
            sendError(from.socket, "not_member", frame.id);
            return;
        }
        // rate limits: kind from priority
        const priority = frame.priority ?? "normal";
        const kind = priority === "force" ? "force" : priority === "urgent" ? "urgent" : "msg";
        if (!checkRate(state, from.alias, kind, policy.rateLimits)) {
            state.stats.refused += 1;
            sendError(from.socket, "rate_limited", frame.id);
            return;
        }
        // declarative policy
        const decision = evaluatePolicy(policy, { from: from.alias, to, room, priority });
        let routedFrame = frame;
        let interruptStatus;
        if (decision.action === "deny") {
            state.stats.refused += 1;
            sendError(from.socket, decision.code, frame.id);
            return;
        }
        if (decision.action === "downgrade") {
            // D6: a downgraded force becomes urgent — keeping the interrupt flag
            // would either lie (dropped after ack at the recipient's validation)
            // or bypass the escalation policy (urgent+interrupt honored). Strip it;
            // the force_downgraded ack already tells the sender the truth.
            routedFrame = { ...frame, priority: "urgent", interrupt: undefined };
            interruptStatus = "force_downgraded";
        }
        else if (priority === "force") {
            interruptStatus = "force_accepted";
        }
        if (knownOnline && target) {
            const t = target;
            // fix: ack(delivered) ONLY after the write to the recipient socket succeeds.
            writeFrame(t.socket, routedFrame, config.maxFrameBytes, (ok) => {
                if (ok) {
                    state.stats.relayed += 1;
                    sendAck(from, frame.id, "delivered", interruptStatus);
                }
                else {
                    // identity guard — a re-hello may have replaced the record.
                    if (state.peers.get(t.alias) === t)
                        closePeer(state, t.alias, "write_failure");
                    notifyDropped(enqueueMailbox(state, config, t.alias, routedFrame));
                    sendAck(from, frame.id, "queued_offline", interruptStatus);
                }
            });
        }
        else {
            // known offline alias → mailbox, THEN ack(queued_offline)
            notifyDropped(enqueueMailbox(state, config, to, routedFrame));
            state.stats.relayed += 1;
            sendAck(from, frame.id, "queued_offline", interruptStatus);
        }
    };
    /** read receipt — deliver to the original message sender (frame.to). */
    const routeRead = (from, frame) => {
        routeOnlineOnly(from, frame, true);
    };
    const routeOnlineOnly = (from, frame, silentDrop) => {
        const to = frame.to;
        const target = to !== undefined ? state.peers.get(to) : undefined;
        if (target && target.helloDone && !target.socket.destroyed) {
            const t = target;
            writeFrame(t.socket, frame, config.maxFrameBytes, (ok) => {
                if (ok) {
                    state.stats.relayed += 1;
                    if (!silentDrop)
                        sendAck(from, frame.id, "delivered");
                }
                else {
                    // identity guard — same stale-record hazard as routeMsg.
                    if (state.peers.get(t.alias) === t)
                        closePeer(state, t.alias, "write_failure");
                    if (!silentDrop)
                        sendAck(from, frame.id, "dropped_offline");
                }
            });
        }
        else if (!silentDrop) {
            // replies to offline targets: explicit honest status
            sendAck(from, frame.id, "dropped_offline");
        }
        // reminds to offline targets: dropped silently
    };
    const dispatch = (socket, frame) => {
        const peer = frame.from !== undefined ? state.peers.get(frame.from) : undefined;
        const authed = peer !== undefined && peer.socket === socket && peer.helloDone;
        if (frame.type === "hello") {
            if (authed) {
                sendError(socket, "invalid_frame", frame.id); // already claimed
                return;
            }
            handleHello(socket, frame);
            return;
        }
        if (!authed || !peer) {
            sendError(socket, "hello_required", frame.id); // only hello allowed pre-welcome
            return;
        }
        peer.lastSeenAt = Date.now();
        switch (frame.type) {
            case "msg":
                routeMsg(peer, frame);
                break;
            case "reserve":
                // replace the peer's reservations, broadcast the new state to
                // every other peer (like presence), then ack the sender.
                peer.reservations = Array.isArray(frame.reservations) ? [...frame.reservations] : [];
                broadcastReservations(state, peer, sendTo);
                sendAck(peer, frame.id, "ok");
                break;
            case "reply":
                // replies are messages too — rate-limit them so a
                // confirmation ping-pong cannot spam without bound.
                if (!checkRate(state, peer.alias, "msg", policy.rateLimits)) {
                    sendError(peer.socket, "rate_limited", frame.id);
                    break;
                }
                if (frame.replyAll === true)
                    routeReplyAll(peer, frame);
                else if (frame.replyTargets !== undefined)
                    routeReplyTargets(peer, frame);
                else
                    routeOnlineOnly(peer, frame, false);
                break;
            case "remind":
                routeOnlineOnly(peer, frame, true);
                break;
            case "read":
                // read receipt → routed to the ORIGINAL sender of the msgId,
                // online-only and silent (never acked, never mailboxed).
                routeRead(peer, frame);
                break;
            case "activity": {
                // Phase 3: the peer announces its turn state — stored, then shared
                // with the members of every room it belongs to (fire-and-forget).
                const actState = frame.status === "busy"
                    ? "busy"
                    : frame.status === "rate_limited"
                        ? "rate_limited"
                        : frame.status === "blocked"
                            ? "blocked"
                            : "idle";
                peer.activity = { state: actState, at: new Date().toISOString() };
                for (const roomId of peer.rooms.keys()) {
                    broadcastToRoom(state, roomId, buildFrame({ type: "activity", from: peer.alias, status: actState, room: roomId }), peer.alias, sendTo);
                }
                break;
            }
            case "ping":
                sendTo(peer, buildFrame({ type: "pong", id: frame.id }));
                break;
            case "status_req": {
                const s = state.stats;
                sendTo(peer, buildFrame({
                    type: "status_res",
                    id: frame.id,
                    broker: brokerInfo,
                    peers: peersSnapshot(state, frame.room),
                    rooms: [...state.rooms.keys()],
                    // M2: broker counters ride along — relayed/refused/mailbox
                    stats: { relayed: s.relayed, refused: s.refused, mailboxDelivered: s.mailboxDelivered, mailboxDropped: s.mailboxDropped },
                }));
                break;
            }
            case "join": {
                if (frame.room === undefined) {
                    sendError(socket, "invalid_room", frame.id);
                    break;
                }
                const res = joinRoom(state, config, peer, frame.room, frame.role ?? "member");
                if (res.ok) {
                    sendAck(peer, frame.id, "ok");
                    broadcastToRoom(state, frame.room, presenceFrame(peer.alias, "online", frame.room), peer.alias, sendTo);
                }
                else {
                    sendError(socket, res.code, frame.id);
                }
                break;
            }
            case "leave": {
                if (frame.room === undefined) {
                    sendError(socket, "invalid_room", frame.id);
                    break;
                }
                const res = leaveRoom(state, peer, frame.room);
                if (res.ok) {
                    sendAck(peer, frame.id, "ok");
                    // presence(offline-in-room) to the remaining members (was a
                    // silent no-op before) — peers learn the leave immediately.
                    broadcastToRoom(state, frame.room, presenceFrame(peer.alias, "offline", frame.room), peer.alias, sendTo);
                }
                else {
                    sendError(socket, res.code, frame.id);
                }
                break;
            }
            default:
                sendError(socket, "invalid_frame", frame.id);
        }
    };
    /** broadcast one peer's full reservation state to every other peer. */
    function broadcastReservations(state, peer, sendTo) {
        const frame = buildFrame({
            type: "reserve",
            from: peer.alias,
            reservations: peer.reservations,
        });
        for (const p of state.peers.values()) {
            if (p.alias === peer.alias)
                continue;
            sendTo(p, frame);
        }
    }
    const serverHandler = (socket, tokenRequired = false) => {
        if (tokenRequired)
            tokenSockets.add(socket);
        socket.setNoDelay(true);
        const decoder = new FrameDecoder(config.maxFrameBytes);
        // handshake bound: hello ≤ 5 s
        const helloTimer = setTimeout(() => {
            dbg(`helloTimer: destroying socket after ${HELLO_TIMEOUT_MS}ms without hello`);
            sendError(socket, "timeout");
            socket.destroy();
        }, HELLO_TIMEOUT_MS);
        helloTimer.unref();
        socket.on("data", (chunk) => {
            let lines;
            try {
                lines = decoder.push(chunk);
            }
            catch (err) {
                if (err instanceof FrameSizeError) {
                    sendError(socket, "oversized"); // oversized + close
                    socket.destroy();
                    return;
                }
                throw err;
            }
            for (const line of lines) {
                const parsed = parseFrameLine(line);
                if (!parsed.ok) {
                    sendError(socket, parsed.code);
                    if (parsed.code === "invalid_alias" || parsed.code === "invalid_frame")
                        continue;
                    continue;
                }
                // any valid frame clears the handshake deadline once hello done
                dispatch(socket, parsed.frame);
                const p = parsed.frame.from !== undefined ? state.peers.get(parsed.frame.from) : undefined;
                if (p && p.socket === socket && p.helloDone)
                    clearTimeout(helloTimer);
                if (parsed.frame.type === "hello")
                    clearTimeout(helloTimer);
            }
        });
        socket.on("error", () => { });
        socket.on("close", () => {
            clearTimeout(helloTimer);
            for (const peer of [...state.peers.values()]) {
                if (peer.socket === socket) {
                    dbg(`socket closed: ${peer.alias} (had hello=${peer.helloDone}, lastSeen ${Date.now() - peer.lastSeenAt}ms ago)`);
                    closePeer(state, peer.alias, "socket_closed");
                }
            }
        });
    };
    const onTcpConn = (socket) => serverHandler(socket, true);
    const server = tcp === undefined
        ? net.createServer((s) => serverHandler(s, false))
        : tcp.tls
            ? tls.createServer({
                cert: config.tlsCert !== undefined ? readFileSync(config.tlsCert) : undefined,
                key: config.tlsKey !== undefined ? readFileSync(config.tlsKey) : undefined,
            }, onTcpConn)
            : net.createServer(onTcpConn);
    // D38 multi-machine: with alsoUnix the broker serves BOTH the local unix
    // socket (tokenless — existing local sessions unchanged) and the tcp/tls
    // endpoint (token required) for remote machines such as a LAN MacBook.
    const unixServer = tcp !== undefined && options.alsoUnix === true
        ? net.createServer((s) => serverHandler(s, false))
        : undefined;
    // silence sweep: destroy sockets silent > brokerSilenceMs
    const sweep = setInterval(() => {
        const now = Date.now();
        for (const peer of [...state.peers.values()]) {
            if (peer.helloDone && now - peer.lastSeenAt > config.brokerSilenceMs) {
                dbg(`sweep: destroying ${peer.alias} (silent ${now - peer.lastSeenAt}ms > ${config.brokerSilenceMs}ms)`);
                peer.socket.destroy(); // close event → closePeer
            }
        }
        // prune stale known aliases — mailbox eligibility only.
        pruneStaleKnownAliases(state, now);
        // expire reservations older than the configured TTL (0 = unlimited),
        // and tell the peers when something expired.
        if (config.reservationTtlMs > 0) {
            for (const peer of [...state.peers.values()]) {
                const before = peer.reservations.length;
                if (before === 0)
                    continue;
                peer.reservations = peer.reservations.filter((r) => {
                    if (r.since === undefined)
                        return true;
                    const t = Date.parse(r.since);
                    if (Number.isNaN(t))
                        return true;
                    return now - t <= config.reservationTtlMs;
                });
                if (peer.reservations.length < before) {
                    broadcastReservations(state, peer, sendTo);
                }
            }
        }
    }, SWEEP_INTERVAL_MS);
    sweep.unref();
    const mailboxPurge = setInterval(() => notifyDropped(purgeAllExpired(state, config)), MAILBOX_PURGE_INTERVAL_MS);
    mailboxPurge.unref();
    const purgeMailboxExpired = (now) => {
        notifyDropped(purgeAllExpired(state, config, now));
    };
    const listening = unixServer !== undefined ? [server, unixServer] : [server];
    return new Promise((resolve, reject) => {
        let settled = false;
        const fail = (err) => {
            if (settled)
                return;
            settled = true;
            for (const srv of listening)
                srv.close();
            reject(err);
        };
        for (const srv of listening)
            srv.once("error", fail);
        let waiting = listening.length;
        const onListening = () => {
            waiting -= 1;
            if (waiting > 0 || settled)
                return;
            settled = true;
            for (const srv of listening)
                srv.removeListener("error", fail);
            const addr = server.address();
            const bound = typeof addr === "object" && addr !== null && "port" in addr
                ? `tcp://${tcp?.host ?? "0.0.0.0"}:${addr.port}`
                : sockPath;
            resolve({
                server,
                state,
                socketPath: unixServer !== undefined ? `${bound} + unix://${sockPath}` : bound,
                purgeMailboxExpired,
                close: () => new Promise((res) => {
                    clearInterval(sweep);
                    clearInterval(mailboxPurge);
                    for (const peer of state.peers.values())
                        peer.socket.destroy();
                    let n = listening.length;
                    for (const srv of listening) {
                        srv.close(() => {
                            n -= 1;
                            if (n <= 0)
                                res();
                        });
                    }
                }),
            });
        };
        if (tcp !== undefined)
            server.listen(tcp.port, tcp.host, onListening);
        else
            server.listen(sockPath, onListening);
        if (unixServer !== undefined)
            unixServer.listen(sockPath, onListening);
    });
}
/** prune stale known aliases — no live peer, no mailbox, and the alias
*  has not re-hello'd in 24 h (mailbox eligibility only). Exported for tests. */
export function pruneStaleKnownAliases(state, now = Date.now(), maxAgeMs = 86_400_000) {
    for (const [alias, lastHello] of [...state.knownAliases]) {
        if (state.peers.has(alias) || state.mailbox.has(alias))
            continue;
        if (now - lastHello > maxAgeMs)
            state.knownAliases.delete(alias);
    }
}
/** Purge tables + presence(offline) to former room members. */
export function closePeer(state, alias, _reason, maxFrameBytes = DEFAULT_MAX_FRAME_BYTES) {
    const peer = state.peers.get(alias);
    if (!peer)
        return;
    const roomIds = detachFromRooms(state, peer);
    state.peers.delete(alias);
    if (!peer.socket.destroyed)
        peer.socket.destroy();
    for (const roomId of roomIds) {
        broadcastToRoom(state, roomId, presenceFrame(alias, "offline", roomId), alias, (p, f) => {
            // same writeFrame discipline as every other write (bounded,
            // encoded); fire-and-forget — broadcast failures are swallowed.
            writeFrame(p.socket, f, maxFrameBytes, () => { });
        });
    }
    // reservations live with the connection — the peer is gone, so
    // notify the remaining peers that its reservations are void.
    if (peer.reservations.length > 0) {
        for (const p of state.peers.values()) {
            writeFrame(p.socket, buildFrame({ type: "reserve", from: alias, reservations: [], id: makeMsgId() }), maxFrameBytes, () => { });
        }
    }
}
// ---- standalone main: lock, bind, signals ----
function pidAlive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    }
    catch {
        return false;
    }
}
/** post-write confirmation — the lock must contain OUR pid. */
function lockHeldByUs(lockPath) {
    try {
        return Number(readFileSync(lockPath, "utf8").trim()) === process.pid;
    }
    catch {
        return false;
    }
}
/** Acquire lock, allowing only the launching parent's explicit PID handoff. */
export function acquireLock(dir, starterPid) {
    mkdirSync(dir, { recursive: true });
    const lockPath = brokerLockPath(dir);
    for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
            writeFileSync(lockPath, String(process.pid), { flag: "wx" });
            // wx-create is atomic, but a concurrent stale-takeover can unlink +
            // rewrite between our write and our next step — re-read and confirm the
            // lock carries OUR pid before proceeding; retry the loop if we lost.
            if (lockHeldByUs(lockPath))
                return true;
            continue;
        }
        catch {
            // exists: check staleness
        }
        try {
            const pid = Number(readFileSync(lockPath, "utf8").trim());
            const starterOwnsLock = pid === starterPid && pid === process.ppid;
            if (Number.isInteger(pid) && pid > 0 && pid !== process.pid && pidAlive(pid) &&
                !starterOwnsLock)
                return false;
        }
        catch {
            // unreadable → treat as stale
        }
        try {
            unlinkSync(lockPath);
        }
        catch {
            // gone already
        }
    }
    return false;
}
/** connect probe — true if a live broker answers on the socket path. */
function probeSocketAlive(sockPath, timeoutMs = 1000) {
    return new Promise((resolve) => {
        const socket = net.createConnection(sockPath);
        let settled = false;
        const finish = (ok) => {
            if (settled)
                return;
            settled = true;
            clearTimeout(timer);
            socket.destroy();
            resolve(ok);
        };
        const timer = setTimeout(() => finish(false), timeoutMs);
        timer.unref();
        socket.once("connect", () => finish(true));
        socket.once("error", () => finish(false));
    });
}
export async function main() {
    const dir = runtimeDir();
    const sDir = stateDir();
    mkdirSync(dir, { recursive: true });
    const starterPid = Number(process.env.MESH_BROKER_STARTER_PID);
    if (!acquireLock(dir, starterPid)) {
        process.stderr.write("mesh broker: lock held by a live process\n");
        process.exit(1);
    }
    const sockPath = brokerSocketPath(dir);
    if (existsSync(sockPath)) {
        // probe before unlink — never hijack a live broker's socket. Only a
        // dead/stale socket path may be removed.
        if (await probeSocketAlive(sockPath)) {
            process.stderr.write("mesh broker: socket path served by a live broker\n");
            process.exit(1);
        }
        try {
            unlinkSync(sockPath);
        }
        catch {
            // best effort
        }
    }
    const config = loadConfig(sDir);
    config.brokerId = loadOrCreateBrokerId(sDir, config.brokerId);
    const policy = loadPolicy(sDir);
    // D38: honor MESH_LISTEN / config.listen (validated by loadConfig — only
    // well-formed endpoints land here). tcp:// or tls:// opens the mesh to
    // remote machines (shared token REQUIRED on those connections); the
    // local unix socket stays up too (alsoUnix) so local sessions — which
    // connect tokenless over the socket — are never disrupted.
    const listenEndpoint = config.listen !== undefined ? parseEndpoint(config.listen) : null;
    const tcpListen = listenEndpoint !== null && listenEndpoint.kind !== "unix"
        ? { host: listenEndpoint.host, port: listenEndpoint.port, tls: listenEndpoint.kind === "tls" }
        : undefined;
    if (tcpListen?.tls === true && (config.tlsCert === undefined || config.tlsKey === undefined)) {
        process.stderr.write("mesh broker: tls:// listen requires MESH_TLS_CERT and MESH_TLS_KEY\n");
        process.exit(1);
    }
    const broker = await createBroker({
        config,
        policy,
        socketPath: sockPath,
        tcpListen,
        alsoUnix: tcpListen !== undefined,
    });
    const shutdown = () => {
        void broker.close().finally(() => {
            try {
                unlinkSync(sockPath);
            }
            catch {
                // already gone
            }
            try {
                unlinkSync(brokerLockPath(dir));
            }
            catch {
                // already gone
            }
            process.exit(0);
        });
    };
    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);
    process.stdout.write(`mesh broker up pid=${process.pid} endpoints=${broker.socketPath}\n`);
}
const isMain = (() => {
    try {
        return process.argv[1] !== undefined && fileURLToPath(import.meta.url) === process.argv[1];
    }
    catch {
        return false;
    }
})();
if (isMain) {
    main().catch((err) => {
        process.stderr.write(`mesh broker fatal: ${String(err)}\n`);
        process.exit(1);
    });
}
