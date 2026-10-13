// broker/rooms.ts — membership, roles, presence broadcasts.
import { buildFrame, isValidRoom } from "../protocol/envelope.js";
import { DEFAULT_ROOM, MAX_PEERS_PER_ROOM, } from "../shared/config.js";
/** Join a room (creating it). Enforces caps and roomId syntax. */
export function joinRoom(state, config, peer, roomId, role = "member") {
    if (!isValidRoom(roomId))
        return { ok: false, code: "invalid_room" };
    if (peer.rooms.has(roomId)) {
        peer.rooms.set(roomId, role); // role update on re-join
        return { ok: true };
    }
    if (peer.rooms.size >= config.maxRoomsPerPeer)
        return { ok: false, code: "invalid_room" };
    let members = state.rooms.get(roomId);
    if (!members) {
        members = new Set();
        state.rooms.set(roomId, members);
    }
    if (!members.has(peer.alias) && members.size >= MAX_PEERS_PER_ROOM) {
        return { ok: false, code: "invalid_room" };
    }
    members.add(peer.alias);
    peer.rooms.set(roomId, role);
    return { ok: true };
}
/**
 * Leave a room. A peer MAY end up in zero rooms (it simply cannot send or
 * receive room messages until it joins one again). Previously the last room
 * was refused — that made "default" un-leavable, and since the hello
 * re-auto-joined it, "default" kept coming back after /mesh leave default.
 */
export function leaveRoom(state, peer, roomId) {
    if (!peer.rooms.has(roomId))
        return { ok: false, code: "not_member" };
    peer.rooms.delete(roomId);
    const members = state.rooms.get(roomId);
    if (members) {
        members.delete(peer.alias);
        if (members.size === 0)
            state.rooms.delete(roomId);
    }
    return { ok: true };
}
export function presenceFrame(alias, status, room) {
    return buildFrame({ type: "presence", from: alias, status, room });
}
/** Write a frame to every online member of a room (optionally excluding one). */
export function broadcastToRoom(state, roomId, frame, excludeAlias, write) {
    const members = state.rooms.get(roomId);
    if (!members)
        return;
    for (const alias of members) {
        if (alias === excludeAlias)
            continue;
        const peer = state.peers.get(alias);
        if (peer && write)
            write(peer, frame);
    }
}
/** Broadcast presence(online) to all rooms the peer belongs to. */
export function announceOnline(state, peer, write) {
    for (const roomId of peer.rooms.keys()) {
        broadcastToRoom(state, roomId, presenceFrame(peer.alias, "online", roomId), peer.alias, write);
    }
}
/** Detach peer from all rooms; returns room ids it belonged to. */
export function detachFromRooms(state, peer) {
    const roomIds = [...peer.rooms.keys()];
    for (const roomId of roomIds) {
        const members = state.rooms.get(roomId);
        if (members) {
            members.delete(peer.alias);
            if (members.size === 0)
                state.rooms.delete(roomId);
        }
    }
    peer.rooms.clear();
    return roomIds;
}
/** Peers visible from a set of rooms (union), for welcome/status snapshots. */
export function peersSnapshot(state, roomId) {
    const out = [];
    for (const peer of state.peers.values()) {
        if (!peer.helloDone)
            continue;
        if (roomId !== undefined && !peer.rooms.has(roomId))
            continue;
        const rooms = [...peer.rooms.keys()];
        out.push({
            alias: peer.alias,
            rooms,
            role: roomId !== undefined ? peer.rooms.get(roomId) : peer.rooms.get(DEFAULT_ROOM),
            since: new Date(peer.connectedAt).toISOString(),
            lastSeenAt: new Date(peer.lastSeenAt).toISOString(),
            reservations: peer.reservations.length > 0 ? [...peer.reservations] : undefined,
            clientVersion: peer.clientVersion,
            activity: peer.activity,
            via: peer.via,
        });
    }
    return out;
}
