import { buildFrame, type FileReservation, type MeshErrorCode, type MeshRole } from "../protocol/envelope.js";
import { type MeshConfig } from "../shared/config.js";
import type { BrokerState, PeerRecord } from "./state.js";
export type RoomResult = {
    ok: true;
} | {
    ok: false;
    code: MeshErrorCode;
};
/** Join a room (creating it). Enforces caps and roomId syntax. */
export declare function joinRoom(state: BrokerState, config: MeshConfig, peer: PeerRecord, roomId: string, role?: MeshRole): RoomResult;
/**
 * Leave a room. A peer MAY end up in zero rooms (it simply cannot send or
 * receive room messages until it joins one again). Previously the last room
 * was refused — that made "default" un-leavable, and since the hello
 * re-auto-joined it, "default" kept coming back after /mesh leave default.
 */
export declare function leaveRoom(state: BrokerState, peer: PeerRecord, roomId: string): RoomResult;
export declare function presenceFrame(alias: string, status: "online" | "offline", room: string): ReturnType<typeof buildFrame>;
/** Write a frame to every online member of a room (optionally excluding one). */
export declare function broadcastToRoom(state: BrokerState, roomId: string, frame: ReturnType<typeof buildFrame>, excludeAlias?: string, write?: (peer: PeerRecord, frame: ReturnType<typeof buildFrame>) => void): void;
/** Broadcast presence(online) to all rooms the peer belongs to. */
export declare function announceOnline(state: BrokerState, peer: PeerRecord, write: (peer: PeerRecord, frame: ReturnType<typeof buildFrame>) => void): void;
/** Detach peer from all rooms; returns room ids it belonged to. */
export declare function detachFromRooms(state: BrokerState, peer: PeerRecord): string[];
/** Peers visible from a set of rooms (union), for welcome/status snapshots. */
export declare function peersSnapshot(state: BrokerState, roomId?: string): {
    alias: string;
    rooms: string[];
    role?: MeshRole;
    since?: string;
    lastSeenAt?: string;
    reservations?: FileReservation[];
    clientVersion?: string;
    activity?: {
        state: "busy" | "idle" | "rate_limited" | "blocked";
        at: string;
    };
    via?: string;
}[];
