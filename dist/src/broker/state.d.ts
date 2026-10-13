import type { Socket } from "node:net";
import type { MeshFrame, MeshRole, FileReservation } from "../protocol/envelope.js";
export interface PeerRecord {
    alias: string;
    socket: Socket;
    rooms: Map<string, MeshRole>;
    pid?: number;
    clientVersion?: string;
    connectedAt: number;
    lastSeenAt: number;
    helloDone: boolean;
    /** File reservations declared by the peer. Live with the connection. */
    reservations: FileReservation[];
    /** Phase 3: last announced turn state (busy = a turn is running). */
    activity?: {
        state: "busy" | "idle" | "rate_limited" | "blocked";
        at: string;
    };
    /** D40: connection origin — "tcp:<remote-ip>" / "tls:<remote-ip>" for
    *  network peers, undefined for broker-local unix-socket peers. */
    via?: string;
}
export interface StoredMsg {
    frame: MeshFrame;
    enqueuedAt: number;
}
export interface TokenBucket {
    tokens: number;
    lastRefillAt: number;
}
export interface PeerRates {
    msg: TokenBucket;
    urgent: TokenBucket;
    force: TokenBucket;
}
export interface BrokerStats {
    startedAt: number;
    relayed: number;
    refused: number;
    mailboxDelivered: number;
    mailboxDropped: number;
}
/** Broker state: 100% memory, rebuilt by re-hello after restart. */
export declare class BrokerState {
    readonly peers: Map<string, PeerRecord>;
    readonly rooms: Map<string, Set<string>>;
    readonly mailbox: Map<string, StoredMsg[]>;
    readonly rates: Map<string, PeerRates>;
    /** Aliases seen at least once since broker start (mailbox eligibility).
    *  Map<alias, lastHelloAt> — stale aliases are pruned by the sweep
    *  (no live peer, no mailbox, unseen > 24 h). */
    readonly knownAliases: Map<string, number>;
    readonly stats: BrokerStats;
}
export declare function newBucket(tokens: number): TokenBucket;
