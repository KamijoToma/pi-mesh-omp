import { type Server } from "node:net";
import { type MeshConfig } from "../shared/config.js";
import { type MeshPolicy } from "./policy.js";
import { BrokerState } from "./state.js";
export interface BrokerOptions {
    config: MeshConfig;
    policy: MeshPolicy;
    socketPath?: string;
    /** TCP/TLS listen (host+port) — otherwise the unix socket is used. */
    tcpListen?: {
        host: string;
        port: number;
        tls: boolean;
    };
    /** With tcpListen: ALSO keep serving the local unix socket (tokenless)
    *  alongside TCP/TLS — local sessions keep working unchanged while
    *  remote machines join over the network. */
    alsoUnix?: boolean;
}
export interface RunningBroker {
    server: Server;
    state: BrokerState;
    socketPath: string;
    close: () => Promise<void>;
    /** Run one mailbox TTL purge NOW and send drop notices (the interval
     *  does the same every MAILBOX_PURGE_INTERVAL_MS; this hook makes the
     *  behavior testable/deterministic). */
    purgeMailboxExpired: (now?: number) => void;
}
export declare function createBroker(options: BrokerOptions): Promise<RunningBroker>;
/** prune stale known aliases — no live peer, no mailbox, and the alias
*  has not re-hello'd in 24 h (mailbox eligibility only). Exported for tests. */
export declare function pruneStaleKnownAliases(state: BrokerState, now?: number, maxAgeMs?: number): void;
/** Purge tables + presence(offline) to former room members. */
export declare function closePeer(state: BrokerState, alias: string, _reason: string, maxFrameBytes?: number): void;
/** Acquire lock, allowing only the launching parent's explicit PID handoff. */
export declare function acquireLock(dir: string, starterPid?: number): boolean;
export declare function main(): Promise<void>;
