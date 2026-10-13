import type { MeshFrame } from "../protocol/envelope.js";
import type { MeshConfig } from "../shared/config.js";
import type { BrokerState, StoredMsg } from "./state.js";
/**
 * Enqueue for a known offline alias. At cap, the oldest is dropped.
 * Returns every entry dropped by TTL or cap so the caller can notify
 * the senders.
 */
export declare function enqueueMailbox(state: BrokerState, config: MeshConfig, alias: string, frame: MeshFrame, now?: number): StoredMsg[];
/** Periodic TTL purge across all aliases. Returns everything dropped. */
export declare function purgeAllExpired(state: BrokerState, config: MeshConfig, now?: number): StoredMsg[];
export declare function mailboxSize(state: BrokerState, config: MeshConfig, alias: string): number;
/**
 * Drain the mailbox at hello: returns stored frames (as `mailbox` frames with
 * queuedAt) in enqueue order, clears the queue, and surfaces any entries
 * that expired while the owner was away (for drop notices).
 */
export declare function flushMailbox(state: BrokerState, config: MeshConfig, alias: string, now?: number): {
    frames: MeshFrame[];
    dropped: StoredMsg[];
};
