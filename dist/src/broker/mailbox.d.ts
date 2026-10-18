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
/**
 * Return the fresh queue at hello without consuming it. Entries stay queued
 * until a write is attempted; only the broker's successful write callback
 * records delivery.
 */
export declare function flushMailbox(state: BrokerState, config: MeshConfig, alias: string, now?: number): {
    entries: StoredMsg[];
    dropped: StoredMsg[];
};
/** Restore a failed write at its original age and position, honoring TTL/cap. */
export declare function requeueMailbox(state: BrokerState, config: MeshConfig, alias: string, stored: StoredMsg, now?: number): StoredMsg[];
