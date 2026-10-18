// broker/mailbox.ts — offline queue per alias: cap, TTL, flush.
// Every function that drops entries RETURNS them so the broker can send
// honest drop notices to the original senders — a message that leaves the
// mailbox without being delivered must never vanish silently.
import type { MeshFrame } from "../protocol/envelope.js";
import type { MeshConfig } from "../shared/config.js";
import type { BrokerState, StoredMsg } from "./state.js";

/** Split one alias queue at the TTL boundary (mutates state.mailbox). */
function partitionExpired(
  state: BrokerState,
  alias: string,
  ttlMs: number,
  now: number,
): { fresh: StoredMsg[]; dropped: StoredMsg[] } {
  const queue = state.mailbox.get(alias) ?? [];
  const fresh: StoredMsg[] = [];
  const dropped: StoredMsg[] = [];
  for (const m of queue) {
    if (now - m.enqueuedAt <= ttlMs) fresh.push(m);
    else dropped.push(m);
  }
  if (dropped.length > 0) state.stats.mailboxDropped += dropped.length;
  if (fresh.length === 0) state.mailbox.delete(alias);
  else state.mailbox.set(alias, fresh);
  return { fresh, dropped };
}

/**
 * Enqueue for a known offline alias. At cap, the oldest is dropped.
 * Returns every entry dropped by TTL or cap so the caller can notify
 * the senders.
 */
export function enqueueMailbox(
  state: BrokerState,
  config: MeshConfig,
  alias: string,
  frame: MeshFrame,
  now: number = Date.now(),
): StoredMsg[] {
  const { fresh, dropped } = partitionExpired(state, alias, config.mailboxTtlMs, now);
  fresh.push({ frame, enqueuedAt: now });
  while (fresh.length > config.mailboxCap) {
    const evicted = fresh.shift();
    if (evicted === undefined) break;
    dropped.push(evicted);
    state.stats.mailboxDropped += 1;
  }
  state.mailbox.set(alias, fresh);
  return dropped;
}

/** Periodic TTL purge across all aliases. Returns everything dropped. */
export function purgeAllExpired(
  state: BrokerState,
  config: MeshConfig,
  now: number = Date.now(),
): StoredMsg[] {
  const dropped: StoredMsg[] = [];
  for (const alias of [...state.mailbox.keys()]) {
    dropped.push(...partitionExpired(state, alias, config.mailboxTtlMs, now).dropped);
  }
  return dropped;
}

/**
 * Return the fresh queue at hello without consuming it. Entries stay queued
 * until a write is attempted; only the broker's successful write callback
 * records delivery.
 */
export function flushMailbox(
  state: BrokerState,
  config: MeshConfig,
  alias: string,
  now: number = Date.now(),
): { entries: StoredMsg[]; dropped: StoredMsg[] } {
  const { fresh, dropped } = partitionExpired(state, alias, config.mailboxTtlMs, now);
  return { entries: fresh, dropped };
}

/** Restore a failed write at its original age and position, honoring TTL/cap. */
export function requeueMailbox(
  state: BrokerState,
  config: MeshConfig,
  alias: string,
  stored: StoredMsg,
  now: number = Date.now(),
): StoredMsg[] {
  const queue = state.mailbox.get(alias) ?? [];
  const next = queue.findIndex((entry) => entry.enqueuedAt >= stored.enqueuedAt);
  queue.splice(next < 0 ? queue.length : next, 0, stored);
  state.mailbox.set(alias, queue);
  const { fresh, dropped } = partitionExpired(state, alias, config.mailboxTtlMs, now);
  while (fresh.length > config.mailboxCap) {
    const evicted = fresh.shift();
    if (evicted === undefined) break;
    dropped.push(evicted);
    state.stats.mailboxDropped += 1;
  }
  if (fresh.length === 0) state.mailbox.delete(alias);
  else state.mailbox.set(alias, fresh);
  return dropped;
}
