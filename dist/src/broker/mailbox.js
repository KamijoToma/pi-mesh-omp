/** Split one alias queue at the TTL boundary (mutates state.mailbox). */
function partitionExpired(state, alias, ttlMs, now) {
    const queue = state.mailbox.get(alias) ?? [];
    const fresh = [];
    const dropped = [];
    for (const m of queue) {
        if (now - m.enqueuedAt <= ttlMs)
            fresh.push(m);
        else
            dropped.push(m);
    }
    if (dropped.length > 0)
        state.stats.mailboxDropped += dropped.length;
    if (fresh.length === 0)
        state.mailbox.delete(alias);
    else
        state.mailbox.set(alias, fresh);
    return { fresh, dropped };
}
/**
 * Enqueue for a known offline alias. At cap, the oldest is dropped.
 * Returns every entry dropped by TTL or cap so the caller can notify
 * the senders.
 */
export function enqueueMailbox(state, config, alias, frame, now = Date.now()) {
    const { fresh, dropped } = partitionExpired(state, alias, config.mailboxTtlMs, now);
    fresh.push({ frame, enqueuedAt: now });
    while (fresh.length > config.mailboxCap) {
        const evicted = fresh.shift();
        if (evicted === undefined)
            break;
        dropped.push(evicted);
        state.stats.mailboxDropped += 1;
    }
    state.mailbox.set(alias, fresh);
    return dropped;
}
/** Periodic TTL purge across all aliases. Returns everything dropped. */
export function purgeAllExpired(state, config, now = Date.now()) {
    const dropped = [];
    for (const alias of [...state.mailbox.keys()]) {
        dropped.push(...partitionExpired(state, alias, config.mailboxTtlMs, now).dropped);
    }
    return dropped;
}
export function mailboxSize(state, config, alias) {
    return partitionExpired(state, alias, config.mailboxTtlMs, Date.now()).fresh.length;
}
/**
 * Drain the mailbox at hello: returns stored frames (as `mailbox` frames with
 * queuedAt) in enqueue order, clears the queue, and surfaces any entries
 * that expired while the owner was away (for drop notices).
 */
export function flushMailbox(state, config, alias, now = Date.now()) {
    const { fresh, dropped } = partitionExpired(state, alias, config.mailboxTtlMs, now);
    state.mailbox.delete(alias);
    state.stats.mailboxDelivered += fresh.length;
    return {
        frames: fresh.map((stored) => ({
            ...stored.frame,
            type: "mailbox",
            queuedAt: new Date(stored.enqueuedAt).toISOString(),
        })),
        dropped,
    };
}
