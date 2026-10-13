/** Broker state: 100% memory, rebuilt by re-hello after restart. */
export class BrokerState {
    peers = new Map();
    rooms = new Map();
    mailbox = new Map();
    rates = new Map();
    /** Aliases seen at least once since broker start (mailbox eligibility).
    *  Map<alias, lastHelloAt> — stale aliases are pruned by the sweep
    *  (no live peer, no mailbox, unseen > 24 h). */
    knownAliases = new Map();
    stats = {
        startedAt: Date.now(),
        relayed: 0,
        refused: 0,
        mailboxDelivered: 0,
        mailboxDropped: 0,
    };
}
export function newBucket(tokens) {
    return { tokens, lastRefillAt: Date.now() };
}
