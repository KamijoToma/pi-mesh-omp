import { batchDetails, buildBatchMessage } from "./batcher.js";
import { localTime } from "./inbound.js";
/** Human-facing preview cap for /mesh inbox lines (documentation, not a protocol bound). */
const PREVIEW_MAX_CHARS = 120;
export class DeferredInbox {
    pi;
    formatFor;
    changed;
    pending = [];
    queued = [];
    draining = false;
    constructor(pi, formatFor, changed) {
        this.pi = pi;
        this.formatFor = formatFor;
        this.changed = changed;
    }
    get count() {
        return this.pending.length + this.queued.length;
    }
    snapshotForReload() {
        return { pending: [...this.pending], queued: [...this.queued], draining: this.draining };
    }
    restoreAfterReload(state) {
        this.pending = [...state.pending];
        this.queued = [...state.queued];
        this.draining = state.draining;
        this.changed(this.count);
    }
    push(frame) {
        this.pending.push(frame);
        this.changed(this.count);
    }
    list() {
        const frames = [...this.queued, ...this.pending];
        if (frames.length === 0)
            return "mesh: deferred inbox empty";
        return `mesh: deferred ${frames.length}\n` + frames.map((f) => `@${f.from ?? "?"} ${localTime(f.ts)} ${(f.body ?? "").replace(/\s+/g, " ").slice(0, PREVIEW_MAX_CHARS)}`).join("\n");
    }
    send(frames, immediate) {
        const batch = buildBatchMessage(frames, this.formatFor);
        const content = batch.content.replace(/^\[mesh batch[^\n]*\]/, `[mesh deferred — ${frames.length} broadcast(s) not addressed to you]`);
        this.pi.sendMessage({
            customType: "mesh-inbound",
            content,
            display: true,
            details: batchDetails(frames),
        }, immediate ? { deliverAs: "followUp", triggerTurn: true } : { deliverAs: "nextTurn" });
    }
    /** Queue at input, BEFORE Pi drains nextTurn. Holding locally until then
     *  lets /mesh inbox flush promote the batch without duplicate injections:
     *  Pi's public API cannot remove an already-queued nextTurn message. */
    queueForPrompt() {
        if (this.pending.length === 0)
            return;
        this.send(this.pending, false);
        this.queued.push(...this.pending);
        this.pending = [];
    }
    /** Only user-prompt preflight drains nextTurn; mesh-triggered agent_start
     *  does not. Do not clear the inbox just because a direct message woke us. */
    promptStarting() {
        this.draining = true;
    }
    agentStarting() {
        if (!this.draining)
            return;
        this.draining = false;
        this.queued = [];
        this.changed(this.count);
    }
    flush() {
        const count = this.pending.length;
        if (count === 0)
            return 0;
        this.send(this.pending, true);
        this.pending = [];
        this.changed(this.count);
        return count;
    }
    clear() {
        this.pending = [];
        this.queued = [];
        this.draining = false;
        this.changed(0);
    }
}
