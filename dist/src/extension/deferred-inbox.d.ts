import type { MeshFrame } from "../protocol/envelope.js";
import { type FormatOpts } from "./inbound.js";
import type { ExtensionAPI } from "./pi-types.js";
export interface DeferredInboxReloadState {
    pending: MeshFrame[];
    queued: MeshFrame[];
    draining: boolean;
}
export declare class DeferredInbox {
    private readonly pi;
    private readonly formatFor;
    private readonly changed;
    /** Called once per accepted host batch, never again during prompt drain. */
    private readonly onInjected;
    private pending;
    private queued;
    private draining;
    constructor(pi: Pick<ExtensionAPI, "sendMessage">, formatFor: (frame: MeshFrame) => FormatOpts, changed: (count: number) => void, 
    /** Called once per accepted host batch, never again during prompt drain. */
    onInjected: (frames: readonly MeshFrame[]) => void);
    get count(): number;
    snapshotForReload(): DeferredInboxReloadState;
    restoreAfterReload(state: DeferredInboxReloadState): void;
    push(frame: MeshFrame): void;
    list(): string;
    private send;
    /** Queue at input, BEFORE Pi drains nextTurn. Holding locally until then
     *  lets /mesh inbox flush promote the batch without duplicate injections:
     *  Pi's public API cannot remove an already-queued nextTurn message. */
    queueForPrompt(): void;
    /** Only user-prompt preflight drains nextTurn; mesh-triggered agent_start
     *  does not. Do not clear the inbox just because a direct message woke us. */
    promptStarting(): void;
    agentStarting(): void;
    flush(): number;
    clear(): void;
}
