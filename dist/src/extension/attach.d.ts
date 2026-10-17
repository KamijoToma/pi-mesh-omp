import type { MeshClient } from "../client/client.js";
import type { MeshFrame } from "../protocol/envelope.js";
import { type DeferredInboxReloadState } from "./deferred-inbox.js";
import type { MeshHud } from "./hud.js";
import { type FormatOpts } from "./inbound.js";
import { ReplyHintTracker, type ReplyHintReloadState } from "./reply-hints.js";
import type { ExtensionAPI, SessionContext } from "./pi-types.js";
import type { MeshRuntime } from "./tools.js";
export interface MeshInboundReloadState {
    deferred: DeferredInboxReloadState;
    held: {
        frame: MeshFrame;
        matchedReply: boolean;
    }[];
    batched: MeshFrame[];
    fullContextSent: boolean;
    lastPeersOnline: string[];
    replyHints: ReplyHintReloadState;
    liveCooldowns: [string, number][];
}
/** Format opts for one inbound frame (compact mode + gated hints). */
export declare function formatOptsFor(frame: MeshFrame, verbose: boolean, homeRoom: string, hints: ReplyHintTracker): FormatOpts;
/** Compact reconnect diff line (~30 tokens vs ~500 for the full block). */
export declare function buildReconnectDiff(prev: readonly string[], next: readonly string[]): string;
/** Public, process-local identity bridge for other extensions. */
export declare function exposeAlias(pi: ExtensionAPI, alias: string, rooms: readonly string[]): void;
/**
 * Attach every client event handler to `client`. `rt` is the CURRENT runtime
 * object (its `.client` field may be replaced later by /mesh reset — the
 * handlers always read the runtime they were attached with, so the reset path
 * re-attaches on the new client).
 */
export declare function attachClientListeners(pi: ExtensionAPI, rt: MeshRuntime, getHud: () => MeshHud | null, ctx: SessionContext, client: MeshClient, saveIdentity: (rt: MeshRuntime) => void, 
/** min gap between two live entries of the SAME agent (anti-flood). */
liveCooldownMs?: number, isCurrent?: () => boolean, restored?: MeshInboundReloadState): void;
