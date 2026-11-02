import type { ExtensionAPI, SessionContext } from "./pi-types.js";
import type { MeshLoopControl, MeshRuntime } from "./tools.js";
export declare const DEFAULT_MESH_LOOP_MAX_RUNS = 8;
export declare const MAX_MESH_LOOP_RUNS = 50;
export declare class MeshLoopController implements MeshLoopControl {
    private readonly pi;
    private readonly getRuntime;
    private state;
    private readonly cancelledPromptContents;
    private promptSequence;
    constructor(pi: ExtensionAPI, getRuntime: () => MeshRuntime | null);
    isActive(): boolean;
    start(rt: MeshRuntime, ctx: SessionContext, maxRuns?: number, taskText?: string): {
        ok: boolean;
        message: string;
    };
    stop(reason: string): {
        active: boolean;
        message: string;
    };
    /** Drop per-session loop state without persisting or transferring it. */
    clear(): boolean;
    discard(): void;
    hasPendingPromptCancellation(): boolean;
    onAgentStart(rt: MeshRuntime, ctx: SessionContext): void;
    /** Abort a queued loop-only prompt if its loop was stopped before dispatch. */
    onBeforeAgentStart(rt: MeshRuntime, event: unknown, ctx: SessionContext): void;
    onSettled(rt: MeshRuntime, ctx: SessionContext): void;
    /** Called only after a mesh inbound message has been accepted by sendMessage. */
    onInboundInjected(rt: MeshRuntime): void;
    /** Any terminal mission settlement can unblock a parked loop. */
    onMissionsChanged(rt: MeshRuntime): void;
    private currentState;
    private continueWhenReady;
    private blocked;
    private sendPrompt;
    private clearState;
}
