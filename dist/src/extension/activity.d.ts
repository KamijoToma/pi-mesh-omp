import type { SessionContext } from "./pi-types.js";
export type MeshActivityState = "busy" | "idle" | "rate_limited" | "blocked";
export interface ActivityDependencies {
    context(): SessionContext | null;
    isCurrent(): boolean;
    announce(state: MeshActivityState): void;
    holdUntil(until: number | undefined): void;
    flushHeld(): void;
    observeModel: boolean;
}
/** One controller per session. Only an active error hold owns background timers. */
export declare class MeshActivity {
    #private;
    constructor(deps: ActivityDependencies);
    beginRun(): void;
    toolCalled(): void;
    failed(state: "rate_limited" | "blocked"): void;
    checkModel(): void;
    successfulTurn(): void;
    settled(): void;
    recovered(): void;
    dispose(): void;
}
