import type { ExtensionAPI, SessionContext } from "./pi-types.js";
export interface HostLifecycleHandlers {
    switchSession(ctx: SessionContext, handoff: boolean): Promise<void>;
    settled(ctx: SessionContext): void;
    modelSelected(ctx: SessionContext): void;
    beforeFork(): void;
}
/** Omp injects its public SDK facade; Pi does not. No process-name guessing. */
export declare function isOmpHost(pi: ExtensionAPI): boolean;
export declare function registerHostLifecycle(pi: ExtensionAPI, handlers: HostLifecycleHandlers): void;
/** Return a cancellation closure, keeping host timer handle types inside the boundary. */
export declare function scheduleHostTimeout(ctx: SessionContext | null, callback: () => void, ms: number): () => void;
export declare function currentModelKey(ctx: SessionContext | null): string | undefined;
/** Startup remains recoverable, but never disappears behind a silent catch. */
export declare function reportConnectionError(pi: ExtensionAPI, ctx: SessionContext | null, error: unknown): void;
