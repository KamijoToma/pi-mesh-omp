import type { ExtensionAPI, SessionContext } from "./pi-types.js";
import type { MeshHostTui } from "./tui.js";
export interface MeshExtensionInstance {
    readonly reloadApiVersion: 2;
    readonly version: string;
    isReloadSafe(): boolean;
    suspend(): Promise<unknown>;
    restore(ctx: SessionContext, state: unknown): Promise<void>;
    /** Resume inbound delivery only after this instance is the published generation. */
    activate(): void;
    dispose(): Promise<void>;
}
export type MeshExtensionFactory = (api: ExtensionAPI, host: MeshHostTui) => MeshExtensionInstance;
/** Install stable host entrypoints once; all implementation callbacks stay generation-local. */
export declare function installReloadableMesh(api: ExtensionAPI, factory: MeshExtensionFactory, entryUrl: string, host: MeshHostTui): void;
