import type { MeshExtensionFactory } from "./reload.js";
export interface ReloadSource {
    revision: string;
    factory: MeshExtensionFactory;
    dispose(): void;
}
/** Read the same built bytes used for a reload, without creating a snapshot. */
export declare function readReloadRevision(entryUrl: string): string;
export declare function loadReloadSource(entryUrl: string): Promise<ReloadSource>;
