import type { MeshClient } from "../client/client.js";
import { type MeshConfig } from "../shared/config.js";
export interface TailnetSelection {
    meshId: string;
    service?: string;
    host?: string;
    tag?: string;
    port?: number;
}
export interface TailnetCommand {
    action: "status" | "discover" | "join" | "doctor";
    selection?: TailnetSelection;
}
export interface TailnetCommandResult {
    ok: boolean;
    text: string;
    details: Record<string, unknown>;
    /** Internal result for a live extension; never serialized to CLI stdout. */
    config?: MeshConfig;
}
export declare function executeTailnetCommand(command: TailnetCommand, config: MeshConfig, stateDirectory: string, runtimeDirectory: string, client?: MeshClient, env?: NodeJS.ProcessEnv, isCurrent?: () => boolean): Promise<TailnetCommandResult>;
