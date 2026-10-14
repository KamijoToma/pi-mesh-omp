import type { BrokerInfo, MeshPeerInfo } from "../protocol/envelope.js";
import { type MeshConfig } from "../shared/config.js";
import { type ResolvedConnection } from "./adapter.js";
export interface ConnectionDiagnostics {
    resolved: boolean;
    reachable: boolean;
    authenticated: boolean;
    target?: ResolvedConnection;
    broker?: BrokerInfo;
    peers: MeshPeerInfo[];
    error?: string;
}
/** A socket probe is not authentication; diagnostics never start a local broker. */
export declare function diagnoseConnection(config: MeshConfig, runtimeDirectory: string): Promise<ConnectionDiagnostics>;
