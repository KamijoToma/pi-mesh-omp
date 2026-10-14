import { type MeshConfig, type MeshEndpoint, type TailscaleConfig } from "../shared/config.js";
export interface TailnetNode {
    id: string;
    hostname: string;
    dnsName: string;
    addresses: string[];
    online: boolean;
    tags: string[];
}
export interface TailnetStatus {
    backendState: string;
    self: TailnetNode | null;
    peers: TailnetNode[];
    magicDnsEnabled: boolean;
    dnsSuffix: string;
}
export interface TailnetService {
    name: string;
    hostname: string;
    addresses: string[];
    ports: number[];
}
export interface ResolvedConnection {
    endpoint: MeshEndpoint | null;
    url: string | null;
    source: "local" | "explicit" | "tailscale-service" | "tailscale-node";
    meshId?: string;
    node?: TailnetNode;
    service?: TailnetService;
}
export declare function clearTailnetDiscoveryCache(): void;
export declare function readTailnetStatus(config?: TailscaleConfig): Promise<TailnetStatus>;
export declare function listTailnetServices(config?: TailscaleConfig): Promise<TailnetService[]>;
export declare function resolveConnection(config: MeshConfig, options?: {
    refresh?: boolean;
}): Promise<ResolvedConnection>;
