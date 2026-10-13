import { MeshClient } from "../client/client.js";
import { type MeshConfig } from "../shared/config.js";
/** Client config from <cwd>/.mesh/config.json + env. */
export declare function resolveCliConfig(): Partial<MeshConfig>;
export declare function cliAlias(): string;
export interface EphemeralClientOpts {
    alias?: string;
    rooms?: string[];
}
/** Ephemeral one-shot client: fresh cli-<rand6> alias, no auto-reconnect.
 * An EXPLICIT alias is strict (D7): a collision is a hard error, never a
 * silent fallback to a random identity. */
export declare function ephemeralClient(opts?: EphemeralClientOpts): MeshClient;
