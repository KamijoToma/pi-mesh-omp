import { MeshClient } from "../client/client.js";
import { connectProbe, connectProbeTcp } from "../client/reconnect.js";
import { STATUS_REQ_TIMEOUT_MS } from "../shared/config.js";
import { brokerSocketPath } from "../shared/paths.js";
import { resolveConnection } from "./adapter.js";
/** A socket probe is not authentication; diagnostics never start a local broker. */
export async function diagnoseConnection(config, runtimeDirectory) {
    const result = { resolved: false, reachable: false, authenticated: false, peers: [] };
    try {
        result.target = await resolveConnection(config, { refresh: true });
        result.resolved = true;
    }
    catch (error) {
        result.error = error instanceof Error ? error.message : String(error);
        return result;
    }
    const endpoint = result.target.endpoint;
    const socketPath = endpoint?.kind === "unix" ? endpoint.path : brokerSocketPath(runtimeDirectory);
    result.reachable = endpoint === null || endpoint.kind === "unix"
        ? await connectProbe(socketPath, STATUS_REQ_TIMEOUT_MS)
        : await connectProbeTcp(endpoint.host, endpoint.port, STATUS_REQ_TIMEOUT_MS);
    if (!result.reachable) {
        result.error = "broker_unreachable: TCP/socket connection failed; check the listener, tailnet grants, routes and host approval";
        return result;
    }
    const client = new MeshClient({
        runtimeDir: runtimeDirectory,
        noReconnect: true,
        config: { ...config, brokerUrl: result.target.url ?? `unix://${socketPath}` },
    });
    try {
        const welcome = await client.connect();
        result.authenticated = true;
        result.broker = welcome.broker;
        result.peers = welcome.peers.filter((peer) => peer.alias !== client.alias);
    }
    catch (error) {
        result.error = error instanceof Error ? error.message : String(error);
    }
    finally {
        await client.close();
    }
    return result;
}
