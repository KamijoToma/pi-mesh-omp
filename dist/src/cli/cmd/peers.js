// cli/cmd/peers.ts — live broker snapshot, one line per peer.
import { say, sayErr } from "../out.js";
import { EXIT_OK, EXIT_MESH_FAILURE } from "../codes.js";
import { ephemeralClient } from "../ctx.js";
export async function cmdPeers(room) {
    const client = ephemeralClient();
    let snap;
    try {
        await client.connect();
        snap = await client.status(room);
    }
    catch (error) {
        sayErr(`error: ${error instanceof Error ? error.message : String(error)}`);
        await client.close();
        return EXIT_MESH_FAILURE;
    }
    for (const p of snap.peers) {
        const v = p.clientVersion !== undefined && p.clientVersion !== "" ? `v${p.clientVersion}` : "v?";
        say(`${p.alias}\trooms=${p.rooms.join(",")}\tv=${v}\tsince=${p.since ?? "?"}`);
    }
    if (snap.stats !== undefined) {
        const s = snap.stats;
        say(`broker: relayed=${s.relayed} refused=${s.refused} mailboxDelivered=${s.mailboxDelivered} mailboxDropped=${s.mailboxDropped}`);
    }
    await client.close();
    return EXIT_OK;
}
