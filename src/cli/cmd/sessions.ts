// cli/cmd/sessions.ts — mesh sessions: inventory of the PERSISTED identities
// in the CURRENT stateDir (adoption targets for `attach`). READ-ONLY: the
// CLI never creates, moves or deletes identity files (extension territory).
// Scoping is honest: only <cwd>/.mesh or MESH_STATE_DIR is visible — a
// session killed in ANOTHER project does not appear (same scope as ledger).
import { say, sayErr, jsonLine } from "../out.js";
import { EXIT_OK, EXIT_PARTIAL } from "../codes.js";
import { ephemeralClient } from "../ctx.js";
import { MeshIdentity, type ListedIdentity } from "../../shared/identity-store.js";
import { stateDir } from "../../shared/paths.js";

export const SESSIONS_USAGE = "usage: mesh sessions [--json]";

export async function cmdSessions(asJson: boolean): Promise<number> {
  const dir = stateDir();
  const { identities, skipped } = new MeshIdentity(dir).list();
  for (const name of skipped) sayErr(`skipping unusable ${name} (corrupt or foreign version)`);

  // Persisted inventory stays available when live presence cannot be queried.
  let onlineAliases: Set<string> | undefined;
  const client = ephemeralClient();
  try {
    await client.connect();
    const snap = await client.status();
    onlineAliases = new Set(snap.peers.map((p) => p.alias));
  } catch (error) {
    sayErr(`live status unavailable: ${error instanceof Error ? error.message : String(error)}; presence is unknown`);
  } finally {
    await client.close();
  }

  if (asJson) {
    for (const id of identities) {
      jsonLine({ sessionId: id.sessionId, alias: id.alias, rooms: id.rooms, reservations: id.reservations.length, updatedAt: id.updatedAt, online: onlineAliases === undefined ? null : onlineAliases.has(id.alias) });
    }
    return onlineAliases === undefined ? EXIT_PARTIAL : EXIT_OK;
  }

  if (identities.length === 0) {
    say(`(no persisted identities in ${dir})`);
    return onlineAliases === undefined ? EXIT_PARTIAL : EXIT_OK;
  }
  say(`identities in ${dir} (scope: this stateDir only):`);
  for (const id of identities) {
    const online = onlineAliases === undefined ? "unknown" : onlineAliases.has(id.alias) ? "online" : "offline";
    say(`  ${id.alias}\tsession=${id.sessionId.slice(0, 8)}…\trooms=${id.rooms.join(",") || "-"}\treservations=${id.reservations.length}\t${online}\tupdated=${id.updatedAt}`);
  }
  say("adopt with: mesh attach <alias>  |  mesh attach --session <id>");
  return onlineAliases === undefined ? EXIT_PARTIAL : EXIT_OK;
}
