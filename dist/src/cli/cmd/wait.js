// cli/cmd/wait.ts — one-shot wait_all. HONEST LIMIT: missions live in the
// memory of the process that LAUNCHED them — a fresh CLI process has none.
// The scripted path is `send --await` (synchronous); multi-mission waiting
// belongs to a persistent peer (Phase 5 attach --json).
import { say, sayErr } from "../out.js";
import { EXIT_OK, EXIT_USAGE, EXIT_EXPIRED, EXIT_PARTIAL, EXIT_MESH_FAILURE } from "../codes.js";
import { ephemeralClient } from "../ctx.js";
import { CLI_WAIT_TIMEOUT_MS } from "../../shared/config.js";
import { validateTimeoutMs } from "../validate.js";
export const WAIT_USAGE = "usage: mesh wait [--timeout MS]";
export async function cmdWait(timeoutRaw) {
    let timeout = CLI_WAIT_TIMEOUT_MS;
    if (timeoutRaw !== undefined) {
        const v = validateTimeoutMs(timeoutRaw);
        if (!v.ok) {
            sayErr(v.error);
            return EXIT_USAGE;
        }
        timeout = Number(timeoutRaw);
    }
    const client = ephemeralClient();
    try {
        await client.connect();
    }
    catch {
        say("blocked: broker_unavailable");
        await client.close();
        return EXIT_MESH_FAILURE;
    }
    const summary = await client.waitAll(timeout);
    if (summary.total === 0 && summary.status === "complete") {
        say("no awaited missions in this process — missions are memory of the sending process");
        say("(scripted path: mesh send --await; persistent missions: attach, Phase 5)");
        await client.close();
        return EXIT_OK;
    }
    say(`verdict ${summary.status}${summary.reason !== undefined ? ` (${summary.reason})` : ""} answered=${summary.answered}/${summary.total} in ${summary.elapsedMs}ms`);
    for (const a of summary.answers)
        say(`  + ${a.to}: ${a.response}`);
    for (const m of summary.missing)
        say(`  x ${m.to} (${m.msgId})${m.reason !== undefined ? ` — ${m.reason}` : ""}`);
    await client.close();
    // Cancellation is explicit; interruption preserves an incomplete batch.
    if (summary.status === "complete" || summary.status === "cancelled")
        return EXIT_OK;
    if (summary.status === "error")
        return EXIT_MESH_FAILURE;
    if (summary.status === "interrupted")
        return EXIT_PARTIAL;
    return summary.answered === 0 ? EXIT_EXPIRED : EXIT_PARTIAL;
}
