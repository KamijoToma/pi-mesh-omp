// extension/ledger.ts — hash-only append-only ledger <stateDir>/ledger.jsonl.
// body-free durable: never a message body, only hashes + metadata.
// fix: callers write `delivered` ONLY after the broker ack (enforced by wiring).
// recursive forbidden-key scan before EVERY append → throw fail-closed.
import { appendFileSync, existsSync, mkdirSync, readFileSync, renameSync, statSync } from "node:fs";
import path from "node:path";
import { hasForbiddenPersistedKey } from "../protocol/envelope.js";
import { nowIso } from "../protocol/frames.js";
import { DEFAULT_LEDGER_MAX_BYTES } from "../shared/config.js";
import { LEDGER_FILE_NAME } from "../shared/paths.js";
export const LEDGER_SCHEMA = "mesh.ledger.v1";
export class MeshLedger {
    stateDir;
    maxBytes;
    constructor(stateDir, maxBytes = DEFAULT_LEDGER_MAX_BYTES) {
        this.stateDir = stateDir;
        this.maxBytes = maxBytes;
    }
    get path() {
        return path.join(this.stateDir, LEDGER_FILE_NAME);
    }
    /**
    * Append one record. Fail-closed: recursive scan of forbidden persisted
    * keys BEFORE writing — a violation throws and zero bytes are appended.
    */
    append(input) {
        const record = {
            schema: LEDGER_SCHEMA,
            ...input,
            ts: nowIso(),
            bodyStored: false,
        };
        if (hasForbiddenPersistedKey(record)) {
            throw new Error("mesh ledger fail-closed: forbidden persisted key (E25)");
        }
        mkdirSync(this.stateDir, { recursive: true });
        this.rotateIfNeeded();
        appendFileSync(this.path, JSON.stringify(record) + "\n", "utf8");
        return record;
    }
    /**
    * read the last `limit` records, optionally filtered (hash-only — the
    * ledger never contains bodies,. For agents: durable mesh history.
    */
    read(limit = 20, filter) {
        let content;
        try {
            content = readFileSync(this.path, "utf8");
        }
        catch {
            return [];
        }
        const out = [];
        for (const line of content.split("\n")) {
            if (line.trim() === "")
                continue;
            try {
                const r = JSON.parse(line);
                if (r.schema !== LEDGER_SCHEMA)
                    continue;
                if (filter?.from !== undefined && r.from !== filter.from)
                    continue;
                if (filter?.to !== undefined && r.to !== filter.to)
                    continue;
                if (filter?.room !== undefined && r.room !== filter.room)
                    continue;
                if (filter?.event !== undefined && r.event !== filter.event)
                    continue;
                out.push(r);
            }
            catch {
                // skip malformed
            }
        }
        return out.slice(-limit);
    }
    /** Simple rotation: when ledger.jsonl exceeds maxBytes → ledger-<date>.jsonl.1. */
    rotateIfNeeded() {
        let size = 0;
        try {
            size = statSync(this.path).size;
        }
        catch {
            return; // no file yet
        }
        if (size <= this.maxBytes)
            return;
        const date = new Date().toISOString().slice(0, 10);
        // ledger-<date>.jsonl.N — N increments so repeated rotations never overwrite
        let n = 1;
        while (existsSync(path.join(this.stateDir, `ledger-${date}.jsonl.${n}`)))
            n += 1;
        renameSync(this.path, path.join(this.stateDir, `ledger-${date}.jsonl.${n}`));
    }
}
