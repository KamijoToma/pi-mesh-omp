// protocol/frames.ts — NDJSON framing, sha256, ids, clock. No Pi imports.
import { createHash, randomBytes } from "node:crypto";
import { MSG_ID_RAND_CHARS } from "../shared/config.js";
export class FrameSizeError extends Error {
    name = "FrameSizeError";
    constructor(message) {
        super(message);
    }
}
export function sha256(data) {
    return createHash("sha256").update(data, "utf8").digest("hex");
}
export function nowIso() {
    return new Date().toISOString();
}
/** m_<time36>_<rand8hex>. */
export function makeMsgId(rand = randomBytes) {
    const time36 = Date.now().toString(36);
    const randHex = rand(MSG_ID_RAND_CHARS / 2).toString("hex").slice(0, MSG_ID_RAND_CHARS);
    return `m_${time36}_${randHex}`;
}
/** Encode one frame as a NDJSON line. Throws FrameSizeError over maxBytes. */
export function encodeFrame(value, maxBytes) {
    const line = JSON.stringify(value) + "\n";
    const buf = Buffer.from(line, "utf8");
    if (buf.byteLength > maxBytes) {
        throw new FrameSizeError(`frame ${buf.byteLength}B exceeds max ${maxBytes}B`);
    }
    return buf;
}
/**
 * Incremental NDJSON decoder with chunk reassembly.
 * Throws FrameSizeError when a single line (or the partial buffer) exceeds maxBytes.
 */
export class FrameDecoder {
    maxBytes;
    buf = "";
    bufBytes = 0;
    constructor(maxBytes) {
        this.maxBytes = maxBytes;
    }
    /** Feed a chunk; returns complete lines (without trailing \n). */
    push(chunk) {
        const text = typeof chunk === "string" ? chunk : chunk.toString("utf8");
        this.buf += text;
        this.bufBytes = Buffer.byteLength(this.buf, "utf8");
        const lines = [];
        let idx;
        while ((idx = this.buf.indexOf("\n")) !== -1) {
            const line = this.buf.slice(0, idx);
            const rest = this.buf.slice(idx + 1);
            this.buf = rest;
            this.bufBytes = Buffer.byteLength(rest, "utf8");
            if (Buffer.byteLength(line, "utf8") > this.maxBytes) {
                throw new FrameSizeError(`frame line exceeds max ${this.maxBytes}B`);
            }
            if (line.length > 0)
                lines.push(line);
        }
        if (this.bufBytes > this.maxBytes) {
            throw new FrameSizeError(`partial frame exceeds max ${this.maxBytes}B`);
        }
        return lines;
    }
}
