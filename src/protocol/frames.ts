// protocol/frames.ts — NDJSON framing, sha256, ids, clock. No Pi imports.
import { createHash, randomBytes } from "node:crypto";
import { StringDecoder } from "node:string_decoder";
import { MSG_ID_RAND_CHARS } from "../shared/config.js";

export class FrameSizeError extends Error {
  override readonly name = "FrameSizeError";
  constructor(message: string) {
    super(message);
  }
}

export function sha256(data: string): string {
  return createHash("sha256").update(data, "utf8").digest("hex");
}

export function nowIso(): string {
  return new Date().toISOString();
}

/** m_<time36>_<rand8hex>. */
export function makeMsgId(rand: (size: number) => Buffer = randomBytes): string {
  const time36 = Date.now().toString(36);
  const randHex = rand(MSG_ID_RAND_CHARS / 2).toString("hex").slice(0, MSG_ID_RAND_CHARS);
  return `m_${time36}_${randHex}`;
}

/** Encode one frame as a NDJSON line. Throws FrameSizeError over maxBytes. */
export function encodeFrame(value: unknown, maxBytes: number): Buffer {
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
  private buf = "";
  private bufBytes = 0;
  private readonly decoder = new StringDecoder("utf8");

  constructor(private readonly maxBytes: number) {}

  /** Feed a chunk; returns complete lines (without trailing \n). */
  push(chunk: Buffer | string): string[] {
    const bytes = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk;
    const lines: string[] = [];
    let start = 0;
    while (start < bytes.length) {
      const newline = bytes.indexOf(0x0a, start);
      const end = newline === -1 ? bytes.length : newline;
      this.bufBytes += end - start;
      if (this.bufBytes > this.maxBytes) {
        throw new FrameSizeError(`${newline === -1 ? "partial frame" : "frame line"} exceeds max ${this.maxBytes}B`);
      }
      // Include the newline in decoding so any incomplete UTF-8 sequence
      // is flushed at the line boundary, never into the next frame.
      this.buf += this.decoder.write(bytes.subarray(start, newline === -1 ? end : end + 1));
      if (newline === -1) break;
      const line = this.buf.slice(0, -1);
      if (line.length > 0) lines.push(line);
      this.buf = "";
      this.bufBytes = 0;
      start = end + 1;
    }
    return lines;
  }
}
