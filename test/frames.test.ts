// test/frames.test.ts — NDJSON framing: chunked reassembly, >maxBytes throws,
// UTF-8 multibyte at chunk boundary, multiple frames in one chunk (§6.1, D17).
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { buildFrame, parseFrameLine } from "../src/protocol/envelope.js";
import {
  encodeFrame,
  FrameDecoder,
  FrameSizeError,
  makeMsgId,
  sha256,
} from "../src/protocol/frames.js";

const MAX = 1024;

describe("frames: encode/decode roundtrip", () => {
  it("encodeFrame → decoder → parseFrameLine roundtrip", () => {
    const f = buildFrame({ type: "msg", from: "alice", to: "bob", body: "hello ✓" });
    const buf = encodeFrame(f, MAX);
    const dec = new FrameDecoder(MAX);
    const lines = dec.push(buf);
    assert.equal(lines.length, 1);
    const parsed = parseFrameLine(lines[0]!);
    assert.equal(parsed.ok, true);
    if (parsed.ok) assert.equal(parsed.frame.id, f.id);
  });

  it("encodeFrame throws FrameSizeError over maxBytes", () => {
    const f = buildFrame({ type: "msg", from: "alice", to: "bob", body: "x".repeat(MAX) });
    assert.throws(() => encodeFrame(f, MAX), FrameSizeError);
  });
});

describe("frames: chunked reassembly", () => {
  it("frame split across two chunks reassembles", () => {
    const f = buildFrame({ type: "ping", from: "alice" });
    const buf = encodeFrame(f, MAX);
    const mid = Math.floor(buf.length / 2);
    const dec = new FrameDecoder(MAX);
    assert.deepEqual(dec.push(buf.subarray(0, mid)), []);
    const lines = dec.push(buf.subarray(mid));
    assert.equal(lines.length, 1);
    const parsed = parseFrameLine(lines[0]!);
    assert.equal(parsed.ok, true);
  });

  it("multiple frames in one chunk all decode", () => {
    const f1 = buildFrame({ type: "ping", from: "alice" });
    const f2 = buildFrame({ type: "pong", from: "broker" });
    const f3 = buildFrame({ type: "msg", from: "alice", to: "bob", body: "third" });
    const dec = new FrameDecoder(MAX);
    const lines = dec.push(Buffer.concat([encodeFrame(f1, MAX), encodeFrame(f2, MAX), encodeFrame(f3, MAX)]));
    assert.equal(lines.length, 3);
    const parsed = lines.map((l) => parseFrameLine(l));
    assert.ok(parsed.every((p) => p.ok));
    if (parsed[2]!.ok) assert.equal(parsed[2]!.frame.body, "third");
  });

  it("empty lines are skipped", () => {
    const dec = new FrameDecoder(MAX);
    const f = buildFrame({ type: "ping", from: "alice" });
    const lines = dec.push(Buffer.concat([Buffer.from("\n\n"), encodeFrame(f, MAX)]));
    assert.equal(lines.length, 1);
  });
});

describe("frames: size bounds", () => {
  it("complete line > maxBytes throws FrameSizeError", () => {
    const dec = new FrameDecoder(64);
    const line = JSON.stringify({ v: 1, padding: "x".repeat(100) }) + "\n";
    assert.throws(() => dec.push(Buffer.from(line, "utf8")), FrameSizeError);
  });

  it("partial buffer > maxBytes throws FrameSizeError (no newline needed)", () => {
    const dec = new FrameDecoder(64);
    assert.throws(() => dec.push(Buffer.from("x".repeat(65), "utf8")), FrameSizeError);
  });

  it("counts incomplete multibyte bytes toward the partial frame limit", () => {
    const dec = new FrameDecoder(2);
    const bytes = Buffer.from("😀");
    assert.deepEqual(dec.push(bytes.subarray(0, 2)), []);
    assert.throws(() => dec.push(bytes.subarray(2, 3)), FrameSizeError);
  });
});

describe("frames: UTF-8 multibyte at chunk boundary", () => {
  it("preserves exact body and hash at every byte split inside UTF-8 characters", () => {
    const body = "héllo—✓—münchen 😀 中文";
    const f = buildFrame({ type: "msg", from: "alice", to: "bob", body });
    const buf = encodeFrame(f, MAX);
    const bodyStart = buf.indexOf(Buffer.from(body));
    assert.ok(bodyStart >= 0);
    for (let split = bodyStart + 1; split < bodyStart + Buffer.byteLength(body); split += 1) {
      if ((buf[split]! & 0xc0) !== 0x80) continue;
      const dec = new FrameDecoder(MAX);
      assert.deepEqual(dec.push(buf.subarray(0, split)), []);
      const lines = dec.push(buf.subarray(split));
      assert.equal(lines.length, 1);
      const parsed = parseFrameLine(lines[0]!);
      assert.equal(parsed.ok, true, `split at byte ${split}`);
      if (parsed.ok) {
        assert.equal(parsed.frame.body, body);
        assert.equal(parsed.frame.bodyHash, sha256(body));
      }
    }
  });
});

describe("frames: ids and hashes", () => {
  it("makeMsgId matches m_<time36>_<rand8hex>", () => {
    const id = makeMsgId();
    assert.match(id, /^m_[0-9a-z]+_[0-9a-f]{8}$/);
    assert.notEqual(makeMsgId(), makeMsgId());
  });

  it("sha256 known vector", () => {
    assert.equal(
      sha256("abc"),
      "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
    );
  });
});
