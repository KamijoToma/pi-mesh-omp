import assert from "node:assert/strict";
import { test } from "node:test";
import { InboundBatcher } from "../src/extension/batcher.js";
import { buildFrame, type MeshFrame } from "../src/protocol/envelope.js";

test("graph selection discards held old-graph messages instead of injecting them with new arrivals", () => {
  const delivered: MeshFrame[] = [];
  const batcher = new InboundBatcher(0, 0, () => true, (frames) => delivered.push(...frames));
  batcher.push(buildFrame({ type: "msg", id: "old-graph-message", from: "old-peer", body: "old graph task" }));
  batcher.clear();
  batcher.push(buildFrame({ type: "msg", id: "new-graph-message", from: "new-peer", body: "new graph task" }));
  batcher.flushNow();
  assert.deepEqual(delivered.map((frame) => frame.id), ["new-graph-message"]);
});
