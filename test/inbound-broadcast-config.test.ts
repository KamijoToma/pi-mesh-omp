import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, it } from "node:test";
import { loadConfig } from "../src/shared/config.js";

describe("inboundBroadcasts config", () => {
  it("defaults < config.json < env; invalid values preserve the lower layer", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "mesh-policy-cfg-"));
    try {
      assert.equal(loadConfig(dir, {}).inboundBroadcasts, "immediate");
      for (const value of ["immediate", "deferred"]) {
        writeFileSync(path.join(dir, "config.json"), JSON.stringify({ inboundBroadcasts: value }));
        assert.equal(loadConfig(dir, {}).inboundBroadcasts, value);
        assert.equal(loadConfig(dir, { MESH_INBOUND_BROADCASTS: "invalid" }).inboundBroadcasts, value);
        for (const override of ["immediate", "deferred"]) {
          assert.equal(loadConfig(dir, { MESH_INBOUND_BROADCASTS: override }).inboundBroadcasts, override);
        }
      }
      for (const invalid of [true, 1, null, "DEFERRED", "invalid"]) {
        writeFileSync(path.join(dir, "config.json"), JSON.stringify({ inboundBroadcasts: invalid }));
        assert.equal(loadConfig(dir, {}).inboundBroadcasts, "immediate");
        assert.equal(loadConfig(dir, { MESH_INBOUND_BROADCASTS: "deferred" }).inboundBroadcasts, "deferred");
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("batch windows accept only integers and max hold env overrides preserve valid file values", () => {
    const dir = mkdtempSync(path.join(os.tmpdir(), "mesh-batch-cfg-"));
    try {
      const defaults = loadConfig(dir, {});
      for (const invalid of [0.5, 1.5, -1, Number.MAX_SAFE_INTEGER + 1]) {
        writeFileSync(path.join(dir, "config.json"), JSON.stringify({
          inboundBatchMs: invalid, inboundBatchMaxHoldMs: invalid,
        }));
        assert.equal(loadConfig(dir, {}).inboundBatchMs, defaults.inboundBatchMs);
        assert.equal(loadConfig(dir, {}).inboundBatchMaxHoldMs, defaults.inboundBatchMaxHoldMs);
      }
      writeFileSync(path.join(dir, "config.json"), JSON.stringify({
        inboundBatchMs: 123, inboundBatchMaxHoldMs: 456,
      }));
      for (const raw of ["0.5", "1.5", "-1", "invalid", "", " ", "Infinity", "9007199254740992"]) {
        const cfg = loadConfig(dir, {
          MESH_INBOUND_BATCH_MS: raw, MESH_INBOUND_BATCH_MAX_HOLD_MS: raw,
        });
        assert.equal(cfg.inboundBatchMs, 123, raw);
        assert.equal(cfg.inboundBatchMaxHoldMs, 456, raw);
      }
      const overridden = loadConfig(dir, {
        MESH_INBOUND_BATCH_MS: "0", MESH_INBOUND_BATCH_MAX_HOLD_MS: "789",
      });
      assert.equal(overridden.inboundBatchMs, 0);
      assert.equal(overridden.inboundBatchMaxHoldMs, 789);
      assert.equal(loadConfig(dir, { MESH_INBOUND_BATCH_MAX_HOLD_MS: "0" }).inboundBatchMaxHoldMs, 456);
      writeFileSync(path.join(dir, "config.json"), JSON.stringify({
        inboundBatchMs: 0, inboundBatchMaxHoldMs: 0,
      }));
      assert.equal(loadConfig(dir, {}).inboundBatchMs, 0);
      assert.equal(loadConfig(dir, {}).inboundBatchMaxHoldMs, defaults.inboundBatchMaxHoldMs);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
