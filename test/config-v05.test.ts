// test/config-v05.test.ts — v0.5 config surface: watchdog defaults, compact
// context verbosity, reservation TTL default + explicit 0 opt-out.
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, it } from "node:test";
import { loadConfig } from "../src/shared/config.js";

function tmpStateDir(): { dir: string; cleanup: () => void } {
  const dir = mkdtempSync(path.join(os.tmpdir(), "mesh-cfg-"));
  return { dir, cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

describe("loadConfig v0.5 defaults", () => {
  it("watchdog on, 2 MiB spike / 64 calls / 1 MiB compaction, compact verbosity, 6 h reservation TTL", () => {
    const { dir, cleanup } = tmpStateDir();
    try {
      const cfg = loadConfig(dir, {});
      assert.equal(cfg.watchdog, true);
      assert.equal(cfg.watchdogSpikeBytes, 2_097_152);
      assert.equal(cfg.watchdogMaxCalls, 64);
      assert.equal(cfg.watchdogCompactionBytes, 1_048_576);
      assert.equal(cfg.contextVerbosity, "compact");
      assert.equal(cfg.reservationTtlMs, 21_600_000);
    } finally {
      cleanup();
    }
  });

  it("config.json opt-outs: watchdog off, full verbosity, reservationTtlMs 0", () => {
    const { dir, cleanup } = tmpStateDir();
    try {
      writeFileSync(path.join(dir, "config.json"), JSON.stringify({
        watchdog: false,
        contextVerbosity: "full",
        reservationTtlMs: 0,
      }));
      const cfg = loadConfig(dir, {});
      assert.equal(cfg.watchdog, false);
      assert.equal(cfg.contextVerbosity, "full");
      assert.equal(cfg.reservationTtlMs, 0, "explicit 0 is a VALID opt-out, not coerced back to 6 h");
    } finally {
      cleanup();
    }
  });

  it("env overrides: MESH_WATCHDOG=0 off, MESH_CONTEXT_VERBOSE=1 full, MESH_RESERVATION_TTL_MS=0 unlimited", () => {
    const { dir, cleanup } = tmpStateDir();
    try {
      mkdirSync(dir, { recursive: true });
      const cfg = loadConfig(dir, { MESH_WATCHDOG: "0", MESH_CONTEXT_VERBOSE: "1", MESH_RESERVATION_TTL_MS: "0" });
      assert.equal(cfg.watchdog, false);
      assert.equal(cfg.contextVerbosity, "full");
      assert.equal(cfg.reservationTtlMs, 0);
    } finally {
      cleanup();
    }
  });

  it("interrupt receipts honor file opt-out and explicit environment overrides", () => {
    const { dir, cleanup } = tmpStateDir();
    try {
      assert.equal(loadConfig(dir, {}).interruptReceipts, true);
      for (const value of [false, true]) {
        writeFileSync(path.join(dir, "config.json"), JSON.stringify({ interruptReceipts: value }));
        assert.equal(loadConfig(dir, {}).interruptReceipts, value);
        assert.equal(loadConfig(dir, { MESH_INTERRUPT_RECEIPTS: "invalid" }).interruptReceipts, value);
        for (const raw of ["1", "true"]) {
          assert.equal(loadConfig(dir, { MESH_INTERRUPT_RECEIPTS: raw }).interruptReceipts, true);
        }
        for (const raw of ["0", "false"]) {
          assert.equal(loadConfig(dir, { MESH_INTERRUPT_RECEIPTS: raw }).interruptReceipts, false);
        }
      }
    } finally {
      cleanup();
    }
  });

  it("rejects fractional integer settings without erasing valid lower layers", () => {
    const { dir, cleanup } = tmpStateDir();
    try {
      const defaults = loadConfig(dir, {});
      const fields = [
        ["mailboxCap", "MESH_MAILBOX_CAP"],
        ["mailboxTtlMs", "MESH_MAILBOX_TTL_MS"],
        ["reservationTtlMs", "MESH_RESERVATION_TTL_MS"],
        ["activityIdleMs", "MESH_ACTIVITY_IDLE_MS"],
      ] as const;
      for (const [field, key] of fields) {
        for (const invalid of [0.5, 1.5, -1, Number.MAX_SAFE_INTEGER + 1]) {
          writeFileSync(path.join(dir, "config.json"), JSON.stringify({ [field]: invalid }));
          assert.equal(loadConfig(dir, {})[field], defaults[field], `${field}: ${invalid}`);
          writeFileSync(path.join(dir, "config.json"), JSON.stringify({ [field]: 123 }));
          assert.equal(loadConfig(dir, { [key]: String(invalid) })[field], 123, `${key}: ${invalid}`);
        }
        assert.equal(loadConfig(dir, { [key]: "456" })[field], 456);
      }
      writeFileSync(path.join(dir, "config.json"), JSON.stringify({ mailboxCap: 0, mailboxTtlMs: 0 }));
      const cfg = loadConfig(dir, { MESH_MAILBOX_CAP: "0", MESH_MAILBOX_TTL_MS: "0" });
      assert.equal(cfg.mailboxCap, defaults.mailboxCap);
      assert.equal(cfg.mailboxTtlMs, defaults.mailboxTtlMs);
    } finally {
      cleanup();
    }
  });
});
