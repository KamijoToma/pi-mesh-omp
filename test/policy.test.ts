// test/policy.test.ts — wildcards, deny precedence, forceAllowedFrom,
// rateLimits override merge (D11, §12). Broker-level E13/E14 in broker.test.ts.
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { describe, it } from "node:test";
import {
  DEFAULT_POLICY,
  evaluatePolicy,
  loadPolicy,
} from "../src/broker/policy.js";
import { checkRate, DEFAULT_RATE_LIMITS } from "../src/broker/ratelimit.js";
import { BrokerState } from "../src/broker/state.js";
import { makeTempDirs } from "./helpers.js";

describe("policy: evaluatePolicy", () => {
  const base = { from: "alice", to: "bob", room: "default", priority: "normal" as const };

  it("default policy allows everything", () => {
    assert.deepEqual(evaluatePolicy(DEFAULT_POLICY, base), { action: "allow" });
  });

  it("wildcard '*' matches; prefix 'foo*' matches prefix; else exact", () => {
    const policy = {
      ...DEFAULT_POLICY,
      allow: [{ from: "lead-*", to: "*", room: "ops*" }],
      deny: [],
    };
    assert.equal(
      evaluatePolicy(policy, { from: "lead-1", to: "bob", room: "ops", priority: "normal" }).action,
      "allow",
    );
    assert.equal(
      evaluatePolicy(policy, { from: "alice", to: "bob", room: "ops", priority: "normal" }).action,
      "deny",
    );
    assert.equal(
      evaluatePolicy(policy, { from: "lead-1", to: "bob", room: "dev", priority: "normal" }).action,
      "deny",
    );
  });

  it("deny takes precedence over allow", () => {
    const policy = {
      ...DEFAULT_POLICY,
      allow: [{ from: "*", to: "*", room: "*" }],
      deny: [{ from: "observer-*", to: "*" }],
    };
    const denied = evaluatePolicy(policy, { from: "observer-1", to: "bob", room: "default", priority: "normal" });
    assert.deepEqual(denied, { action: "deny", code: "policy_denied" });
    assert.equal(evaluatePolicy(policy, base).action, "allow");
  });

  it("E14 primitive: force from alias not in forceAllowedFrom → policy_denied", () => {
    const policy = { ...DEFAULT_POLICY, forceAllowedFrom: ["lead"] };
    const denied = evaluatePolicy(policy, { from: "alice", to: "bob", room: "default", priority: "force" });
    assert.deepEqual(denied, { action: "deny", code: "policy_denied" });
    assert.equal(
      evaluatePolicy(policy, { from: "lead", to: "bob", room: "default", priority: "force" }).action,
      "allow",
    );
  });

  it("forceDowngrade: unauthorized force → downgrade instead of deny", () => {
    const policy = { ...DEFAULT_POLICY, forceAllowedFrom: ["lead"], forceDowngrade: true };
    assert.equal(
      evaluatePolicy(policy, { from: "alice", to: "bob", room: "default", priority: "force" }).action,
      "downgrade",
    );
    assert.equal(
      evaluatePolicy(policy, { from: "lead", to: "bob", room: "default", priority: "force" }).action,
      "allow",
    );
  });

  it("empty forceAllowedFrom allows force for nobody", () => {
    const denied = evaluatePolicy(DEFAULT_POLICY, { from: "lead", to: "bob", room: "default", priority: "force" });
    assert.deepEqual(denied, { action: "deny", code: "policy_denied" });
  });
});

describe("policy: loadPolicy", () => {
  it("missing file → permissive default", (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const p = loadPolicy(dirs.stateDir, {});
    assert.deepEqual(p.allow, [{ from: "*", to: "*", room: "*" }]);
    assert.deepEqual(p.deny, []);
    assert.deepEqual(p.rateLimits, DEFAULT_RATE_LIMITS);
  });

  it("invalid JSON → permissive default", (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    writeFileSync(path.join(dirs.stateDir, "policy.json"), "{not json", "utf8");
    const p = loadPolicy(dirs.stateDir, {});
    assert.deepEqual(p.allow, [{ from: "*", to: "*", room: "*" }]);
  });

  it("rateLimits override merges over defaults", (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    writeFileSync(
      path.join(dirs.stateDir, "policy.json"),
      JSON.stringify({ rateLimits: { msgPerMin: 3 } }),
      "utf8",
    );
    const p = loadPolicy(dirs.stateDir, {});
    assert.equal(p.rateLimits.msgPerMin, 3);
    assert.equal(p.rateLimits.urgentPerMin, DEFAULT_RATE_LIMITS.urgentPerMin);
    assert.equal(p.rateLimits.forcePerMin, DEFAULT_RATE_LIMITS.forcePerMin);
  });

  it("invalid fractional limits fall back and never reject the first message", (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    for (const invalid of [0.5, 1.5, 0, -1, "3", null, Number.MAX_SAFE_INTEGER + 1]) {
      writeFileSync(path.join(dirs.stateDir, "policy.json"), JSON.stringify({
        rateLimits: { msgPerMin: invalid, urgentPerMin: invalid, forcePerMin: invalid },
      }));
      const policy = loadPolicy(dirs.stateDir, {});
      assert.deepEqual(policy.rateLimits, DEFAULT_RATE_LIMITS);
      for (const kind of ["msg", "urgent", "force"] as const) {
        assert.equal(checkRate(new BrokerState(), "alice", kind, policy.rateLimits), true, `${kind}: ${invalid}`);
      }
    }
  });

  it("valid integer policy limits bound actual sends", (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    writeFileSync(path.join(dirs.stateDir, "policy.json"), JSON.stringify({
      rateLimits: { msgPerMin: 1 },
    }));
    const policy = loadPolicy(dirs.stateDir, {});
    const state = new BrokerState();
    const now = Date.now();
    assert.equal(checkRate(state, "alice", "msg", policy.rateLimits, now), true);
    assert.equal(checkRate(state, "alice", "msg", policy.rateLimits, now), false);
  });

  it("parses allow/deny/forceAllowedFrom/forceDowngrade from file", (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    writeFileSync(
      path.join(dirs.stateDir, "policy.json"),
      JSON.stringify({
        allow: [{ from: "lead-*", to: "*" }],
        deny: [{ from: "bad", to: "*" }],
        forceAllowedFrom: ["lead-1"],
        forceDowngrade: true,
      }),
      "utf8",
    );
    const p = loadPolicy(dirs.stateDir, {});
    assert.deepEqual(p.allow, [{ from: "lead-*", to: "*", room: undefined }]);
    assert.deepEqual(p.deny, [{ from: "bad", to: "*", room: undefined }]);
    assert.deepEqual(p.forceAllowedFrom, ["lead-1"]);
    assert.equal(p.forceDowngrade, true);
  });

  it("MESH_POLICY env override wins over stateDir", (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const custom = path.join(dirs.stateDir, "custom-policy.json");
    writeFileSync(custom, JSON.stringify({ deny: [{ from: "x-*", to: "*" }] }), "utf8");
    const p = loadPolicy(undefined, { MESH_POLICY: custom });
    assert.deepEqual(p.deny, [{ from: "x-*", to: "*", room: undefined }]);
  });
});
