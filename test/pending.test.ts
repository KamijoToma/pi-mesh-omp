// test/pending.test.ts — strict replyTo correlation, expiry timing, remind cap
// 2, late reply counted, cancelAll closed (D8, E7/E8/E23 primitives).
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { PendingReplies } from "../src/client/pending.js";
import { buildFrame } from "../src/protocol/envelope.js";
import { MAX_REMINDS } from "../src/shared/config.js";
import { sleep, withKeepAlive } from "./helpers.js";

describe("pending: correlation & resolution", () => {
  it("strict replyTo === msgId correlation resolves with the reply frame", async () => {
    const reminds: string[] = [];
    const p = new PendingReplies((id) => reminds.push(id));
    const promise = p.register("m_aaa_11111111", Date.now() + 2000);
    const reply = buildFrame({ type: "reply", from: "bob", to: "alice", replyTo: "m_aaa_11111111", body: "ok" });
    const wrong = buildFrame({ type: "reply", from: "bob", to: "alice", replyTo: "m_bbb_22222222", body: "stray" });
    assert.equal(p.handleReply(wrong), false); // wrong replyTo → ignored
    assert.equal(p.unmatchedReplyCount, 1);
    assert.equal(p.handleReply(reply), true);
    const res = await promise;
    assert.equal(res.kind, "reply");
    assert.equal(res.frame?.body, "ok");
  });

  it("expiry is terminal at expiresAt", async () => {
    const p = new PendingReplies(() => {});
    const t0 = Date.now();
    const res = await withKeepAlive(p.register("m_exp_11111111", t0 + 120));
    assert.equal(res.kind, "expired");
    assert.ok(Date.now() - t0 >= 110, "expires at/after deadline");
  });

  it("reminds fire ≤ 2 (MAX_REMINDS) and stop after resolution", async () => {
    const reminds: string[] = [];
    const p = new PendingReplies((id) => reminds.push(id));
    const res = await withKeepAlive(p.register("m_rem_11111111", Date.now() + 160));
    assert.equal(res.kind, "expired");
    assert.equal(reminds.length, MAX_REMINDS); // T/2 and 3T/4
    assert.equal(reminds.length, 2);
    await sleep(100); // no further reminds after expiry
    assert.equal(reminds.length, 2);
  });

  it("E8 primitive: late reply after expiry is ignored + counted", async () => {
    const p = new PendingReplies(() => {});
    const res = await withKeepAlive(p.register("m_late_11111111", Date.now() + 60));
    assert.equal(res.kind, "expired");
    const late = buildFrame({ type: "reply", from: "bob", to: "alice", replyTo: "m_late_11111111", body: "late" });
    assert.equal(p.handleReply(late), false);
    assert.equal(p.unmatchedReplyCount, 1);
  });

  it("E23 primitive: cancelAll resolves every pending with closed", async () => {
    const p = new PendingReplies(() => {});
    const r1 = p.register("m_s1_11111111", Date.now() + 5000);
    const r2 = p.register("m_s2_11111111", Date.now() + 5000);
    p.cancelAll("closed");
    const [res1, res2] = await Promise.all([r1, r2]);
    assert.equal(res1.kind, "error");
    assert.equal(res1.reason, "closed");
    assert.equal(res2.kind, "error");
    assert.equal(res2.reason, "closed");
    assert.equal(p.size, 0);
  });

  it("re-registering the same msgId supersedes the previous pending", async () => {
    const p = new PendingReplies(() => {});
    const first = p.register("m_dup_11111111", Date.now() + 5000);
    const second = p.register("m_dup_11111111", Date.now() + 5000);
    p.cancelAll("done");
    const [r1, r2] = await Promise.all([first, second]);
    assert.equal(r1.kind, "error");
    assert.equal(r1.reason, "superseded");
    assert.equal(r2.kind, "error");
    assert.equal(r2.reason, "done");
  });

  it("launch flag: isLaunch reports live LAUNCH missions only", async () => {
    const p = new PendingReplies(() => {});
    const blocking = p.register("m_blk_11111111", Date.now() + 5000);
    const launched = p.register("m_lau_11111111", Date.now() + 5000, true);
    assert.equal(p.isLaunch("m_blk_11111111"), false, "default register is blocking");
    assert.equal(p.isLaunch("m_lau_11111111"), true, "register(..., true) is launch");
    assert.equal(p.isLaunch("m_unknown"), false, "unknown msgId is never launch");
    // consuming the reply clears the flag (handleReply consumed the entry)
    assert.equal(p.handleReply(buildFrame({ type: "reply", from: "bob", to: "alice", replyTo: "m_lau_11111111", body: "ok" })), true);
    assert.equal(p.isLaunch("m_lau_11111111"), false);
    assert.equal((await launched).kind, "reply");
    p.cancel("m_blk_11111111", "done");
    await blocking;
  });
});

describe("pending: N3 — no instant reminds at timeout 0", () => {
  it("timeout 0 → zero reminds and immediate expiry", async () => {
    const reminds: string[] = [];
    const p = new PendingReplies((id) => reminds.push(id));
    const res = await withKeepAlive(p.register("m_zero_11111111", Date.now())); // total clamps to 0
    assert.equal(res.kind, "expired");
    await sleep(80);
    assert.equal(reminds.length, 0, "no remind may fire at delay 0");
    assert.equal(p.size, 0);
  });

  it("past deadline → zero reminds and immediate expiry", async () => {
    const reminds: string[] = [];
    const p = new PendingReplies((id) => reminds.push(id));
    const res = await withKeepAlive(p.register("m_past_11111111", Date.now() - 500));
    assert.equal(res.kind, "expired");
    await sleep(80);
    assert.equal(reminds.length, 0);
    assert.equal(p.size, 0);
  });
});
