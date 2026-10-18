// test/reply-targets.test.ts — sender-designated reply targets (replyTo):
// the sender of a msg can choose WHO receives the reply (single or several),
// defaulting to the sender. The recipient's plain mesh_reply fans out to all
// designated targets; an explicit `to`/`replyAll` still overrides.
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { rmSync } from "node:fs";
import { MeshClient } from "../src/client/client.js";
import type { MeshFrame } from "../src/protocol/envelope.js";
import { makeTempDirs, startTestBroker, waitFor, type TempDirs } from "./helpers.js";

describe("sender-designated reply targets (replyTo)", () => {
  let dirs: TempDirs;
  let broker: Awaited<ReturnType<typeof startTestBroker>>;
  let lead: MeshClient;
  let bob: MeshClient;
  let carol: MeshClient;
  let dave: MeshClient;
  const inboxes = new Map<string, MeshFrame[]>();

  const collect = (client: MeshClient, alias: string): void => {
    inboxes.set(alias, []);
    client.on("inbound", (f: MeshFrame) => {
      inboxes.get(alias)?.push(f);
    });
  };
  const framesOf = (alias: string, type: string): MeshFrame[] =>
    (inboxes.get(alias) ?? []).filter((f) => f.type === type);

  before(async () => {
    dirs = makeTempDirs("mesh-replytargets-");
    broker = await startTestBroker(dirs.runtimeDir);
    lead = new MeshClient({ alias: "lead", runtimeDir: dirs.runtimeDir });
    bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir });
    carol = new MeshClient({ alias: "carol", runtimeDir: dirs.runtimeDir });
    dave = new MeshClient({ alias: "dave", runtimeDir: dirs.runtimeDir });
    await Promise.all([lead.connect(), bob.connect(), carol.connect(), dave.connect()]);
    collect(lead, "lead");
    collect(bob, "bob");
    collect(carol, "carol");
    collect(dave, "dave");
  });

  after(async () => {
    await lead.close().catch(() => {});
    await bob.close().catch(() => {});
    await carol.close().catch(() => {});
    await dave.close().catch(() => {});
    await broker.close();
    rmSync(dirs.root, { recursive: true, force: true });
  });

  it("send with replyTo carries replyTargets on the msg frame", async () => {
    const res = await lead.send({ to: "bob", message: "answer carol", replyTo: ["carol", "dave"] });
    assert.equal(res.status, "delivered");
    await waitFor(() => (framesOf("bob", "msg").length > 0 ? true : undefined));
    const msg = framesOf("bob", "msg").at(-1);
    assert.ok(msg !== undefined);
    assert.deepEqual(msg.replyTargets, ["carol", "dave"]);
  });

  it("a plain reply goes to ALL designated targets, NOT the sender", async () => {
    const msg = framesOf("bob", "msg").at(-1);
    assert.ok(msg !== undefined);
    const res = await bob.reply(msg.id, "for carol and dave");
    assert.equal(res.status, "delivered");
    assert.equal(res.deliveredCount, 2);
    assert.equal(res.totalCount, 2);
    await waitFor(() => (framesOf("carol", "reply").length > 0 ? true : undefined));
    await waitFor(() => (framesOf("dave", "reply").length > 0 ? true : undefined));
    assert.equal(framesOf("carol", "reply").at(-1)?.body, "for carol and dave");
    assert.equal(framesOf("dave", "reply").at(-1)?.body, "for carol and dave");
    assert.equal(framesOf("lead", "reply").length, 0, "sender must NOT receive the reply");
  });

  it("an explicit `to` overrides the designated targets", async () => {
    const res = await lead.send({ to: "bob", message: "answer lead instead", replyTo: ["carol"] });
    assert.equal(res.status, "delivered");
    await waitFor(() => (framesOf("bob", "msg").length >= 2 ? true : undefined));
    const msg = framesOf("bob", "msg").at(-1);
    assert.ok(msg !== undefined);
    const carolBefore = framesOf("carol", "reply").length;
    const r = await bob.reply(msg.id, "back to lead", { to: "lead" });
    assert.equal(r.status, "delivered");
    await waitFor(() => (framesOf("lead", "reply").length > 0 ? true : undefined));
    assert.equal(framesOf("lead", "reply").at(-1)?.body, "back to lead");
    assert.equal(
      framesOf("carol", "reply").length,
      carolBefore,
      "designated target must NOT get the overridden reply",
    );
  });

  it("replyAll still fans out to the whole room (ignores replyTargets)", async () => {
    const res = await lead.send({ to: "bob", message: "answer everyone", replyTo: ["carol"] });
    assert.equal(res.status, "delivered");
    await waitFor(() => (framesOf("bob", "msg").length >= 3 ? true : undefined));
    const msg = framesOf("bob", "msg").at(-1);
    assert.ok(msg !== undefined);
    const r = await bob.reply(msg.id, "to the room", { replyAll: true });
    assert.equal(r.status, "delivered");
    await waitFor(() => (framesOf("lead", "reply").length >= 1 ? true : undefined));
    assert.ok(framesOf("lead", "reply").some((f) => f.body === "to the room"));
    assert.ok(framesOf("carol", "reply").some((f) => f.body === "to the room"));
    assert.ok(framesOf("dave", "reply").some((f) => f.body === "to the room"));
  });

  it("without replyTo the reply still goes to the sender (default)", async () => {
    const res = await lead.send({ to: "bob", message: "plain question" });
    assert.equal(res.status, "delivered");
    await waitFor(() => (framesOf("bob", "msg").length >= 4 ? true : undefined));
    const msg = framesOf("bob", "msg").at(-1);
    assert.ok(msg !== undefined);
    assert.equal(msg.replyTargets, undefined);
    const r = await bob.reply(msg.id, "plain answer");
    assert.equal(r.status, "delivered");
    await waitFor(() => (framesOf("lead", "reply").some((f) => f.body === "plain answer") ? true : undefined));
  });

  it("invalid replyTo is refused locally", async () => {
    const bad = await lead.send({ to: "bob", message: "x", replyTo: ["bad alias!"] });
    assert.equal(bad.status, "error");
    assert.equal(bad.reason, "invalid_alias");
    const empty = await lead.send({ to: "bob", message: "x", replyTo: [] });
    assert.equal(empty.status, "error");
    assert.match(empty.reason ?? "", /replyTo/);
    const tooMany = await lead.send({
      to: "bob",
      message: "x",
      replyTo: ["a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9"],
    });
    assert.equal(tooMany.status, "error");
    assert.match(tooMany.reason ?? "", /replyTo/);
  });

  it("a single string replyTo is accepted (normalized)", async () => {
    const res = await lead.send({ to: "bob", message: "single target", replyTo: "@carol" });
    assert.equal(res.status, "delivered");
    await waitFor(() => (framesOf("bob", "msg").length >= 5 ? true : undefined));
    const msg = framesOf("bob", "msg").at(-1);
    assert.ok(msg !== undefined);
    assert.deepEqual(msg.replyTargets, ["carol"]);
  });

  it("counts distinct offline targets and never mailboxes replies", async () => {
    await dave.close();
    await waitFor(() => broker.state.peers.has("dave") ? undefined : true);
    const sent = await lead.send({
      to: "bob", message: "one target offline", replyTo: ["carol", "dave", "carol", "bob"],
    });
    assert.equal(sent.status, "delivered");
    await waitFor(() => framesOf("bob", "msg").some((f) => f.id === sent.msgId) ? true : undefined);
    const partial = await bob.reply(sent.msgId!, "only carol online");
    assert.equal(partial.status, "delivered");
    assert.equal(partial.deliveredCount, 1);
    assert.equal(partial.totalCount, 2, "duplicates and the replier are excluded, offline dave is included");
    await waitFor(() => framesOf("carol", "reply").some((f) => f.body === "only carol online") ? true : undefined);

    await carol.close();
    await waitFor(() => broker.state.peers.has("carol") ? undefined : true);
    const allOffline = await bob.reply(sent.msgId!, "no targets online");
    assert.equal(allOffline.status, "error");
    assert.equal(allOffline.reason, "dropped_offline");
    await Promise.all([carol.connect(), dave.connect()]);
    const carolStatus = await carol.status();
    const daveStatus = await dave.status();
    assert.ok(carolStatus.peers.some((p) => p.alias === "dave"));
    assert.ok(daveStatus.peers.some((p) => p.alias === "carol"));
    assert.equal(framesOf("dave", "reply").some((f) => f.body === "only carol online"), false);
    for (const alias of ["carol", "dave"]) {
      assert.equal(framesOf(alias, "reply").some((f) => f.body === "no targets online"), false);
    }
  });
});
