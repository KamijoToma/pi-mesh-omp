// test/drop-notice.test.ts — end-to-end honest drop receipt: a queued
// message that later leaves the recipient's offline mailbox (TTL) settles
// the sender's live awaitReply mission immediately and fires an event the
// extension can surface, instead of burning the whole mission timeout.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { MeshClient } from "../src/client/client.js";
import type { MeshFrame } from "../src/protocol/envelope.js";
import { brokerSocketPathOf, makeTempDirs, RawClient, startTestBroker, sleep, waitFor, type TempDirs } from "./helpers.js";
import type { RunningBroker } from "../src/broker/broker.js";

describe("drop notice: client settles a live mission when the mailbox drops the message", () => {
  let dirs: TempDirs;
  let broker: RunningBroker;
  let alice: MeshClient;

  before(async () => {
    dirs = makeTempDirs("mesh-dropnotice-");
    broker = await startTestBroker(dirs.runtimeDir, { config: { mailboxTtlMs: 200 } });
    alice = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir });
    await alice.connect();

    // bob becomes a known alias, then goes away (his mailbox is now eligible)
    const bob = await RawClient.connect(brokerSocketPathOf(dirs.runtimeDir));
    bob.hello("bob");
    await bob.waitFrame((f) => f.type === "welcome");
    bob.close();
    await sleep(150);
  });

  after(async () => {
    await alice.close();
    await broker.close();
    dirs.cleanup();
  });

  it("launch-mode awaitReply mission → dropped_offline event + mission failed, not waiting", async () => {
    const sent = await alice.send({
      to: "bob",
      message: "answer me later",
      awaitReply: true,
      block: false,
    });
    assert.equal(sent.status, "queued_offline");
    assert.equal(alice.pendingCount, 1); // mission live in the background

    const noticed = new Promise<MeshFrame>((resolve) => {
      alice.once("dropped_offline", resolve);
    });

    await sleep(300); // past the 200 ms mailbox TTL
    broker.purgeMailboxExpired(); // deterministic purge

    const frame = await noticed;
    assert.equal(frame.id, sent.msgId); // correlates the exact message
    assert.equal(frame.to, "bob");

    // the mission settled NOW — no 30-min timeout burn for a dead letter
    assert.equal(alice.pendingCount, 0);
    const mission = alice.missionStatus().find((m) => m.msgId === sent.msgId);
    assert.ok(mission !== undefined);
    assert.equal(mission.status, "failed");
    assert.equal(broker.state.stats.mailboxDropped, 1);
  });
});

it("mailbox envelope growth explicitly fails the live mission while the recipient stays online", async (t) => {
  const dirs = makeTempDirs("mesh-mailbox-growth-");
  t.after(() => dirs.cleanup());
  const broker = await startTestBroker(dirs.runtimeDir);
  t.after(() => broker.close());
  const alice = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir });
  t.after(() => alice.close());
  await alice.connect();
  const bob1 = await RawClient.connect(brokerSocketPathOf(dirs.runtimeDir));
  bob1.hello("bob");
  await bob1.waitFrame((f) => f.type === "welcome");
  bob1.close();
  await waitFor(() => !broker.state.peers.has("bob"));

  const sent = await alice.send({
    to: "bob", message: "\\".repeat(32629), awaitReply: true, block: false,
  });
  assert.equal(sent.status, "queued_offline");
  assert.equal(alice.pendingCount, 1);
  let noticed: MeshFrame | undefined;
  alice.once("dropped_offline", (frame: MeshFrame) => { noticed = frame; });
  const bob2 = await RawClient.connect(brokerSocketPathOf(dirs.runtimeDir));
  t.after(() => bob2.close());
  bob2.hello("bob");
  await bob2.waitFrame((f) => f.type === "welcome");
  const drop = await waitFor(() => noticed);
  assert.equal(drop.id, sent.msgId);
  assert.equal(alice.pendingCount, 0);
  assert.equal(alice.missionStatus().find((m) => m.msgId === sent.msgId)?.status, "failed");
  assert.equal(broker.state.stats.mailboxDelivered, 0);
  assert.equal(broker.state.stats.mailboxDropped, 1);
  bob2.send({ type: "ping", from: "bob", id: "m_after_drop" });
  await bob2.waitFrame((f) => f.type === "pong" && f.id === "m_after_drop");
  assert.equal(bob2.frames.some((f) => f.type === "mailbox"), false);
});
