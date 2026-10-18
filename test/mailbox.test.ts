// test/mailbox.test.ts — E5 (queued_offline + delivery at hello + body intact),
// E6 (cap drop oldest), TTL expiry, delivery order (D9, §7.7).
import assert from "node:assert/strict";
import { once } from "node:events";
import net, { type Socket } from "node:net";
import { describe, it } from "node:test";
import { buildFrame } from "../src/protocol/envelope.js";
import { encodeFrame } from "../src/protocol/frames.js";
import {
  brokerSocketPathOf,
  makeTempDirs,
  RawClient,
  startTestBroker,
  sleep,
  waitFor,
  type TempDirs,
} from "./helpers.js";
import type { RunningBroker } from "../src/broker/broker.js";

async function setup(
  t: { after: (fn: () => void | Promise<void>) => void },
  overrides: Parameters<typeof startTestBroker>[1] = {},
): Promise<{ dirs: TempDirs; broker: RunningBroker; sock: string }> {
  const dirs = makeTempDirs();
  t.after(() => dirs.cleanup());
  const broker = await startTestBroker(dirs.runtimeDir, overrides);
  t.after(async () => {
    await broker.close();
  });
  return { dirs, broker, sock: brokerSocketPathOf(dirs.runtimeDir) };
}

describe("mailbox: offline queue", () => {
  it("E5: known offline alias → queued_offline + mailbox delivery at next hello, body intact", async (t) => {
    const { broker, sock } = await setup(t);
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");

    // bob comes online once (becomes a known alias), then disconnects
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await sleep(150); // broker observes the close

    alice.send({ type: "msg", from: "alice", to: "bob", body: "while-you-were-out" });
    const ack = await alice.waitFrame((f) => f.type === "ack");
    assert.equal(ack.status, "queued_offline");

    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const welcome = await bob2.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.mailboxCount, 1);
    const mf = await bob2.waitFrame((f) => f.type === "mailbox");
    assert.equal(mf.body, "while-you-were-out"); // body intact
    assert.equal(mf.from, "alice");
    assert.ok(typeof mf.queuedAt === "string");
    const delivered = await alice.waitFrame((f) => f.type === "ack" && f.id === mf.id && f.status === "delivered");
    assert.equal(delivered.id, ack.id, "delivery confirmation correlates the original queued message");
    assert.equal(broker.state.stats.mailboxDelivered, 1);

    bob2.close();
    await alice.waitFrames((f) => f.type === "presence" && f.from === "bob" && f.status === "offline", 2);
    const bob3 = await RawClient.connect(sock);
    t.after(() => bob3.close());
    bob3.hello("bob");
    const again = await bob3.waitFrame((f) => f.type === "welcome");
    assert.equal(again.mailboxCount, 0, "successfully written entries must not replay on reconnect");
    bob3.send({ type: "ping", from: "bob", id: "m_after_flush" });
    await bob3.waitFrame((f) => f.type === "pong" && f.id === "m_after_flush");
    assert.equal(bob3.frames.some((f) => f.type === "mailbox"), false);
  });

  it("a valid 65528-byte ingress that grows past the default cap is explicitly dropped without closing the recipient", async (t) => {
    const { broker, sock } = await setup(t);
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await alice.waitFrame((f) => f.type === "presence" && f.from === "bob" && f.status === "offline");
    const original = buildFrame({
      type: "msg", id: "m_mailbox_full_0001", from: "alice", to: "bob",
      room: "default", priority: "normal", body: "\\".repeat(32629),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    assert.equal(encodeFrame(original, 65536).byteLength, 65528);
    alice.sendFrame(original);
    await alice.waitFrame((f) => f.type === "ack" && f.id === original.id && f.status === "queued_offline");

    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    await bob2.waitFrame((f) => f.type === "welcome");
    const dropped = await alice.waitFrame((f) => f.type === "ack" && f.id === original.id && f.status === "dropped_offline");
    assert.equal(dropped.to, "bob");
    bob2.send({ type: "ping", from: "bob", id: "m_after_dead_letter" });
    await bob2.waitFrame((f) => f.type === "pong" && f.id === "m_after_dead_letter");
    assert.equal(bob2.frames.some((f) => f.type === "mailbox"), false);
    assert.equal(bob2.closed, false);
    assert.equal(broker.state.stats.mailboxDelivered, 0);
    assert.equal(broker.state.stats.mailboxDropped, 1);
    assert.equal(alice.frames.some((f) => f.type === "ack" && f.id === original.id && f.status === "delivered"), false);
  });

  it("a whole welcome encode failure retains the mailbox for a later successful hello", async (t) => {
    const { broker, sock } = await setup(t, { config: { maxFrameBytes: 1024 } });
    const alice = await RawClient.connect(sock, 1024);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock, 1024);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await alice.waitFrame((f) => f.type === "presence" && f.from === "bob" && f.status === "offline");
    alice.send({ type: "msg", id: "m_welcome_retry", from: "alice", to: "bob", body: "keep until hello succeeds" });
    await alice.waitFrame((f) => f.type === "ack" && f.id === "m_welcome_retry");
    alice.send({
      type: "reserve", id: "m_large_snapshot", from: "alice",
      reservations: [{ pattern: "p".repeat(400), reason: "r".repeat(200) }],
    });
    await alice.waitFrame((f) => f.type === "ack" && f.id === "m_large_snapshot");
    const rejected = await RawClient.connect(sock, 1024);
    rejected.hello("bob");
    await rejected.waitClosed();
    assert.equal(rejected.frames.some((f) => f.type === "welcome" || f.type === "mailbox"), false);
    assert.equal(broker.state.stats.mailboxDelivered, 0);
    assert.equal(broker.state.stats.mailboxDropped, 0);

    alice.send({ type: "reserve", id: "m_small_snapshot", from: "alice", reservations: [] });
    await alice.waitFrame((f) => f.type === "ack" && f.id === "m_small_snapshot");
    const bob2 = await RawClient.connect(sock, 1024);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const welcome = await bob2.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.mailboxCount, 1);
    const received = await bob2.waitFrame((f) => f.type === "mailbox" && f.id === "m_welcome_retry");
    assert.equal(received.body, "keep until hello succeeds");
    await alice.waitFrame((f) => f.type === "ack" && f.id === received.id && f.status === "delivered");
    assert.equal(broker.state.stats.mailboxDelivered, 1);
  });

  it("real socket backpressure and failure retries only unwritten entries at their original age", async (t) => {
    const count = 32;
    const { broker, sock } = await setup(t, {
      policy: { rateLimits: { msgPerMin: 100, urgentPerMin: 15, forcePerMin: 1 } },
    });
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await alice.waitFrame((f) => f.type === "presence" && f.from === "bob" && f.status === "offline");
    const queuedAfter = Date.now();
    for (let i = 0; i < count; i += 1) {
      alice.send({ type: "msg", id: `m_retry_${i}`, from: "alice", to: "bob", body: "x".repeat(32700) });
    }
    await alice.waitFrames((f) => f.type === "ack" && f.status === "queued_offline", count);
    const queuedBefore = Date.now();

    const accepted = once(broker.server, "connection");
    const stalled = net.createConnection(sock);
    t.after(() => stalled.destroy());
    await once(stalled, "connect");
    stalled.on("error", () => {}); // Reset is expected when the blocked stream is destroyed.
    const [serverSocket] = await accepted as [Socket];
    stalled.pause(); // A real non-reading recipient fills the unix socket buffers.
    stalled.write(encodeFrame(buildFrame({ type: "hello", from: "bob" }), 65536));
    await alice.waitFrame((f) => f.type === "ack" && f.status === "delivered");
    await waitFor(() => serverSocket.writableLength > 0);
    await sleep(50); // A retry must not reset queuedAt to this later time.
    const closed = once(serverSocket, "close");
    serverSocket.destroy();
    stalled.destroy();
    await closed;
    alice.send({ type: "status_req", id: "m_failed_write_barrier", from: "alice" });
    await alice.waitFrame((f) => f.type === "status_res" && f.id === "m_failed_write_barrier");
    const written = new Set(alice.frames.filter((f) => f.type === "ack" && f.status === "delivered").map((f) => f.id));
    assert.ok(written.size > 0 && written.size < count, "the socket failed partway through the real flush");
    assert.equal(broker.state.stats.mailboxDropped, 0);

    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const welcome = await bob2.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.mailboxCount, count - written.size);
    const retry = await bob2.waitFrames((f) => f.type === "mailbox", count - written.size);
    for (const frame of retry) {
      assert.equal(written.has(frame.id), false, "already-written entries must never replay");
      const queuedAt = Date.parse(frame.queuedAt ?? "");
      assert.ok(queuedAt >= queuedAfter && queuedAt <= queuedBefore, "failure preserves the original enqueue time");
    }
    await alice.waitFrames((f) => f.type === "ack" && f.status === "delivered", count);
    assert.equal(new Set([...written, ...retry.map((f) => f.id)]).size, count);
    assert.equal(broker.state.stats.mailboxDelivered, count);
  });

  it("E6: mailbox cap 3, send 5 → oldest dropped, 3 delivered at hello", async (t) => {
    const { broker, sock } = await setup(t, { config: { mailboxCap: 3 } });
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await sleep(150);

    for (let i = 0; i < 5; i += 1) {
      alice.send({ type: "msg", from: "alice", to: "bob", body: `m${i}` });
    }
    const acks = await alice.waitFrames((f) => f.type === "ack", 7); // 5 queued + 2 drop notices
    const queued = acks.filter((a) => a.status === "queued_offline");
    const notices = acks.filter((a) => a.status === "dropped_offline");
    assert.equal(queued.length, 5); // still acked (E6)
    assert.equal(notices.length, 2); // evicted senders are TOLD (honest drops)
    assert.equal(broker.state.stats.mailboxDropped, 2); // drop-oldest counter

    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const welcome = await bob2.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.mailboxCount, 3);
    const mfs = await bob2.waitFrames((f) => f.type === "mailbox", 3);
    assert.deepEqual(
      mfs.map((f) => f.body),
      ["m2", "m3", "m4"], // oldest dropped, ORDER PRESERVED
    );
  });

  it("TTL: expired mailbox entries are not delivered", async (t) => {
    const { sock } = await setup(t, { config: { mailboxTtlMs: 200 } });
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await sleep(150);

    alice.send({ type: "msg", from: "alice", to: "bob", body: "stale" });
    const ack = await alice.waitFrame((f) => f.type === "ack");
    assert.equal(ack.status, "queued_offline");

    await sleep(400); // beyond TTL (200 ms)
    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const welcome = await bob2.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.mailboxCount, 0);
    // allow a beat for any stray mailbox frame — none must arrive
    await sleep(150);
    assert.equal(bob2.frames.filter((f) => f.type === "mailbox").length, 0);
  });

  it("order preserved across multiple offline senders", async (t) => {
    const { sock } = await setup(t);
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const carol = await RawClient.connect(sock);
    t.after(() => carol.close());
    carol.hello("carol");
    await carol.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await sleep(150);

    alice.send({ type: "msg", from: "alice", to: "bob", body: "a1" });
    carol.send({ type: "msg", from: "carol", to: "bob", body: "c1" });
    alice.send({ type: "msg", from: "alice", to: "bob", body: "a2" });
    await alice.waitFrames((f) => f.type === "ack", 2);
    await carol.waitFrames((f) => f.type === "ack", 1);

    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const mfs = await bob2.waitFrames((f) => f.type === "mailbox", 3);
    const bodies = mfs.map((f) => f.body);
    // cross-sender arrival order at the broker is not deterministic; what the
    // mailbox guarantees is per-sender FIFO + no loss.
    assert.deepEqual([...bodies].sort(), ["a1", "a2", "c1"]);
    assert.ok(bodies.indexOf("a1") < bodies.indexOf("a2"), "per-sender order preserved");
  });
});

describe("mailbox: honest drop notices (sender learns a queued message was dropped)", () => {
  it("TTL expiry → the online sender receives ack(dropped_offline) carrying the original msg id", async (t) => {
    const { broker, sock } = await setup(t, { config: { mailboxTtlMs: 200 } });
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");

    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await sleep(150); // broker observes the close → bob is a known offline alias

    alice.send({ type: "msg", from: "alice", to: "bob", body: "gone-later", id: "m_dropnotice1" });
    const ack = await alice.waitFrame((f) => f.type === "ack");
    assert.equal(ack.status, "queued_offline");

    await sleep(300); // past the 200 ms TTL
    broker.purgeMailboxExpired(); // deterministic purge (independent of the 60 s interval)

    const notice = await alice.waitFrame(
      (f) => f.type === "ack" && f.status === "dropped_offline",
    );
    assert.equal(notice.id, "m_dropnotice1"); // correlates the ORIGINAL message
    assert.equal(notice.to, "bob"); // the mailbox it was bound for
    assert.equal(broker.state.stats.mailboxDropped, 1);
  });

  it("cap eviction → the evicted entry produces an immediate drop notice", async (t) => {
    const { broker, sock } = await setup(t, { config: { mailboxCap: 1 } });
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await sleep(150);

    alice.send({ type: "msg", from: "alice", to: "bob", body: "first", id: "m_dropnotice2a" });
    await alice.waitFrame((f) => f.type === "ack");
    alice.send({ type: "msg", from: "alice", to: "bob", body: "second", id: "m_dropnotice2b" });
    await alice.waitFrame((f) => f.type === "ack" && f.status === "queued_offline");

    const notice = await alice.waitFrame(
      (f) => f.type === "ack" && f.status === "dropped_offline",
    );
    assert.equal(notice.id, "m_dropnotice2a"); // the OLDEST was evicted
    assert.equal(broker.state.stats.mailboxDropped, 1);

    // bob returns: only the survivor is delivered
    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const welcome = await bob2.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.mailboxCount, 1);
  });

  it("expired-at-flush: entries that aged out while the owner was away also notify the sender", async (t) => {
    const { broker, sock } = await setup(t, { config: { mailboxTtlMs: 200 } });
    const alice = await RawClient.connect(sock);
    t.after(() => alice.close());
    alice.hello("alice");
    await alice.waitFrame((f) => f.type === "welcome");
    const bob1 = await RawClient.connect(sock);
    bob1.hello("bob");
    await bob1.waitFrame((f) => f.type === "welcome");
    bob1.close();
    await sleep(150);

    alice.send({ type: "msg", from: "alice", to: "bob", body: "aged-out", id: "m_dropnotice3" });
    await alice.waitFrame((f) => f.type === "ack");
    await sleep(300); // TTL passes while bob is still away

    const bob2 = await RawClient.connect(sock);
    t.after(() => bob2.close());
    bob2.hello("bob");
    const welcome = await bob2.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.mailboxCount, 0); // nothing left to deliver

    const notice = await alice.waitFrame(
      (f) => f.type === "ack" && f.status === "dropped_offline",
    );
    assert.equal(notice.id, "m_dropnotice3");
  });
});
