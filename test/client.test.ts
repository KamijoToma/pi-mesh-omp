// test/client.test.ts — MeshClient against a REAL broker: E2 (kill+restart →
// auto-reconnect → delivered), E7 (awaitReply → 2 reminds → expired), E8 (late
// reply after expiry), E23 (close during pending → closed), E24 (send in
// the reconnect window succeeds after restart).
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { describe, it, type TestContext } from "node:test";
import { MeshClient } from "../src/client/client.js";
import { buildFrame, parseFrameLine, type MeshFrame } from "../src/protocol/envelope.js";
import { encodeFrame, FrameDecoder } from "../src/protocol/frames.js";
import { DEFAULT_MAX_FRAME_BYTES, MAX_BODY_BYTES } from "../src/shared/config.js";
import {
  brokerSocketPathOf,
  makeTempDirs,
  sleep,
  startTestBroker,
  waitFor,
  type TempDirs,
} from "./helpers.js";

function track<T extends { close: () => Promise<void> | void }>(
  t: { after: (fn: () => void | Promise<void>) => void },
  x: T,
): T {
  t.after(async () => {
    await x.close();
  });
  return x;
}

async function fixture(t: { after: (fn: () => void | Promise<void>) => void }): Promise<{
  dirs: TempDirs;
  alice: MeshClient;
  bob: MeshClient;
}> {
  const dirs = makeTempDirs();
  t.after(() => dirs.cleanup());
  const broker = await startTestBroker(dirs.runtimeDir);
  t.after(async () => {
    await broker.close();
  });
  const alice = track(t, new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, config: {} }));
  const bob = track(t, new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir, config: {} }));
  await alice.connect();
  await bob.connect();
  return { dirs, alice, bob };
}

describe("client: awaitReply lifecycle", () => {
  it("E7: awaitReply timeoutMs 400 → exactly 2 reminds then expired (terminal)", async (t) => {
    const { alice, bob } = await fixture(t);
    const reminds: MeshFrame[] = [];
    bob.on("inbound", (f) => {
      if (f.type === "remind") reminds.push(f);
    });
    const res = await alice.send({ to: "bob", message: "need-answer", awaitReply: true, timeoutMs: 400 });
    assert.equal(res.status, "expired");
    await sleep(150); // no extra reminds may trickle in
    assert.equal(reminds.length, 2);
    assert.ok(reminds.every((r) => r.replyTo === res.msgId));
  });

  it("E8: late reply after expiry → no pending match, but surfaced as inbound (not lost)", async (t) => {
    const { alice, bob } = await fixture(t);
    const msgIdP = new Promise<string>((resolve) => {
      bob.once("inbound", (f: MeshFrame) => {
        if (f.type === "msg") resolve(f.id);
      });
    });
    let replyEvent = false;
    alice.on("reply", () => {
      replyEvent = true;
    });
    const orphanP = new Promise<MeshFrame>((resolve) => {
      alice.on("inbound", (f: MeshFrame) => {
        if (f.type === "reply") resolve(f);
      });
    });
    const res = await alice.send({ to: "bob", message: "answer-me", awaitReply: true, timeoutMs: 300 });
    assert.equal(res.status, "expired");
    const msgId = await msgIdP;
    assert.notEqual(msgId, "");
    const rres = await bob.reply(msgId, "too-late");
    assert.equal(rres.status, "delivered"); // broker routes it…
    const orphan = await orphanP;
    assert.equal(replyEvent, false); // …the expired pending ignores it…
    assert.equal(orphan.body, "too-late"); // …but the answer is NOT lost
    assert.equal(orphan.replyTo, msgId);
  });

  it("orphan reply (sender did not awaitReply) is surfaced as inbound — the cs-room bug", async (t) => {
    const { alice, bob } = await fixture(t);
    // both in the same non-default room, like the cs-room sessions
    await alice.join("cs-room");
    await bob.join("cs-room");
    const receivedP = new Promise<MeshFrame>((resolve) => {
      bob.once("inbound", (f: MeshFrame) => {
        if (f.type === "msg") resolve(f);
      });
    });
    const orphanP = new Promise<MeshFrame>((resolve) => {
      alice.on("inbound", (f: MeshFrame) => {
        if (f.type === "reply") resolve(f);
      });
    });
    // alice sends WITHOUT awaitReply (fire-and-forget, like the missions)
    const res = await alice.send({ to: "bob", message: "MISSION RE A", room: "cs-room" });
    assert.equal(res.status, "delivered");
    const received = await receivedP;
    // bob answers with mesh_reply → delivered on the wire
    const rres = await bob.reply(received.id, "MISSION COMPLETE");
    assert.equal(rres.status, "delivered");
    // alice's session MUST see the answer now
    const orphan = await orphanP;
    assert.equal(orphan.body, "MISSION COMPLETE");
    assert.equal(orphan.from, "bob");
    assert.equal(orphan.room, "cs-room");
  });

  it("awaitReply happy path: reply resolves with response + outputHash", async (t) => {
    const { alice, bob } = await fixture(t);
    bob.once("inbound", (f) => {
      if (f.type === "msg") void bob.reply(f.id, `ack:${f.body ?? ""}`);
    });
    const res = await alice.send({ to: "bob", message: "ping", awaitReply: true, timeoutMs: 3000 });
    assert.equal(res.status, "reply");
    if (res.status === "reply") {
      assert.equal(res.response, "ack:ping");
      assert.match(res.outputHash, /^[0-9a-f]{64}$/);
    }
  });

  it("E23: close during pending awaitReply → error closed", async (t) => {
    const { alice } = await fixture(t);
    const sendPromise = alice.send({ to: "bob", message: "hang", awaitReply: true, timeoutMs: 5000 });
    await sleep(100); // let the msg land and the pending register
    await alice.close();
    const res = await sendPromise;
    assert.equal(res.status, "error");
    if (res.status === "error") assert.equal(res.reason, "closed");
  });
});

describe("client: encoded frame limits", () => {
  it("rejects an escaped launch frame without leaving a pending mission or ACK waiter", async (t) => {
    const { alice, bob } = await fixture(t);
    const rejected = await alice.send({
      to: "bob",
      message: "\\".repeat(MAX_BODY_BYTES),
      awaitReply: true,
      block: false,
    });
    assert.deepEqual(rejected, { status: "error", reason: "oversized" });
    assert.equal(alice.pendingCount, 0);
    assert.deepEqual(alice.missionStatus(), []);
    assert.equal(alice.isReloadSafe(), true);

    const received = new Promise<MeshFrame>((resolve) => bob.once("inbound", resolve));
    const body = "x".repeat(MAX_BODY_BYTES);
    const accepted = await alice.send({ to: "bob", message: body });
    assert.equal(accepted.status, "delivered");
    assert.equal((await received).body, body);
  });

  it("rejects an escaped reply before registering an ACK and allows a valid reply afterward", async (t) => {
    const { alice, bob } = await fixture(t);
    const incoming = new Promise<MeshFrame>((resolve) => bob.once("inbound", resolve));
    await alice.send({ to: "bob", message: "reply with a report" });
    const original = await incoming;
    const rejected = await bob.reply(original.id, "\\".repeat(MAX_BODY_BYTES));
    assert.equal(rejected.status, "error");
    if (rejected.status === "error") assert.equal(rejected.reason, "oversized");
    assert.equal(bob.isReloadSafe(), true);

    const received = new Promise<MeshFrame>((resolve) => alice.once("inbound", resolve));
    assert.equal((await bob.reply(original.id, "valid report")).status, "delivered");
    assert.equal((await received).body, "valid report");
  });
});

describe("client: broker kill & reconnect", () => {
  it("E1: unusable runtime directory returns blocked instead of throwing (I10)", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    // runtimeDir inside a REGULAR FILE → broker can never spawn/connect there
    const blocker = path.join(dirs.root, "not-a-dir");
    writeFileSync(blocker, "x", "utf8");
    const alice = track(
      t,
      new MeshClient({ alias: "alice", runtimeDir: path.join(blocker, "run"), config: {}, noReconnect: true }),
    );
    const res = await alice.send({ to: "bob", message: "anyone there?" });
    assert.equal(res.status, "blocked");
  });

  it("E2: kill+restart broker same runtimeDir → auto-reconnect + send delivered", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    let broker = await startTestBroker(dirs.runtimeDir);
    const alice = track(t, new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, config: {} }));
    const bob = track(t, new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir, config: {} }));
    await alice.connect();
    await bob.connect();

    await broker.close(); // broker killed; sockets destroyed
    await waitFor(() => (!alice.isOnline() && !bob.isOnline() ? true : undefined));
    broker = await startTestBroker(dirs.runtimeDir); // restart, same runtimeDir
    t.after(async () => {
      await broker.close();
    });

    // both clients must auto-reconnect (backoff → re-hello)
    await waitFor(() => (alice.isOnline() && bob.isOnline() ? true : undefined), 5000);
    const res = await alice.send({ to: "bob", message: "post-crash" });
    assert.equal(res.status, "delivered");
  });

  it("E24: send during the reconnect window succeeds after restart", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    let broker = await startTestBroker(dirs.runtimeDir);
    // alice: noReconnect → she stays inside the reconnect window until send().
    const alice = track(t, new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, config: {}, noReconnect: true }));
    const bob = track(t, new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir, config: {} }));
    await alice.connect();
    await bob.connect();

    await broker.close();
    await waitFor(() => (!alice.isOnline() ? true : undefined)); // reconnect window open for alice
    broker = await startTestBroker(dirs.runtimeDir);
    t.after(async () => {
      await broker.close();
    });
    await waitFor(() => (bob.isOnline() ? true : undefined), 5000); // bob re-hello'd on the fresh broker
    // send while alice is not yet reconnected → connect() under the hood → delivered
    const res = await alice.send({ to: "bob", message: "during-reconnect", timeoutMs: 4000 });
    assert.equal(res.status, "delivered");
  });
});

describe("client: peekInbox (read-only ledger-enrichment accessor)", () => {
  it("returns the inbound frame without mutating the inbox; survives a reply", async (t) => {
    const { alice, bob } = await fixture(t);
    let msgId = "";
    bob.once("inbound", (f) => {
      if (f.type === "msg") msgId = f.id;
    });
    const sres = await alice.send({ to: "bob", message: "peek-me", priority: "urgent" });
    assert.equal(sres.status, "delivered");
    await waitFor(() => (msgId !== "" ? true : undefined));

    const first = bob.peekInbox(msgId);
    assert.ok(first, "inbox frame must be peekable");
    assert.equal(first.from, "alice");
    assert.equal(first.room, "default");
    assert.equal(first.priority, "urgent");
    // read-only: a second peek returns the same frame — no eviction, no mutation
    assert.equal(bob.peekInbox(msgId), first);

    const rres = await bob.reply(msgId, "ack");
    assert.equal(rres.status, "delivered");
    // still present after the reply round-trip
    assert.equal(bob.peekInbox(msgId), first);
    assert.equal(bob.peekInbox("m_unknown_00000000"), undefined);
  });
});

describe("client: N2 — unknown ack status mapping", () => {
  it("dropped_offline ack on a msg → error with the status, never delivered", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    // Fake broker: welcomes hellos, acks every msg with dropped_offline (a
    // status a real broker never sends for msg — regression guard for the
    // client-side mapping, which previously fell through to `delivered`).
    const server = net.createServer((socket) => {
      const decoder = new FrameDecoder(DEFAULT_MAX_FRAME_BYTES);
      socket.on("data", (chunk) => {
        let lines: string[] = [];
        try {
          lines = decoder.push(chunk);
        } catch {
          return;
        }
        for (const line of lines) {
          const parsed = parseFrameLine(line);
          if (!parsed.ok) continue;
          const f = parsed.frame;
          if (f.type === "hello") {
            socket.write(
              encodeFrame(
                buildFrame({
                  type: "welcome",
                  id: f.id,
                  from: f.from,
                  rooms: ["default"],
                  peers: [],
                }),
                DEFAULT_MAX_FRAME_BYTES,
              ),
            );
          } else if (f.type === "msg") {
            socket.write(
              encodeFrame(
                buildFrame({ type: "ack", id: f.id, status: "dropped_offline" }),
                DEFAULT_MAX_FRAME_BYTES,
              ),
            );
          }
        }
      });
      socket.on("error", () => {});
    });
    await new Promise<void>((resolve) => {
      server.listen(brokerSocketPathOf(dirs.runtimeDir), resolve);
    });

    const alice = track(
      t,
      new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, config: {}, noReconnect: true }),
    );
    // t.after hooks run FIFO: the client must close BEFORE server.close
    // (server.close waits for open connections).
    t.after(async () => {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    });
    const res = await alice.send({ to: "bob", message: "hi" });
    assert.notEqual(res.status, "delivered", "unknown ack status must not map to delivered");
    assert.equal(res.status, "error");
    if (res.status === "error") assert.match(res.reason, /dropped_offline/);
  });
});

async function rawLifecycleFixture(
  t: TestContext,
  receive: (frame: MeshFrame, socket: net.Socket) => void,
  autoWelcome = true,
) {
  const dirs = makeTempDirs("mesh-wire-lifecycle-");
  const frames: MeshFrame[] = [];
  const sockets = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
    const decoder = new FrameDecoder(DEFAULT_MAX_FRAME_BYTES);
    socket.on("data", (chunk) => {
      for (const line of decoder.push(chunk)) {
        const parsed = parseFrameLine(line);
        if (!parsed.ok) continue;
        const frame = parsed.frame;
        frames.push(frame);
        if (autoWelcome && frame.type === "hello") {
          socket.write(encodeFrame(buildFrame({
            type: "welcome", id: frame.id, from: frame.from, rooms: ["default"], peers: [],
          }), DEFAULT_MAX_FRAME_BYTES));
        }
        receive(frame, socket);
      }
    });
  });
  await new Promise<void>((resolve) => server.listen(brokerSocketPathOf(dirs.runtimeDir), resolve));
  const alice = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, noReconnect: true });
  t.after(async () => {
    await alice.close();
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    dirs.cleanup();
  });
  return { alice, frames, sockets };
}

describe("client: send cancellation and truthful request failures", () => {
  it("pre-aborted send does not connect, register, write, or later flush a cancelled frame", { timeout: 5000 }, async (t) => {
    const { dirs, bob } = await fixture(t);
    const sender = track(t, new MeshClient({ alias: "pre-aborted", runtimeDir: dirs.runtimeDir, noReconnect: true }));
    const incoming: MeshFrame[] = [];
    bob.on("inbound", (frame: MeshFrame) => incoming.push(frame));
    const abort = new AbortController();
    abort.abort();
    const result = await sender.send({ to: "bob", message: "must never arrive", awaitReply: true, signal: abort.signal });
    assert.deepEqual(result, { status: "error", reason: "aborted" });
    assert.equal(sender.isOnline(), false);
    assert.equal(sender.pendingCount, 0);
    assert.deepEqual(sender.missionStatus(), []);
    assert.equal(sender.isReloadSafe(), true);
    assert.ok(!(await bob.status()).peers.some((peer) => peer.alias === "pre-aborted"));
    await sender.connect();
    await sender.send({ to: "bob", message: "only live message" });
    await waitFor(() => incoming.some((frame) => frame.body === "only live message"));
    assert.deepEqual(incoming.filter((frame) => frame.type === "msg").map((frame) => frame.body), ["only live message"]);
  });

  it("aborts promptly while sharing a connection handshake without aborting the shared connection", { timeout: 5000 }, async (t) => {
    let releaseWelcome: (() => void) | undefined;
    const { alice, frames } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "hello") {
        releaseWelcome = () => socket.write(encodeFrame(buildFrame({
          type: "welcome", id: frame.id, rooms: ["default"], peers: [],
        }), DEFAULT_MAX_FRAME_BYTES));
      } else if (frame.type === "msg") {
        socket.write(encodeFrame(buildFrame({ type: "ack", id: frame.id, status: "delivered" }), DEFAULT_MAX_FRAME_BYTES));
      }
    }, false);
    const shared = alice.connect();
    const abort = new AbortController();
    const sending = alice.send({ to: "bob", message: "cancel during connect", awaitReply: true, signal: abort.signal });
    await waitFor(() => releaseWelcome !== undefined);
    abort.abort();
    assert.deepEqual(await sending, { status: "error", reason: "aborted" });
    assert.equal(alice.pendingCount, 0);
    assert.deepEqual(alice.missionStatus(), []);
    releaseWelcome!();
    await shared;
    assert.equal((await alice.send({ to: "bob", message: "shared connection survived" })).status, "delivered");
    assert.deepEqual(frames.filter((frame) => frame.type === "msg").map((frame) => frame.body), ["shared connection survived"]);
    assert.equal(alice.isReloadSafe(), true);
  });

  it("aborts an ACK wait without leaks or a non-delivery claim, and still injects a late answer", { timeout: 5000 }, async (t) => {
    let held: { frame: MeshFrame; socket: net.Socket } | undefined;
    const { alice, frames } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type !== "msg") return;
      if (frame.body === "hold ACK") held = { frame, socket };
      else socket.write(encodeFrame(buildFrame({ type: "ack", id: frame.id, status: "delivered" }), DEFAULT_MAX_FRAME_BYTES));
    });
    const abort = new AbortController();
    const sending = alice.send({ to: "bob", message: "hold ACK", awaitReply: true, signal: abort.signal });
    await waitFor(() => held !== undefined);
    abort.abort();
    const result = await sending;
    assert.equal(result.status, "error");
    if (result.status === "error") {
      assert.equal(result.reason, "aborted");
      assert.equal(result.delivery, "unknown", "the recipient may already have received the written frame");
    }
    assert.equal(alice.pendingCount, 0);
    assert.deepEqual(alice.missionStatus(), []);
    assert.equal(alice.isReloadSafe(), true);
    const inbound: MeshFrame[] = [];
    alice.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    held!.socket.write(Buffer.concat([
      encodeFrame(buildFrame({ type: "ack", id: held!.frame.id, status: "delivered" }), DEFAULT_MAX_FRAME_BYTES),
      encodeFrame(buildFrame({ type: "reply", from: "bob", to: "alice", room: "default", replyTo: held!.frame.id, body: "already executed" }), DEFAULT_MAX_FRAME_BYTES),
    ]));
    await waitFor(() => inbound.some((frame) => frame.body === "already executed"));
    assert.equal((await alice.send({ to: "bob", message: "next operation" })).status, "delivered");
    assert.equal(frames.filter((frame) => frame.body === "hold ACK").length, 1);
    assert.equal(alice.isReloadSafe(), true);
  });

  it("does not downgrade a delivered mailbox notice received before the queued-ACK continuation", { timeout: 5000 }, async (t) => {
    const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "msg") socket.write(Buffer.concat([
        encodeFrame(buildFrame({ type: "ack", id: frame.id, status: "queued_offline" }), DEFAULT_MAX_FRAME_BYTES),
        encodeFrame(buildFrame({ type: "ack", id: frame.id, status: "delivered" }), DEFAULT_MAX_FRAME_BYTES),
      ]));
    });
    const sent = await alice.send({ to: "bob", message: "mailbox race", awaitReply: true, block: false });
    assert.equal(sent.status, "delivered");
    assert.equal(alice.pendingCount, 1);
  });

  it("rejects offline, protocol, and disconnected status requests but preserves a real empty snapshot", { timeout: 5000 }, async (t) => {
    let mode: "empty" | "error" | "disconnect" = "empty";
    const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type !== "status_req") return;
      if (mode === "disconnect") socket.destroy();
      else socket.write(encodeFrame(mode === "empty"
        ? buildFrame({ type: "status_res", id: frame.id, peers: [], rooms: [] })
        : buildFrame({ type: "error", id: frame.id, code: "not_member" }), DEFAULT_MAX_FRAME_BYTES));
    });
    await assert.rejects(alice.status(), /offline/);
    await alice.connect();
    const empty = await alice.status();
    assert.deepEqual(empty.peers, []);
    assert.deepEqual(empty.rooms, []);
    mode = "error";
    await assert.rejects(alice.status("not-joined"), /not_member/);
    mode = "disconnect";
    await assert.rejects(alice.status(), /connection_lost/);
    await assert.rejects(alice.status(), /connection_lost/);
  });

  it("rejects a status request timeout instead of manufacturing an empty mesh", { timeout: 8000 }, async (t) => {
    const { alice } = await rawLifecycleFixture(t, () => {});
    await alice.connect();
    await assert.rejects(alice.status(), /^Error: timeout$/);
    assert.equal(alice.isReloadSafe(), true);
  });

  it("preserves the real connection refusal reason in room round trips", { timeout: 5000 }, async (t) => {
    const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "hello") socket.write(encodeFrame(buildFrame({
        type: "error", id: frame.id, code: "invalid_token",
      }), DEFAULT_MAX_FRAME_BYTES));
    }, false);
    await assert.rejects(alice.join("work"), /invalid_token/);
  });
});

describe("client: broker reboot delivery knowledge", () => {
  it("ends queued uncertainty for launch and blocking sends, while accepting late answers", { timeout: 8000 }, async (t) => {
    const dirs = makeTempDirs("mesh-queued-reboot-");
    const options = { config: { brokerId: "lifecycle-broker" } };
    let broker = await startTestBroker(dirs.runtimeDir, options);
    const alice = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, noReconnect: true });
    const bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir, noReconnect: true });
    const acknowledgments: MeshFrame[] = [];
    alice.on("frame", (frame: MeshFrame) => {
      if (frame.type === "ack") acknowledgments.push(frame);
    });
    t.after(async () => {
      await Promise.all([alice.close(), bob.close()]);
      await broker.close();
      dirs.cleanup();
    });
    await alice.connect();
    await bob.connect();
    await bob.close();
    await waitFor(() => !alice.knowsPeer("bob"));
    const launch = await alice.send({ to: "bob", message: "queued launch", awaitReply: true, block: false });
    assert.equal(launch.status, "queued_offline");
    const blocking = alice.send({ to: "bob", message: "queued blocking", awaitReply: true });
    await waitFor(() => acknowledgments.filter((frame) => frame.status === "queued_offline").length === 2);
    const oldBoot = alice.brokerInfo!.bootId;
    await broker.close();
    await waitFor(() => !alice.isOnline());
    broker = await startTestBroker(dirs.runtimeDir, options);
    await alice.connect();
    assert.notEqual(alice.brokerInfo!.bootId, oldBoot);
    const blocked = await blocking;
    assert.equal(blocked.status, "error");
    if (blocked.status === "error") assert.equal(blocked.reason, "connection_lost");
    const result = await alice.waitAll(1000);
    assert.equal(result.status, "error");
    assert.equal(result.reason, "broker_state_lost_delivery_unknown");
    assert.equal(result.missing.length, 2);
    assert.ok(result.missing.every((mission) => mission.reason === "broker_state_lost_delivery_unknown"));
    assert.equal(alice.pendingCount, 0);
    const late: MeshFrame[] = [];
    alice.on("inbound", (frame: MeshFrame) => late.push(frame));
    await bob.connect();
    assert.ok(launch.msgId);
    assert.equal((await bob.reply(launch.msgId, "late externally completed work", { to: "alice", room: "default" })).status, "delivered");
    await waitFor(() => late.some((frame) => frame.body === "late externally completed work"));
  });

  it("keeps both socket-delivered and mailbox-delivered missions alive across reboot", { timeout: 8000 }, async (t) => {
    const dirs = makeTempDirs("mesh-delivered-reboot-");
    const options = { config: { brokerId: "lifecycle-broker" } };
    let broker = await startTestBroker(dirs.runtimeDir, options);
    const alice = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, noReconnect: true });
    const bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir, noReconnect: true });
    const acknowledgments: MeshFrame[] = [];
    alice.on("frame", (frame: MeshFrame) => {
      if (frame.type === "ack") acknowledgments.push(frame);
    });
    t.after(async () => {
      await Promise.all([alice.close(), bob.close()]);
      await broker.close();
      dirs.cleanup();
    });
    await alice.connect();
    await bob.connect();
    await bob.close();
    await waitFor(() => !alice.knowsPeer("bob"));
    const mailbox = await alice.send({ to: "bob", message: "mailbox confirmed", awaitReply: true, block: false });
    assert.equal(mailbox.status, "queued_offline");
    await bob.connect();
    await waitFor(() => acknowledgments.some((frame) => frame.id === mailbox.msgId && frame.status === "delivered"));
    const direct = await alice.send({ to: "bob", message: "socket confirmed", awaitReply: true, block: false });
    assert.equal(direct.status, "delivered");
    assert.ok(mailbox.msgId && direct.msgId);
    await broker.close();
    await waitFor(() => !alice.isOnline() && !bob.isOnline());
    broker = await startTestBroker(dirs.runtimeDir, options);
    await alice.connect();
    await bob.connect();
    await waitFor(() => alice.knowsPeer("bob"));
    assert.equal(alice.pendingCount, 2);
    const waiting = alice.waitAll(2000);
    await bob.reply(mailbox.msgId, "mailbox work survived");
    await bob.reply(direct.msgId, "socket work survived");
    const result = await waiting;
    assert.equal(result.status, "complete");
    assert.equal(result.answered, 2);
    assert.deepEqual(result.answers.map((answer) => answer.response).sort(), ["mailbox work survived", "socket work survived"]);
  });
});

it("launch detaches its tool abort signal after delivery and retains the background answer", { timeout: 5000 }, async (t) => {
  const { alice, bob } = await fixture(t);
  const abort = new AbortController();
  const launched = await alice.send({
    to: "bob", message: "launch outlives tool", awaitReply: true, block: false, signal: abort.signal,
  });
  assert.equal(launched.status, "delivered");
  assert.ok(launched.msgId);
  abort.abort();
  assert.equal(alice.pendingCount, 1);
  const waiting = alice.waitAll(2000);
  assert.equal((await bob.reply(launched.msgId, "background mission survived")).status, "delivered");
  const result = await waiting;
  assert.equal(result.status, "complete");
  assert.deepEqual(result.answers, [{ msgId: launched.msgId, to: "bob", response: "background mission survived" }]);
});

describe("client: cooperative foreground send waits", () => {
  for (const priority of ["normal", "urgent"] as const) {
    it(`yields to ${priority} input without cancelling the mission or retaining its tool signal`, { timeout: 5000 }, async (t) => {
      const { alice, bob } = await fixture(t);
      const abort = new AbortController();
      const events: MeshFrame[] = [];
      alice.on("inbound", (frame: MeshFrame) => {
        events.push(frame);
        if (frame.type === "msg") alice.interruptWaits("inbound_message", frame.id);
      });
      const sending = alice.send({ to: "bob", message: "work continues", awaitReply: true, signal: abort.signal });
      await waitFor(() => bob.transcript.some((frame) => frame.body === "work continues"));
      const incoming = await bob.send({ to: "alice", message: "new actionable input", priority });
      assert.ok(incoming.msgId);
      const result = await sending;
      assert.equal(result.status, "interrupted");
      if (result.status !== "interrupted") return;
      assert.equal(result.reason, "inbound_message");
      assert.equal(result.interruptedBy, incoming.msgId);
      assert.equal(result.delivery, "delivered");
      assert.equal(result.tracked, true);
      assert.ok(result.msgId);
      abort.abort();
      assert.equal(alice.pendingCount, 1);
      await bob.reply(result.msgId, "completed after yield");
      await waitFor(() => events.some((frame) => frame.body === "completed after yield"));
      assert.equal(events.filter((frame) => frame.body === "completed after yield").length, 1);
      assert.equal(alice.pendingCount, 0);
      const verdict = await alice.waitAll(1000);
      assert.equal(verdict.status, "complete");
      assert.deepEqual(verdict.answers, [{ msgId: result.msgId, to: "bob", response: "completed after yield" }]);
      assert.equal(alice.interruptWaits("nothing_waiting"), false);
    });
  }

  it("yields before handshake completion and never sends that request after connecting", { timeout: 5000 }, async (t) => {
    let releaseWelcome: (() => void) | undefined;
    const { alice, frames } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "hello") releaseWelcome = () => socket.write(encodeFrame(buildFrame({
        type: "welcome", id: frame.id, rooms: ["default"], peers: [],
      }), DEFAULT_MAX_FRAME_BYTES));
      if (frame.type === "msg") socket.write(encodeFrame(buildFrame({
        type: "ack", id: frame.id, status: "delivered",
      }), DEFAULT_MAX_FRAME_BYTES));
    }, false);
    const connection = alice.connect();
    const sending = alice.send({ to: "bob", message: "unsent yielded work", awaitReply: true });
    await waitFor(() => releaseWelcome !== undefined);
    assert.equal(alice.interruptWaits("inbound_message", "incoming"), true);
    const result = await sending;
    assert.equal(result.status, "interrupted");
    if (result.status === "interrupted") {
      assert.equal(result.delivery, "not_sent");
      assert.equal(result.tracked, false);
      assert.equal(result.msgId, undefined);
    }
    releaseWelcome!();
    await connection;
    await alice.send({ to: "bob", message: "only live request" });
    assert.deepEqual(frames.filter((frame) => frame.type === "msg").map((frame) => frame.body), ["only live request"]);
    assert.equal(alice.pendingCount, 0);
    assert.deepEqual(alice.missionStatus(), []);
  });

  it("does not write a request interrupted during pending registration", { timeout: 5000 }, async (t) => {
    const { alice, bob } = await fixture(t);
    alice.once("pending", () => alice.interruptWaits("inbound_message"));
    const result = await alice.send({ to: "bob", message: "interrupted before write", awaitReply: true });
    assert.equal(result.status, "interrupted");
    if (result.status === "interrupted") {
      assert.equal(result.delivery, "not_sent");
      assert.equal(result.tracked, false);
    }
    await alice.send({ to: "bob", message: "following live request" });
    await waitFor(() => bob.transcript.some((frame) => frame.body === "following live request"));
    assert.ok(!bob.transcript.some((frame) => frame.body === "interrupted before write"));
    assert.equal(alice.pendingCount, 0);
    assert.deepEqual(alice.missionStatus(), []);
  });

  it("retains the pre-ACK transaction, processes asynchronous delivery and injects its late answer once", { timeout: 5000 }, async (t) => {
    let held: { frame: MeshFrame; socket: net.Socket } | undefined;
    const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "msg") held = { frame, socket };
    });
    const abort = new AbortController();
    let deliveryConfirmed = false;
    alice.on("frame", (frame: MeshFrame) => {
      if (frame.type === "ack" && frame.id === held?.frame.id && frame.status === "delivered") deliveryConfirmed = true;
    });
    const inbound: MeshFrame[] = [];
    alice.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    const sending = alice.send({ to: "bob", message: "hold ACK", awaitReply: true, signal: abort.signal });
    await waitFor(() => held !== undefined);
    alice.interruptWaits("inbound_message", "incoming");
    const result = await sending;
    assert.equal(result.status, "interrupted");
    if (result.status !== "interrupted") return;
    assert.equal(result.msgId, held!.frame.id);
    assert.equal(result.delivery, "unknown");
    assert.equal(result.tracked, true);
    abort.abort();
    assert.equal(alice.pendingCount, 1);
    held!.socket.write(Buffer.concat([
      encodeFrame(buildFrame({ type: "ack", id: held!.frame.id, status: "queued_offline" }), DEFAULT_MAX_FRAME_BYTES),
      encodeFrame(buildFrame({ type: "ack", id: held!.frame.id, status: "delivered" }), DEFAULT_MAX_FRAME_BYTES),
    ]));
    await waitFor(() => deliveryConfirmed);
    const answer = buildFrame({
      type: "reply", from: "bob", to: "alice", room: "default", replyTo: held!.frame.id, body: "late real answer",
    });
    held!.socket.write(Buffer.concat([
      encodeFrame(answer, DEFAULT_MAX_FRAME_BYTES), encodeFrame(answer, DEFAULT_MAX_FRAME_BYTES),
    ]));
    await waitFor(() => inbound.length === 1);
    assert.equal(inbound[0]!.body, "late real answer");
    assert.equal(alice.pendingCount, 0);
    assert.equal(alice.isReloadSafe(), true);
    const complete = await alice.waitAll(1000);
    assert.equal(complete.status, "complete");
    assert.equal(complete.answered, 1);
  });

  it("settles a retained mission honestly when its delayed ACK refuses it", { timeout: 5000 }, async (t) => {
    let held: { frame: MeshFrame; socket: net.Socket } | undefined;
    const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "msg") held = { frame, socket };
    });
    const sending = alice.send({ to: "bob", message: "refused later", awaitReply: true });
    await waitFor(() => held !== undefined);
    alice.interruptWaits("inbound_message");
    assert.equal((await sending).status, "interrupted");
    held!.socket.write(encodeFrame(buildFrame({
      type: "error", id: held!.frame.id, code: "policy_denied",
    }), DEFAULT_MAX_FRAME_BYTES));
    await waitFor(() => alice.pendingCount === 0);
    const verdict = await alice.waitAll(1000);
    assert.equal(verdict.status, "error");
    assert.equal(verdict.reason, "policy_denied");
    assert.equal(verdict.answered, 0);
    assert.equal(alice.isReloadSafe(), true);
  });

  it("keeps an own matched answer in the send result rather than injecting or interrupting it", { timeout: 5000 }, async (t) => {
    const { alice, bob } = await fixture(t);
    const inbound: MeshFrame[] = [];
    alice.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    const sending = alice.send({ to: "bob", message: "own answer", awaitReply: true });
    await waitFor(() => bob.transcript.some((frame) => frame.body === "own answer"));
    const id = alice.missionStatus()[0]!.msgId;
    await bob.reply(id, "normal result");
    const result = await sending;
    assert.equal(result.status, "reply");
    if (result.status === "reply") assert.equal(result.response, "normal result");
    assert.deepEqual(inbound, []);
  });

  it("does not hide a launch answer behind an unrelated single-task wait", { timeout: 5000 }, async (t) => {
    const { alice, bob } = await fixture(t);
    const launch = await alice.send({ to: "bob", message: "launched", awaitReply: true, block: false });
    assert.ok(launch.msgId);
    const inbound: MeshFrame[] = [];
    alice.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    const sending = alice.send({ to: "bob", message: "different task", awaitReply: true });
    await waitFor(() => alice.pendingCount === 2);
    await bob.reply(launch.msgId, "launch result");
    const result = await sending;
    assert.equal(result.status, "interrupted");
    await waitFor(() => inbound.length === 1);
    assert.equal(inbound[0]!.body, "launch result");
    assert.equal(alice.pendingCount, 1);
    if (result.status !== "interrupted") return;
    assert.ok(result.msgId);
    const aggregate = alice.waitAll(1000);
    await waitFor(() => bob.peekInbox(result.msgId!) !== undefined);
    await bob.reply(result.msgId, "second result");
    assert.equal((await aggregate).answered, 2);
    assert.equal(inbound.length, 1);
  });

  it("releases a single foreground wait on mission-room departure and recovers without resending", { timeout: 5000 }, async (t) => {
    const { alice, bob } = await fixture(t);
    await alice.join("work");
    await bob.join("work");
    const sending = alice.send({ to: "bob", room: "work", message: "room work", awaitReply: true });
    await waitFor(() => bob.transcript.some((frame) => frame.body === "room work"));
    await bob.leave("work");
    const result = await sending;
    assert.equal(result.status, "error");
    if (result.status !== "error") return;
    assert.equal(result.reason, "target_offline");
    assert.equal(result.delivery, "delivered");
    assert.equal(result.tracked, true);
    assert.equal(alice.pendingCount, 1);
    assert.equal(alice.knowsPeer("bob"), true);
    await bob.join("work");
    await waitFor(() => alice.missionStatus()[0]?.reason === undefined);
    const aggregate = alice.waitAll(1000);
    assert.ok(result.msgId);
    await bob.reply(result.msgId, "recovered room answer");
    assert.equal((await aggregate).status, "complete");
  });

  it("keeps initial queued-offline work and unrelated-room departures waiting normally", { timeout: 5000 }, async (t) => {
    const { alice, bob } = await fixture(t);
    let queued = false;
    alice.on("frame", (frame: MeshFrame) => {
      if (frame.type === "ack" && frame.status === "queued_offline") queued = true;
    });
    await bob.close();
    await waitFor(() => !alice.knowsPeer("bob"));
    const sending = alice.send({ to: "bob", message: "queued work", awaitReply: true });
    await waitFor(() => queued);
    const id = alice.missionStatus()[0]!.msgId;
    await bob.connect();
    await waitFor(() => bob.peekInbox(id) !== undefined);
    await alice.join("unrelated");
    await bob.join("unrelated");
    await bob.leave("unrelated");
    await bob.reply(id, "queued work finished");
    const result = await sending;
    assert.equal(result.status, "reply");
    if (result.status === "reply") assert.equal(result.response, "queued work finished");
  });

  it("reports own connection loss before ACK while retaining the possibly delivered mission", { timeout: 5000 }, async (t) => {
    let held: { frame: MeshFrame; socket: net.Socket } | undefined;
    const { alice, sockets } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "msg") held = { frame, socket };
    });
    const sending = alice.send({ to: "bob", message: "written without ACK", awaitReply: true });
    await waitFor(() => held !== undefined);
    held!.socket.destroy();
    const result = await sending;
    assert.equal(result.status, "error");
    if (result.status === "error") {
      assert.equal(result.reason, "connection_lost");
      assert.equal(result.delivery, "unknown");
      assert.equal(result.tracked, true);
    }
    assert.equal(alice.pendingCount, 1);
    await alice.connect();
    const events: MeshFrame[] = [];
    alice.on("inbound", (frame: MeshFrame) => events.push(frame));
    const socket = sockets.values().next().value!;
    socket.write(encodeFrame(buildFrame({
      type: "reply", from: "bob", to: "alice", room: "default",
      replyTo: held!.frame.id, body: "completed across reconnect",
    }), DEFAULT_MAX_FRAME_BYTES));
    await waitFor(() => events.length === 1);
    assert.equal(events[0]!.body, "completed across reconnect");
    assert.equal((await alice.waitAll(1000)).status, "complete");
  });
});

it("preserves expiry after a foreground send yields instead of inventing an answer", { timeout: 5000 }, async (t) => {
  const { alice, bob } = await fixture(t);
  const sending = alice.send({ to: "bob", message: "expires after yielding", awaitReply: true, timeoutMs: 250 });
  await waitFor(() => bob.transcript.some((frame) => frame.body === "expires after yielding"));
  alice.interruptWaits("inbound_message");
  const result = await sending;
  assert.equal(result.status, "interrupted");
  const verdict = await alice.waitAll(1000);
  assert.equal(verdict.status, "timeout");
  assert.equal(verdict.answered, 0);
  assert.equal(verdict.missing.length, 1);
  assert.equal(alice.pendingCount, 0);
  assert.equal(alice.missionStatus()[0]!.status, "expired");
});

it("detects target loss before the ACK continuation and accepts its later real answer", { timeout: 5000 }, async (t) => {
  let held: { frame: MeshFrame; socket: net.Socket } | undefined;
  const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
    if (frame.type !== "msg") return;
    held = { frame, socket };
    socket.write(Buffer.concat([
      encodeFrame(buildFrame({ type: "ack", id: frame.id, status: "delivered" }), DEFAULT_MAX_FRAME_BYTES),
      encodeFrame(buildFrame({ type: "presence", from: "bob", room: "default", status: "offline" }), DEFAULT_MAX_FRAME_BYTES),
    ]));
  });
  const result = await alice.send({ to: "bob", message: "target left immediately", awaitReply: true });
  assert.equal(result.status, "error");
  if (result.status === "error") {
    assert.equal(result.reason, "target_offline");
    assert.equal(result.delivery, "delivered");
    assert.equal(result.tracked, true);
  }
  assert.equal(alice.pendingCount, 1);
  assert.equal((await alice.waitAll(1000)).status, "error");
  const inbound: MeshFrame[] = [];
  alice.on("inbound", (frame: MeshFrame) => inbound.push(frame));
  held!.socket.write(encodeFrame(buildFrame({
    type: "reply", from: "bob", to: "alice", room: "default",
    replyTo: held!.frame.id, body: "finished despite presence loss",
  }), DEFAULT_MAX_FRAME_BYTES));
  await waitFor(() => inbound.length === 1);
  assert.equal(alice.missionStatus()[0]!.reason, undefined);
  assert.equal((await alice.waitAll(1000)).status, "complete");
});

it("returns an own answer before a delayed ACK without double-injection or abandoning the ACK", { timeout: 5000 }, async (t) => {
  let held: { frame: MeshFrame; socket: net.Socket } | undefined;
  const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
    if (frame.type !== "msg") return;
    held = { frame, socket };
    socket.write(encodeFrame(buildFrame({
      type: "reply", from: "bob", to: "alice", room: "default", replyTo: frame.id, body: "answer before ACK",
    }), DEFAULT_MAX_FRAME_BYTES));
  });
  const inbound: MeshFrame[] = [];
  alice.on("inbound", (frame: MeshFrame) => inbound.push(frame));
  const result = await alice.send({ to: "bob", message: "early answer", awaitReply: true });
  assert.equal(result.status, "reply");
  if (result.status === "reply") assert.equal(result.response, "answer before ACK");
  assert.deepEqual(inbound, []);
  assert.equal(alice.isReloadSafe(), false, "the real ACK transaction is still active");
  held!.socket.write(encodeFrame(buildFrame({
    type: "ack", id: held!.frame.id, status: "delivered",
  }), DEFAULT_MAX_FRAME_BYTES));
  await waitFor(() => alice.isReloadSafe());
  assert.equal((await alice.waitAll(1000)).status, "complete");
});

for (const mode of ["launch", "unawaited"] as const) {
  it(`yields the foreground ${mode} ACK phase without pretending the written request was cancelled`, { timeout: 5000 }, async (t) => {
    let held: { frame: MeshFrame; socket: net.Socket } | undefined;
    const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "msg") held = { frame, socket };
    });
    let acknowledged = false;
    alice.on("frame", (frame: MeshFrame) => {
      if (frame.type === "ack" && frame.id === held?.frame.id) acknowledged = true;
    });
    const sending = alice.send({ to: "bob", message: "foreground ACK",
      ...(mode === "launch" ? { awaitReply: true, block: false } : {}) });
    await waitFor(() => held !== undefined);
    assert.equal(alice.interruptWaits("inbound_message"), true);
    const result = await sending;
    assert.equal(result.status, "interrupted");
    if (result.status === "interrupted") {
      assert.equal(result.delivery, "unknown");
      assert.equal(result.tracked, mode === "launch");
      assert.equal(result.msgId, held!.frame.id);
    }
    held!.socket.write(encodeFrame(buildFrame({
      type: "ack", id: held!.frame.id, status: "delivered",
    }), DEFAULT_MAX_FRAME_BYTES));
    await waitFor(() => acknowledged);
    const inbound: MeshFrame[] = [];
    alice.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    held!.socket.write(encodeFrame(buildFrame({
      type: "reply", from: "bob", to: "alice", room: "default", replyTo: held!.frame.id, body: "real late outcome",
    }), DEFAULT_MAX_FRAME_BYTES));
    await waitFor(() => inbound.length === 1);
    assert.equal(inbound[0]!.body, "real late outcome");
    assert.equal(alice.pendingCount, 0);
  });
}

for (const mode of ["blocking", "launch"] as const) {
  it(`an offline event before the initial queued ACK does not reject a ${mode} mission`, { timeout: 5000 }, async (t) => {
    let held: { frame: MeshFrame; socket: net.Socket } | undefined;
    const { alice, frames } = await rawLifecycleFixture(t, (frame, socket) => {
      if (frame.type === "hello") {
        socket.write(encodeFrame(buildFrame({ type: "presence", from: "bob", room: "default", status: "online" }), DEFAULT_MAX_FRAME_BYTES));
      } else if (frame.type === "msg") {
        held = { frame, socket };
        socket.write(encodeFrame(buildFrame({ type: "presence", from: "bob", room: "default", status: "offline" }), DEFAULT_MAX_FRAME_BYTES));
      }
    });
    await alice.connect();
    await waitFor(() => alice.knowsPeer("bob"));
    let settled = false;
    let acknowledged = false;
    alice.on("frame", (frame: MeshFrame) => {
      if (frame.type === "ack" && frame.id === held?.frame.id) acknowledged = true;
    });
    const sending = alice.send({
      to: "bob", message: "queued during departure", awaitReply: true,
      block: mode !== "launch", timeoutMs: 3000,
    }).then((result) => { settled = true; return result; });
    await waitFor(() => held !== undefined && !alice.knowsPeer("bob"));
    assert.equal(settled, false, "availability must not overtake the delivery acknowledgment");
    const waiting = alice.waitAll(2000);
    held!.socket.write(encodeFrame(buildFrame({ type: "ack", id: held!.frame.id, status: "queued_offline" }), DEFAULT_MAX_FRAME_BYTES));
    await waitFor(() => acknowledged);
    if (mode === "blocking") assert.equal(settled, false);
    else assert.equal((await sending).status, "queued_offline");
    held!.socket.write(encodeFrame(buildFrame({
      type: "reply", from: "bob", to: "alice", room: "default", replyTo: held!.frame.id, body: "queued task finished",
    }), DEFAULT_MAX_FRAME_BYTES));
    const verdict = await waiting;
    assert.equal(verdict.status, "complete");
    assert.equal(verdict.answers[0]?.response, "queued task finished");
    if (mode === "blocking") assert.equal((await sending).status, "reply");
    assert.equal(frames.filter((frame) => frame.type === "msg").length, 1);
  });
}

it("a delivered ACK does not erase a departure observed while awaiting that ACK", { timeout: 5000 }, async (t) => {
  let held: { frame: MeshFrame; socket: net.Socket } | undefined;
  const { alice } = await rawLifecycleFixture(t, (frame, socket) => {
    if (frame.type === "hello") {
      socket.write(encodeFrame(buildFrame({ type: "presence", from: "bob", room: "default", status: "online" }), DEFAULT_MAX_FRAME_BYTES));
    } else if (frame.type === "msg") {
      held = { frame, socket };
      socket.write(encodeFrame(buildFrame({ type: "presence", from: "bob", room: "default", status: "offline" }), DEFAULT_MAX_FRAME_BYTES));
    }
  });
  await alice.connect();
  await waitFor(() => alice.knowsPeer("bob"));
  const sending = alice.send({ to: "bob", message: "delivered before departure", awaitReply: true, timeoutMs: 3000 });
  await waitFor(() => held !== undefined && !alice.knowsPeer("bob"));
  const waiting = alice.waitAll(2000);
  held!.socket.write(encodeFrame(buildFrame({ type: "ack", id: held!.frame.id, status: "delivered" }), DEFAULT_MAX_FRAME_BYTES));
  const result = await sending;
  assert.equal(result.status, "error");
  if (result.status === "error") {
    assert.equal(result.reason, "target_offline");
    assert.equal(result.delivery, "delivered");
    assert.equal(result.tracked, true);
  }
  assert.equal((await waiting).reason, "target_offline");
  assert.equal(alice.pendingCount, 1);
  held!.socket.write(encodeFrame(buildFrame({ type: "presence", from: "bob", room: "default", status: "online" }), DEFAULT_MAX_FRAME_BYTES));
  await waitFor(() => alice.knowsPeer("bob"));
  const resumed = alice.waitAll(1000);
  held!.socket.write(encodeFrame(buildFrame({
    type: "reply", from: "bob", to: "alice", room: "default", replyTo: held!.frame.id, body: "answer after recovery",
  }), DEFAULT_MAX_FRAME_BYTES));
  assert.equal((await resumed).status, "complete");
});
