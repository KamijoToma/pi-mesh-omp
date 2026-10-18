// test/client.test.ts — MeshClient against a REAL broker: E2 (kill+restart →
// auto-reconnect → delivered), E7 (awaitReply → 2 reminds → expired), E8 (late
// reply after expiry), E23 (close during pending → shutting_down), E24 (send in
// the reconnect window succeeds after restart).
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import net from "node:net";
import path from "node:path";
import { describe, it } from "node:test";
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

  it("E23: close during pending awaitReply → error shutting_down", async (t) => {
    const { alice } = await fixture(t);
    const sendPromise = alice.send({ to: "bob", message: "hang", awaitReply: true, timeoutMs: 5000 });
    await sleep(100); // let the msg land and the pending register
    await alice.close();
    const res = await sendPromise;
    assert.equal(res.status, "error");
    if (res.status === "error") assert.equal(res.reason, "shutting_down");
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
  it("E1: broker down at send → blocked broker_unavailable, no crash (I10)", async (t) => {
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
    if (res.status === "blocked") assert.equal(res.reason, "broker_unavailable");
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
