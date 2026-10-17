import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { attachClientListeners, exposeAlias } from "../src/extension/attach.js";

// ---- D41: detached guard — client frames after reload/switch never crash ----
import { MeshClient } from "../src/client/client.js";
import type { MeshFrame } from "../src/protocol/envelope.js";

describe("attachClientListeners (D41): stale ctx after reload must not throw", () => {
  it("presence/inbound frames on a detached client are swallowed, no pi.* call", () => {
    const calls: string[] = [];
    const pi = {
      appendEntry: (..._a: unknown[]) => {
        calls.push("appendEntry");
        throw new Error("This extension ctx is stale after session replacement or reload.");
      },
      sendMessage: (..._a: unknown[]) => {
        calls.push("sendMessage");
        throw new Error("This extension ctx is stale after session replacement or reload.");
      },
      on: () => {},
    };
    const client = new MeshClient({ alias: "stale-test" });
    const rt: {
      client: MeshClient;
      ledger: { append: () => void };
      transcript: { record: () => void };
      counters: { ledgerFailures: number; transcriptFailures: number; injectionFailures: number };
      markDetached?: () => void;
      stopHold?: () => void;
    } = {
      client,
      ledger: { append: () => {} },
      transcript: { record: () => {} },
      counters: { ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0 },
    };
    attachClientListeners(
      pi as never,
      rt as never,
      () => null,
      {} as never,
      client as never,
      () => {},
    );
    // simulate session_shutdown → detach
    rt.markDetached!();
    const presence: MeshFrame = {
      v: 1, type: "presence", id: "p1", from: "someone", status: "online", room: "default",
      ts: new Date().toISOString(),
    } as unknown as MeshFrame;
    const msg: MeshFrame = {
      v: 1, type: "msg", id: "m1", from: "someone", to: "stale-test", body: "hi",
      room: "default", ts: new Date().toISOString(),
    } as unknown as MeshFrame;
    client.emit("presence", presence);
    client.emit("inbound", msg);
    client.emit("ready", { alias: "stale-test", rooms: ["default"], peers: [], mailboxCount: 0 });
    // no throw, no pi.* call attempted while detached
    assert.deepEqual(calls, []);
    client.close();
  });

  it("NOT detached: a throwing ctx is caught, never an uncaughtException", () => {
    const pi = {
      appendEntry: () => {
        throw new Error("stale ctx");
      },
      sendMessage: () => {},
      on: () => {},
    };
    const client = new MeshClient({ alias: "stale-test-2" });
    const rt = {
      client,
      ledger: { append: () => {} },
      transcript: { record: () => {} },
      counters: { ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0 },
    };
    attachClientListeners(pi as never, rt as never, () => null, {} as never, client as never, () => {});
    assert.doesNotThrow(() => {
      client.emit("presence", {
        v: 1, type: "presence", id: "p2", from: "x", status: "online", room: "default",
        ts: new Date().toISOString(),
      } as unknown as MeshFrame);
    });
    client.close();
  });
});

describe("public mesh alias bridge", () => {
  it("emits resolved alias and a rooms snapshot without triggering a turn", () => {
    const key = Symbol.for("pi-mesh:alias");
    const globals = globalThis as Record<symbol, unknown>;
    const previous = globals[key];
    const events: unknown[] = [];
    const rooms = ["default"];
    try {
      exposeAlias({ events: { emit: (event: string, data: unknown) => events.push({ event, data }) } } as never, "alice", rooms);
      rooms.push("another");
      assert.equal(globals[key], "alice");
      assert.deepEqual(events, [{ event: "mesh:alias", data: { alias: "alice", rooms: ["default"] } }]);
      exposeAlias({} as never, "bob", []); // hosts without the event bus
      assert.equal(globals[key], "bob");
    } finally {
      if (previous === undefined) delete globals[key];
      else globals[key] = previous;
    }
  });
});
