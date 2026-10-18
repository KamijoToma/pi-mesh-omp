// test/tools-send.test.ts — A8 regression: mesh_send ledger records must store
// the CANONICAL alias. A model calling mesh_send with to='@Bob' must produce
// 'sent' (and post-ack) records with to='bob' (and details.to='bob'), so
// ledger predicates are insensitive to how the alias was typed.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { MeshLedger, type LedgerRecord } from "../src/extension/ledger.js";
import type { ExtensionAPI, SessionContext, ToolDefinition } from "../src/extension/pi-types.js";
import { armReserveTimers, clearReserveTimers, registerTools, type MeshRuntime } from "../src/extension/tools.js";
import { MeshClient, type SendResult } from "../src/client/client.js";
import { MeshIdentity } from "../src/shared/identity-store.js";
import { buildFrame } from "../src/protocol/envelope.js";
import { makeTempDirs, startTestBroker, waitFor } from "./helpers.js";

function makeRuntime(stateDir: string, sendResult: SendResult, rooms: string[] = ["default"]): { rt: MeshRuntime; ledger: MeshLedger } {
  const ledger = new MeshLedger(stateDir);
  const rt = {
    client: {
      alias: "alice",
      rooms,
      roomRoles: new Map(rooms.map((room) => [room, "member"])),
      isOnline: () => true,
      send: () => Promise.resolve(sendResult),
      connect: () => Promise.reject(new Error("must not connect in these tests")),
      // M1 send-guard surface
      knowsPeer: () => true,
      knownPeerList: ["bob"],
      busyForMs: () => undefined,
    },
    ledger,
    transcript: { isEnabled: () => false, record: () => {} },
    guards: { checkSend: () => ({ ok: true, warnings: [] }) },
    ctx: null,
    stateDir,
    runtimeDir: stateDir,
    startedAt: Date.now(),
    ledgerFailures: 0,
    transcriptFailures: 0,
    injectionFailures: 0,
  } as unknown as MeshRuntime;
  return { rt, ledger };
}

function toolRuntime(rt: MeshRuntime): Map<string, ToolDefinition> {
  const tools = new Map<string, ToolDefinition>();
  const pi = {
    registerTool: (def: ToolDefinition) => tools.set(def.name, def),
  } as unknown as ExtensionAPI;
  registerTools(pi, () => rt);
  return tools;
}

function ledgerRecords(ledger: MeshLedger): LedgerRecord[] {
  return readFileSync(ledger.path, "utf8")
    .split("\n")
    .filter((l) => l.trim() !== "")
    .map((l) => JSON.parse(l) as LedgerRecord);
}

describe("mesh_send ledger canonical alias (A8)", () => {
  it("to='@Bob' stores the canonical alias in ledger records and result details", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const { rt, ledger } = makeRuntime(dirs.stateDir, { status: "delivered", msgId: "m_s_11111111" });
    const tools = toolRuntime(rt);
    const tool = tools.get("mesh_send");
    assert.ok(tool, "mesh_send tool registered");

    const res = await tool.execute(
      "tc1",
      { to: "@Bob", message: "hello bob" },
      undefined,
      undefined,
      {} as unknown as SessionContext,
    );

    const recs = ledgerRecords(ledger);
    assert.deepEqual(recs.map((r) => r.event), ["sent", "delivered"]);
    for (const rec of recs) {
      assert.equal(rec.to, "bob", `ledger '${rec.event}' record must store the canonical alias`);
      assert.equal(rec.from, "alice");
      assert.equal(rec.bodyStored, false);
    }
    // details mirror the canonical alias
    assert.equal(res.details?.to, "bob");
    assert.equal(res.details?.status, "delivered");
  });

  it("no room param → resolves to the first joined room when 'default' was left", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    // session in cs-room ONLY (left 'default') — sends must NOT default to
    // "default" (the broker would refuse not_member).
    const { rt, ledger } = makeRuntime(dirs.stateDir, { status: "delivered", msgId: "m_s_22222222" }, ["cs-room"]);
    const tools = toolRuntime(rt);
    const tool = tools.get("mesh_send");
    assert.ok(tool, "mesh_send tool registered");

    const res = await tool.execute(
      "tc2",
      { to: "bob", message: "hello cs" },
      undefined,
      undefined,
      {} as unknown as SessionContext,
    );
    const recs = ledgerRecords(ledger);
    assert.equal(recs[0]?.room, "cs-room", "ledger 'sent' must record the resolved room");
    assert.equal(recs[1]?.room, "cs-room", "ledger 'delivered' must record the resolved room");
    assert.equal(res.details?.room, "cs-room");
  });

  it("explicit room param still wins over the joined rooms", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const { rt, ledger } = makeRuntime(dirs.stateDir, { status: "delivered", msgId: "m_s_33333333" }, ["cs-room"]);
    const tools = toolRuntime(rt);
    const tool = tools.get("mesh_send");
    assert.ok(tool, "mesh_send tool registered");

    await tool.execute(
      "tc3",
      { to: "bob", message: "explicit", room: "ops" },
      undefined,
      undefined,
      {} as unknown as SessionContext,
    );
    const recs = ledgerRecords(ledger);
    assert.equal(recs[0]?.room, "ops");
  });
});

describe("mesh tools require explicit room participation", () => {
  it("network tools stay blocked and local tools remain usable without broker presence", async (t) => {
    const dirs = makeTempDirs();
    const broker = await startTestBroker(dirs.runtimeDir);
    const client = new MeshClient({
      alias: "inactive", rooms: [], runtimeDir: dirs.runtimeDir, noReconnect: true,
      initialReservations: [{ pattern: "held.ts", since: new Date().toISOString() }],
    });
    t.after(async () => {
      await client.close();
      await broker.close();
      dirs.cleanup();
    });
    const { rt } = makeRuntime(dirs.stateDir, { status: "delivered", msgId: "unused" }, []);
    rt.client = client;
    rt.sessionId = "inactive-session";
    rt.identity = new MeshIdentity(dirs.stateDir);
    rt.ledger.append({ event: "blocked", from: "inactive", code: "local-history" });
    client.transcript.push(buildFrame({ type: "msg", from: "someone", body: "local frame" }));
    const tools = toolRuntime(rt);
    const execute = (name: string, params: Record<string, unknown>) =>
      tools.get(name)!.execute(name, params, undefined, undefined, {} as SessionContext);

    for (const [name, params] of [
      ["mesh_send", { to: "bob", message: "do not send" }],
      ["mesh_send", { broadcast: true, room: "default", message: "do not broadcast" }],
      ["mesh_reply", { msgId: "m_missing", message: "do not reply" }],
      ["mesh_status", { all: true }],
      ["mesh_alias", { alias: "activated" }],
      ["mesh_reserve", { paths: ["new.ts"] }],
      ["mesh_release", {}],
    ] as [string, Record<string, unknown>][]) {
      const result = await execute(name, params);
      assert.equal(result.details?.status, "blocked", name);
      assert.equal(result.details?.reason, "not_joined", name);
      assert.equal(client.isOnline(), false, name);
      assert.deepEqual([...broker.state.peers.keys()], [], name);
      assert.equal(broker.state.rooms.has("default"), false, name);
      assert.equal(broker.state.knownAliases.size, 0, name);
    }
    assert.deepEqual(client.reservations.map((reservation) => reservation.pattern), ["held.ts"]);
    assert.equal(client.alias, "inactive");
    assert.deepEqual(client.rooms, []);
    assert.equal((await execute("mesh_alias", {})).details?.alias, "inactive");
    assert.equal((await execute("mesh_history", {})).details?.count, 1);
    assert.equal((await execute("mesh_ledger", {})).details?.count, 1);
    const wait = await execute("mesh_wait_all", {});
    assert.equal(wait.details?.status, "complete");
    assert.equal(wait.details?.total, 0);
    assert.equal(client.isOnline(), false);
    assert.equal(broker.state.peers.size, 0);
  });

  it("joined clients reconnect through model send and control tools without joining default", async (t) => {
    const dirs = makeTempDirs();
    const broker = await startTestBroker(dirs.runtimeDir);
    const client = new MeshClient({
      alias: "joined", rooms: ["opt-in"], runtimeDir: dirs.runtimeDir, noReconnect: true,
    });
    const bob = new MeshClient({
      alias: "bob", rooms: ["opt-in"], runtimeDir: dirs.runtimeDir, noReconnect: true,
    });
    t.after(async () => {
      await client.close();
      await bob.close();
      await broker.close();
      dirs.cleanup();
    });
    await bob.connect();
    const { rt } = makeRuntime(dirs.stateDir, { status: "delivered", msgId: "unused" }, ["opt-in"]);
    rt.client = client;
    rt.sessionId = "joined-session";
    rt.identity = new MeshIdentity(dirs.stateDir);
    const tools = toolRuntime(rt);
    const execute = (name: string, params: Record<string, unknown>) =>
      tools.get(name)!.execute(name, params, undefined, undefined, {} as SessionContext);
    const received: string[] = [];
    bob.on("inbound", (frame) => { if (frame.body !== undefined) received.push(frame.body); });

    assert.equal((await execute("mesh_send", { to: "bob", message: "first connect" })).details?.status, "delivered");
    await waitFor(() => received.includes("first connect"));
    assert.deepEqual([...broker.state.peers.get("joined")!.rooms.keys()], ["opt-in"]);
    for (const [name, params] of [
      ["mesh_status", {}],
      ["mesh_reserve", { paths: ["joined.ts"] }],
      ["mesh_release", {}],
      ["mesh_alias", { alias: "renamed" }],
    ] as [string, Record<string, unknown>][]) {
      const previousAlias = client.alias;
      await client.close();
      await waitFor(() => !broker.state.peers.has(previousAlias));
      const result = await execute(name, params);
      assert.notEqual(result.details?.reason, "not_joined", name);
      assert.equal(client.isOnline(), true, name);
      assert.equal(broker.state.peers.has(client.alias), true, name);
      assert.deepEqual(client.rooms, ["opt-in"], name);
    }
    assert.equal(client.alias, "renamed");
    assert.deepEqual(client.reservations, []);
    assert.equal(broker.state.rooms.has("default"), false);
  });

  it("auto-release cannot reconnect after the final membership is removed", async (t) => {
    const dirs = makeTempDirs();
    const broker = await startTestBroker(dirs.runtimeDir);
    const client = new MeshClient({
      alias: "leaving", rooms: ["opt-in"], runtimeDir: dirs.runtimeDir, noReconnect: true,
    });
    const { rt } = makeRuntime(dirs.stateDir, { status: "delivered", msgId: "unused" }, ["opt-in"]);
    rt.client = client;
    rt.sessionId = "leaving-session";
    rt.identity = new MeshIdentity(dirs.stateDir);
    t.after(async () => {
      clearReserveTimers(rt);
      await client.close();
      await broker.close();
      dirs.cleanup();
    });
    await client.connect();
    assert.equal((await client.reserve(["held.ts"])).status, "delivered");
    await client.leave("opt-in");
    await client.close();
    await waitFor(() => !broker.state.peers.has("leaving"));
    armReserveTimers(rt, ["held.ts"], 1);
    await waitFor(() => rt.reserveTimers?.size === 0);
    assert.equal(client.isOnline(), false);
    assert.equal(broker.state.peers.size, 0);
    assert.deepEqual(client.rooms, []);
    assert.deepEqual(client.reservations, []);
    assert.deepEqual(rt.identity.load(rt.sessionId)?.reservations, []);
    await client.join("opt-in");
    assert.deepEqual(broker.state.peers.get("leaving")?.reservations, []);
  });
});
