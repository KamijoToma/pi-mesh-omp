// test/tools-reply.test.ts — mesh_reply ledger enrichment + B13 causal anchoring:
// a 'sent' record is ledgered BEFORE client.reply (mirroring mesh_send ordering),
// carrying to/room/priority peeked (read-only) from the original inbox frame, and
// the terminal 'delivered' record carries the same enrichment. Undefined keys are
// OMITTED (ledger never serializes null/undefined). E9 reply_without_target is
// refused BEFORE the network call (pre-guard on the peeked original) and writes
// NO 'sent' record. Stubs follow the extension.test.ts fake-runtime pattern.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { MeshLedger, type LedgerRecord } from "../src/extension/ledger.js";
import type { ExtensionAPI, SessionContext, ToolDefinition } from "../src/extension/pi-types.js";
import { registerTools, type MeshRuntime } from "../src/extension/tools.js";
import { buildFrame, type MeshFrame } from "../src/protocol/envelope.js";
import { sha256 } from "../src/protocol/frames.js";
import { MeshClient, type SendResult } from "../src/client/client.js";
import { makeTempDirs, startTestBroker, waitFor } from "./helpers.js";

interface StubClient {
  alias: string;
  inboxFrame: MeshFrame | undefined;
  replyResult: SendResult;
}

function makeRuntime(stateDir: string, stub: StubClient): { rt: MeshRuntime; ledger: MeshLedger } {
  const ledger = new MeshLedger(stateDir);
  const rt = {
    client: {
      alias: stub.alias,
      rooms: ["default"],
      roomRoles: new Map([["default", "member"]]),
      isOnline: () => true,
      peekInbox: (_msgId: string) => stub.inboxFrame,
      reply: (_msgId: string, _body: string, _refs?: string[]) => Promise.resolve(stub.replyResult),
      connect: () => Promise.reject(new Error("must not connect in these tests")),
    },
    ledger,
    transcript: { isEnabled: () => false, record: () => {} },
    guards: { checkReply: () => ({ warnings: [] }) } as never,
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

function execReply(tools: Map<string, ToolDefinition>, msgId: string, message: string) {
  const tool = tools.get("mesh_reply");
  assert.ok(tool, "mesh_reply tool registered");
  return tool.execute("tc1", { msgId, message }, undefined, undefined, {} as unknown as SessionContext);
}

function ledgerLines(ledger: MeshLedger): string[] {
  return readFileSync(ledger.path, "utf8")
    .split("\n")
    .filter((l) => l.trim() !== "");
}

describe("mesh_reply ledger enrichment (to/room/priority from inbox peek)", () => {
  it("known original → 'delivered' record carries to/room/priority from the inbox frame", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const orig = buildFrame({ type: "msg", from: "alice", to: "bob", room: "ops", priority: "urgent", body: "hi" });
    const stub: StubClient = {
      alias: "bob",
      inboxFrame: orig,
      replyResult: { status: "delivered", msgId: "m_r_11111111" },
    };
    const { rt, ledger } = makeRuntime(dirs.stateDir, stub);
    const tools = toolRuntime(rt);

    const res = await execReply(tools, orig.id, "ack");


    const lines = ledgerLines(ledger);
    assert.equal(lines.length, 2);
    // B13: causally-anchored 'sent' BEFORE the terminal record.
    const sent = JSON.parse(lines[0]!) as LedgerRecord;
    assert.equal(sent.event, "sent");
    assert.equal(sent.from, "bob");
    assert.equal(sent.to, "alice");
    assert.equal(sent.room, "ops");
    assert.equal(sent.priority, "urgent");
    assert.equal(sent.bodyHash, sha256("ack"));
    assert.equal(sent.bodyStored, false);
    const rec = JSON.parse(lines[1]!) as LedgerRecord;
    assert.equal(rec.event, "delivered");
    assert.equal(rec.from, "bob");
    assert.equal(rec.to, "alice");
    assert.equal(rec.room, "ops");
    assert.equal(rec.priority, "urgent");
    assert.equal(rec.bodyHash, sent.bodyHash);
    assert.equal(rec.bodyStored, false);
    // details mirror the same enrichment
    assert.equal(res.details?.to, "alice");
    assert.equal(res.details?.room, "ops");
    assert.equal(res.details?.priority, "urgent");
  });

  it("minimal original (no room/priority on the frame) → 'sent' + 'delivered' OMIT room/priority keys (no null)", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const orig = buildFrame({ type: "msg", from: "alice", to: "bob", body: "hi" });
    const stub: StubClient = {
      alias: "bob",
      inboxFrame: orig,
      replyResult: { status: "delivered", msgId: "m_r_22222222" },
    };
    const { rt, ledger } = makeRuntime(dirs.stateDir, stub);
    const tools = toolRuntime(rt);

    await execReply(tools, orig.id, "ack");

    const lines = ledgerLines(ledger);
    assert.equal(lines.length, 2);
    for (const raw of lines) {
      assert.equal(raw.includes('"room":'), false, `raw line must omit "room": ${raw}`);
      assert.equal(raw.includes('"priority":'), false, `raw line must omit "priority": ${raw}`);
    }
    const sent = JSON.parse(lines[0]!) as LedgerRecord;
    assert.equal(sent.event, "sent");
    assert.equal(sent.from, "bob");
    assert.equal(sent.to, "alice"); // to is enriched from orig.from
    const rec = JSON.parse(lines[1]!) as LedgerRecord;
    assert.equal(rec.event, "delivered");
    assert.equal(rec.from, "bob");
    assert.equal(rec.to, "alice");
  });

  it("E9: reply_without_target → blocked BEFORE the network call, NO 'sent' record, no to/room/priority", async (t) => {
    const dirs = makeTempDirs();
    t.after(() => dirs.cleanup());
    const stub: StubClient = {
      alias: "bob",
      inboxFrame: undefined,
      replyResult: { status: "error", msgId: "m_gone_99999999", reason: "reply_without_target" },
    };
    const { rt, ledger } = makeRuntime(dirs.stateDir, stub);
    const tools = toolRuntime(rt);

    const res = await execReply(tools, "m_gone_99999999", "ack");
    assert.equal(res.details?.status, "blocked");
    assert.equal(res.details?.reason, "reply_without_target");


    const lines = ledgerLines(ledger);
    assert.equal(lines.length, 1);
    const raw = lines[0]!;
    const rec = JSON.parse(raw) as LedgerRecord;
    assert.equal(rec.event, "blocked");
    assert.notEqual(rec.event, "sent", "E9 must NOT write a 'sent' record");
    assert.equal(lines.some((l) => (JSON.parse(l) as LedgerRecord).event === "sent"), false, "no 'sent' record on the E9 path");
    assert.equal(rec.code, "reply_without_target");
    assert.equal(rec.id, "m_gone_99999999");
    assert.equal(raw.includes('"to":'), false);
    assert.equal(raw.includes('"room":'), false);
    assert.equal(raw.includes('"priority":'), false);
  });
});

describe("mesh_reply participation", () => {
  it("reconnects an already joined client and delivers the exact inbound reply", async (t) => {
    const dirs = makeTempDirs();
    const broker = await startTestBroker(dirs.runtimeDir);
    const alice = new MeshClient({
      alias: "alice", rooms: ["opt-in"], runtimeDir: dirs.runtimeDir, noReconnect: true,
    });
    const bob = new MeshClient({
      alias: "bob", rooms: ["opt-in"], runtimeDir: dirs.runtimeDir, noReconnect: true,
    });
    t.after(async () => {
      await alice.close();
      await bob.close();
      await broker.close();
      dirs.cleanup();
    });
    await Promise.all([alice.connect(), bob.connect()]);
    const sent = await bob.send({ to: "alice", message: "request" });
    assert.equal(sent.status, "delivered");
    assert.ok(sent.msgId);
    await waitFor(() => alice.peekInbox(sent.msgId!) !== undefined);
    await alice.close();
    await waitFor(() => !broker.state.peers.has("alice"));
    const { rt, ledger } = makeRuntime(dirs.stateDir, {
      alias: "alice", inboxFrame: undefined,
      replyResult: { status: "delivered", msgId: "unused" },
    });
    rt.client = alice;
    const received: MeshFrame[] = [];
    bob.on("inbound", (frame: MeshFrame) => { if (frame.type === "reply") received.push(frame); });
    const result = await execReply(toolRuntime(rt), sent.msgId, "response");
    assert.equal(result.details?.status, "delivered");
    await waitFor(() => received.some((frame) => frame.replyTo === sent.msgId && frame.body === "response"));
    assert.equal(broker.state.peers.has("alice"), true);
    assert.deepEqual(alice.rooms, ["opt-in"]);
    assert.equal(broker.state.rooms.has("default"), false);
    assert.deepEqual(ledger.read().map((record) => record.event), ["sent", "delivered"]);
  });
});
