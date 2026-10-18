// test/commands.test.ts — /mesh parsing and explicit room activation.
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { socketPathForDir } from "../src/shared/paths.js";
import { parseJoinArgs, registerCommands } from "../src/extension/commands.js";
import { MeshClient } from "../src/client/client.js";
import { MeshIdentity } from "../src/shared/identity-store.js";
import { DEFAULT_CONFIG } from "../src/shared/config.js";
import type { CommandDefinition, ExtensionAPI, SessionContext } from "../src/extension/pi-types.js";
import type { MeshRuntime } from "../src/extension/tools.js";
import { MeshTranscript } from "../src/extension/transcript.js";
import { MeshLedger } from "../src/extension/ledger.js";
import { MeshGuards } from "../src/extension/guards.js";
import { makeTempDirs, startTestBroker, waitFor } from "./helpers.js";

describe("parseJoinArgs", () => {
  it("plain room", () => {
    assert.deepEqual(parseJoinArgs(["ops"]), { room: "ops", observer: false });
  });

  it("room + observer", () => {
    assert.deepEqual(parseJoinArgs(["ops", "observer"]), { room: "ops", observer: true });
  });

  it("room as alias", () => {
    assert.deepEqual(parseJoinArgs(["ops", "as", "agent-1"]), {
      room: "ops",
      asAlias: "agent-1",
      observer: false,
    });
  });

  it("room as alias observer", () => {
    assert.deepEqual(parseJoinArgs(["ops", "as", "agent-1", "observer"]), {
      room: "ops",
      asAlias: "agent-1",
      observer: true,
    });
  });

  it("as alias without room (alias-only)", () => {
    assert.deepEqual(parseJoinArgs(["as", "agent-1"]), {
      asAlias: "agent-1",
      observer: false,
    });
  });

  it("no args", () => {
    assert.deepEqual(parseJoinArgs([]), { room: undefined, observer: false });
  });
});

async function commandFixture() {
  const dirs = makeTempDirs("mesh-commands-");
  const broker = await startTestBroker(dirs.runtimeDir);
  const observer = new MeshClient({ alias: "command-observer", runtimeDir: dirs.runtimeDir });
  await observer.connect();
  const messages: string[] = [];
  const ctx = {
    cwd: dirs.root,
    ui: {
      notify: (message: string) => { messages.push(message); },
      setWidget: () => {},
      setStatus: () => {},
    },
    sessionManager: { getSessionId: () => "commands" },
  } as SessionContext;
  const rt: MeshRuntime = {
    client: new MeshClient({ alias: "command-agent", rooms: [], runtimeDir: dirs.runtimeDir }),
    identity: new MeshIdentity(dirs.stateDir),
    transcript: new MeshTranscript(dirs.stateDir, false),
    ledger: new MeshLedger(dirs.stateDir),
    guards: new MeshGuards(),
    startedAt: Date.now(),
    ledgerFailures: 0,
    transcriptFailures: 0,
    injectionFailures: 0,
    stateDir: dirs.stateDir,
    runtimeDir: dirs.runtimeDir,
    sessionId: "commands",
    ctx,
  };
  let command!: CommandDefinition;
  const pi = {
    registerCommand: (_name: string, definition: CommandDefinition) => { command = definition; },
    events: { emit: () => {} },
  } as unknown as ExtensionAPI;
  registerCommands(pi, () => rt);
  return {
    rt, messages, broker,
    command: (args: string) => command.handler(args, ctx),
    presence: async () => (await observer.status()).peers.filter((peer) => peer.alias !== observer.alias),
    close: async () => {
      rt.markDetached?.();
      rt.stopHold?.();
      await rt.client.close();
      await observer.close();
      await broker.close();
      dirs.cleanup();
    },
  };
}

describe("explicit command activation", () => {
  it("keeps network commands and malformed joins offline without renaming", async (t) => {
    const f = await commandFixture();
    t.after(f.close);
    for (const args of ["status", "status ops", "alias renamed", "ping peer", "leave ops"]) {
      await f.command(args);
    }
    for (const args of ["join", "join as renamed", "switch as renamed", "join bad/room as renamed"]) {
      await f.command(args);
    }
    for (const args of ["alias", "help", "broker", "inbox", "log"]) await f.command(args);
    assert.equal(f.rt.client.alias, "command-agent");
    assert.deepEqual(f.rt.client.rooms, []);
    assert.deepEqual(await f.presence(), []);
  });

  it("explicit switch activates only its destination and preserves role/alias semantics", async (t) => {
    const f = await commandFixture();
    t.after(f.close);
    await f.command("switch ops as joined-agent observer");
    assert.equal(f.rt.client.alias, "joined-agent");
    assert.deepEqual(f.rt.client.rooms, ["ops"]);
    assert.equal(f.rt.client.roomRoles.get("ops"), "observer");
    assert.deepEqual((await f.presence()).map((peer) => peer.rooms), [["ops"]]);
    await f.command("join extra");
    assert.deepEqual(f.rt.client.rooms, ["ops", "extra"]);
    await f.command("switch final");
    assert.deepEqual(f.rt.client.rooms, ["final"]);
    await f.command("leave final");
    assert.deepEqual(f.rt.client.rooms, []);
    assert.equal(f.rt.client.isOnline(), false);
    await waitFor(() => !f.broker.state.peers.has("joined-agent"));
    await f.command("status");
    assert.deepEqual(await f.presence(), []);
  });

  it("closes a failed first activation rather than leaving reconnect running", async (t) => {
    const f = await commandFixture();
    t.after(f.close);
    t.mock.method(f.rt.client, "join", async () => { throw new Error("join_refused"); });
    await f.command("join ops");
    assert.equal(f.rt.client.isOnline(), false);
    assert.deepEqual(f.rt.client.rooms, []);
    await waitFor(() => !f.broker.state.peers.has("command-agent"));
    await f.command("status");
    assert.deepEqual(await f.presence(), []);
  });

  it("defers a saved tailnet selection until a valid explicit room join", async (t) => {
    const f = await commandFixture();
    t.after(f.close);
    const remoteDirs = makeTempDirs("mesh-command-selected-");
    const remote = await startTestBroker(remoteDirs.runtimeDir, { config: { meshId: "selected" } });
    const receiver = new MeshClient({
      alias: "selected-peer", rooms: ["selected-room"], runtimeDir: remoteDirs.runtimeDir,
    });
    t.after(async () => {
      await receiver.close();
      await remote.close();
      remoteDirs.cleanup();
    });
    await receiver.connect();
    f.rt.pendingTailnetConfig = {
      ...DEFAULT_CONFIG, meshId: "selected", brokerUrl: `unix://${socketPathForDir(remoteDirs.runtimeDir)}`,
    };
    await f.command("join as renamed");
    await f.command("status");
    assert.deepEqual(await f.presence(), []);
    assert.equal(remote.state.peers.has("command-agent"), false);
    await f.command("join selected-room");
    const incoming = once(receiver, "inbound");
    const sent = await f.rt.client.send({ to: receiver.alias, message: "selected broker only" });
    assert.equal(sent.status, "delivered");
    const [frame] = await incoming;
    assert.equal(frame.body, "selected broker only");
    assert.equal(frame.room, "selected-room");
    assert.deepEqual(await f.presence(), []);
  });

  it("resets an inactive identity without connecting or restoring default rooms", async (t) => {
    const f = await commandFixture();
    t.after(f.close);
    await f.command("reset");
    assert.equal(f.rt.client.isOnline(), false);
    assert.deepEqual(f.rt.client.rooms, []);
    assert.deepEqual(f.rt.identity.load(f.rt.sessionId)?.rooms, []);
    assert.deepEqual(await f.presence(), []);
  });
});
