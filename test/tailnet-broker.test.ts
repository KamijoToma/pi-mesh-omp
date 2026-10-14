import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import * as fs from "node:fs";
import path from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { createBroker } from "../src/broker/broker.js";
import { MeshClient } from "../src/client/client.js";
import { DEFAULT_POLICY } from "../src/broker/policy.js";
import { parseFrameLine } from "../src/protocol/envelope.js";
import { DEFAULT_CONFIG, MAX_BROKER_ID_CHARS } from "../src/shared/config.js";
import { loadOrCreateBrokerId } from "../src/shared/broker-identity.js";
import { socketPathForDir } from "../src/shared/paths.js";
import { makeTempDirs, RawClient, startTestBroker, waitFor, type TempDirs } from "./helpers.js";

const brokerEntry = fileURLToPath(new URL("../src/broker/broker.js", import.meta.url));
const identityEntry = fileURLToPath(new URL("./fixtures/broker-identity-initializer.js", import.meta.url));

function standalone(dirs: TempDirs, extra: NodeJS.ProcessEnv = {}) {
  const env: NodeJS.ProcessEnv = { ...process.env };
  for (const key of Object.keys(env)) if (key.startsWith("MESH_")) delete env[key];
  Object.assign(env, { MESH_RUNTIME_DIR: dirs.runtimeDir, MESH_STATE_DIR: dirs.stateDir, MESH_ID: "team-one", ...extra });
  const child = spawn(process.execPath, [brokerEntry], { env, cwd: dirs.root, stdio: ["ignore", "pipe", "pipe"] });
  let out = "";
  let err = "";
  child.stdout.on("data", (chunk: Buffer) => { out += chunk.toString("utf8"); });
  child.stderr.on("data", (chunk: Buffer) => { err += chunk.toString("utf8"); });
  // Real child processes cannot use fake timers; this is a failure-only
  // watchdog, while readiness and shutdown await process/socket events.
  const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
  const done = once(child, "close").then(([code]) => code as number | null).finally(() => clearTimeout(timer));
  return {
    done,
    ready: async () => {
      await waitFor(() => {
        if (child.exitCode !== null) throw new Error(`broker exited: ${err}`);
        return out.includes("mesh broker up") || undefined;
      });
    },
    get err() { return err; },
    stop: async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
      await done;
    },
  };
}

function identityInitializer(stateDir: string, mode: "interrupt" | "concurrent") {
  const child = spawn(process.execPath, [identityEntry, stateDir, mode], { stdio: ["ignore", "pipe", "pipe", "ipc"] });
  let out = "";
  let err = "";
  child.stdout!.on("data", (chunk: Buffer) => { out += chunk.toString("utf8"); });
  child.stderr!.on("data", (chunk: Buffer) => { err += chunk.toString("utf8"); });
  // Real processes await IPC/exit events; fake timers cannot bound a hung OS child.
  // This watchdog only kills a failed run and never delays a successful test.
  const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
  const done = once(child, "close").then(([code]) => code as number | null).finally(() => clearTimeout(timer));
  const ready = Promise.race([
    once(child, "message").then(([message]) => { assert.equal(message, "ready"); }),
    done.then(() => { throw new Error(`identity initializer exited before readiness: ${err}`); }),
  ]);
  void ready.catch(() => {});
  return {
    done,
    ready,
    get out() { return out; },
    get err() { return err; },
    start: async () => {
      await ready;
      child.send("start");
    },
    stop: async () => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await done.catch(() => {});
    },
  };
}

test("wrong mesh cannot register, announce itself or consume an offline mailbox", async () => {
  const dirs = makeTempDirs("mesh-tailnet-mail-");
  const broker = await startTestBroker(dirs.runtimeDir, { config: { meshId: "team-one", brokerId: "broker-one" } });
  const sender = await RawClient.connect(broker.socketPath);
  const recipient = await RawClient.connect(broker.socketPath);
  let wrong: RawClient | undefined;
  let correct: RawClient | undefined;
  try {
    sender.hello("sender"); // Legacy hello remains compatible with a named mesh.
    const senderWelcome = await sender.waitFrame((f) => f.type === "welcome");
    assert.equal(senderWelcome.broker?.meshId, "team-one");
    assert.equal(senderWelcome.broker?.brokerId, "broker-one");
    recipient.hello("recipient");
    await recipient.waitFrame((f) => f.type === "welcome");
    recipient.close();
    await sender.waitFrame((f) => f.type === "presence" && f.from === "recipient" && f.status === "offline");
    sender.send({ type: "msg", id: "offline-message", from: "sender", to: "recipient", room: "default", body: "private mailbox" });
    assert.equal((await sender.waitFrame((f) => f.type === "ack" && f.id === "offline-message")).status, "queued_offline");
    wrong = await RawClient.connect(broker.socketPath);
    wrong.send({ type: "hello", from: "recipient", meshId: "team-two", rooms: ["wrong-room"], reservations: [{ pattern: "secret.ts" }] });
    assert.equal((await wrong.waitFrame((f) => f.type === "error")).code, "mesh_mismatch");
    await wrong.waitClosed();
    assert.equal(wrong.frames.some((f) => f.type === "welcome" || f.type === "mailbox" || f.body !== undefined), false);
    sender.send({ type: "status_req", id: "after-mismatch", from: "sender" });
    const status = await sender.waitFrame((f) => f.type === "status_res" && f.id === "after-mismatch");
    assert.equal(status.peers?.some((p) => p.alias === "recipient"), false);
    assert.equal(status.rooms?.includes("wrong-room"), false);
    assert.deepEqual(status.broker, senderWelcome.broker);
    correct = await RawClient.connect(broker.socketPath);
    correct.send({ type: "hello", from: "recipient", meshId: "team-one" });
    assert.equal((await correct.waitFrame((f) => f.type === "welcome")).mailboxCount, 1);
    assert.equal((await correct.waitFrame((f) => f.type === "mailbox")).body, "private mailbox");
  } finally {
    sender.close(); recipient.close(); wrong?.close(); correct?.close();
    await broker.close(); dirs.cleanup();
  }
});

test("malformed hello metadata is refused before registration and a legacy hello can still join", async () => {
  const dirs = makeTempDirs("mesh-tailnet-wire-");
  const broker = await startTestBroker(dirs.runtimeDir);
  const observer = await RawClient.connect(broker.socketPath);
  const candidate = await RawClient.connect(broker.socketPath);
  try {
    observer.hello("observer");
    const welcome = await observer.waitFrame((f) => f.type === "welcome");
    assert.equal(welcome.broker?.meshId, "default");
    assert.ok(welcome.broker?.brokerId);
    assert.ok(welcome.broker?.bootId);
    for (const [index, metadata] of [
      { meshId: "m".repeat(65) },
      { broker: { meshId: "default", brokerId: "claimed", bootId: "claimed" } },
    ].entries()) {
      const before = candidate.frames.length;
      candidate.sendRaw(JSON.stringify({
        v: 1, type: "hello", id: `bad-${index}`, ts: "2026-01-01T00:00:00.000Z",
        from: "candidate", rooms: ["invalid-selection"], ...metadata,
      }) + "\n");
      const response = await waitFor(() => candidate.frames.slice(before).find((f) => f.type === "error"));
      assert.equal(response.code, "invalid_frame");
      observer.send({ type: "status_req", id: `snapshot-${index}`, from: "observer" });
      const snapshot = await observer.waitFrame((f) => f.type === "status_res" && f.id === `snapshot-${index}`);
      assert.equal(snapshot.peers?.some((p) => p.alias === "candidate"), false);
      assert.equal(snapshot.rooms?.includes("invalid-selection"), false);
    }
    candidate.hello("candidate");
    assert.equal((await candidate.waitFrame((f) => f.type === "welcome")).broker?.meshId, "default");
  } finally {
    observer.close(); candidate.close(); await broker.close(); dirs.cleanup();
  }
});

test("TCP authentication precedes mesh mismatch and local socket remains tokenless", async () => {
  const dirs = makeTempDirs("mesh-tailnet-token-");
  const broker = await createBroker({
    config: { ...DEFAULT_CONFIG, meshId: "team-one", brokerToken: "secret" }, policy: DEFAULT_POLICY,
    socketPath: socketPathForDir(dirs.runtimeDir), tcpListen: { host: "127.0.0.1", port: 0, tls: false }, alsoUnix: true,
  });
  const local = await RawClient.connect(socketPathForDir(dirs.runtimeDir));
  let remote: MeshClient | undefined;
  try {
    const addr = broker.server.address();
    assert.ok(addr && typeof addr === "object");
    const brokerUrl = `tcp://127.0.0.1:${addr.port}`;
    remote = new MeshClient({ alias: "wrong-token", noReconnect: true, config: { brokerUrl, meshId: "team-two", brokerToken: "wrong" } });
    await assert.rejects(() => remote!.connect(), /invalid_token/);
    await remote.close();
    remote = new MeshClient({ alias: "wrong-mesh", noReconnect: true, config: { brokerUrl, meshId: "team-two", brokerToken: "secret" } });
    await assert.rejects(() => remote!.connect(), /mesh_mismatch/);
    await remote.close();
    remote = new MeshClient({ alias: "right-mesh", noReconnect: true, config: { brokerUrl, meshId: "team-one", brokerToken: "secret" } });
    assert.equal((await remote.connect()).alias, "right-mesh");
    local.send({ type: "hello", from: "local-peer", meshId: "team-one" });
    assert.equal((await local.waitFrame((f) => f.type === "welcome")).broker?.meshId, "team-one");
  } finally {
    local.close(); await remote?.close(); await broker.close(); dirs.cleanup();
  }
});

test("standalone restart preserves stable ID, changes boot ID, and explicit ID overrides without rewriting", async () => {
  const dirs = makeTempDirs("mesh-tailnet-restart-");
  let host = standalone(dirs);
  let client: RawClient | undefined;
  try {
    await host.ready();
    client = await RawClient.connect(socketPathForDir(dirs.runtimeDir));
    client.hello("first-client");
    const first = (await client.waitFrame((f) => f.type === "welcome")).broker;
    assert.ok(first);
    const persisted = fs.readFileSync(path.join(dirs.stateDir, "broker-id"), "utf8");
    assert.equal(persisted.trim(), first.brokerId);
    client.close(); await host.stop();
    host = standalone(dirs);
    await host.ready();
    client = await RawClient.connect(socketPathForDir(dirs.runtimeDir));
    client.hello("second-client");
    const second = (await client.waitFrame((f) => f.type === "welcome")).broker;
    assert.ok(second);
    assert.equal(second.brokerId, first.brokerId);
    assert.notEqual(second.bootId, first.bootId);
    assert.equal(second.meshId, "team-one");
    client.send({ type: "status_req", from: "second-client" });
    assert.deepEqual((await client.waitFrame((f) => f.type === "status_res")).broker, second);
    client.close(); await host.stop();
    host = standalone(dirs, { MESH_BROKER_ID: "explicit-broker" });
    await host.ready();
    client = await RawClient.connect(socketPathForDir(dirs.runtimeDir));
    client.hello("third-client");
    assert.equal((await client.waitFrame((f) => f.type === "welcome")).broker?.brokerId, "explicit-broker");
    assert.equal(fs.readFileSync(path.join(dirs.stateDir, "broker-id"), "utf8"), persisted);
  } finally {
    client?.close(); await host.stop(); dirs.cleanup();
  }
});

test("an initializer interrupted before writing cannot strand the next standalone startup", async () => {
  const dirs = makeTempDirs("mesh-tailnet-interrupted-id-");
  const initializer = identityInitializer(dirs.stateDir, "interrupt");
  try {
    await initializer.start();
    assert.equal(await initializer.done, 73, initializer.err);
    const host = standalone(dirs);
    let client: RawClient | undefined;
    try {
      await host.ready();
      client = await RawClient.connect(socketPathForDir(dirs.runtimeDir));
      client.hello("after-interruption");
      const identity = (await client.waitFrame((f) => f.type === "welcome")).broker;
      assert.ok(identity);
      assert.equal(fs.readFileSync(path.join(dirs.stateDir, "broker-id"), "utf8"), `${identity.brokerId}\n`);
      assert.equal(loadOrCreateBrokerId(dirs.stateDir), identity.brokerId);
    } finally {
      client?.close(); await host.stop();
    }
  } finally {
    await initializer.stop(); dirs.cleanup();
  }
});

test("concurrent initializer processes observe one complete stable identity", async () => {
  const dirs = makeTempDirs("mesh-tailnet-concurrent-id-");
  const initializers = Array.from({ length: 8 }, () => identityInitializer(dirs.stateDir, "concurrent"));
  try {
    await Promise.all(initializers.map((initializer) => initializer.ready));
    await Promise.all(initializers.map((initializer) => initializer.start()));
    const codes = await Promise.all(initializers.map((initializer) => initializer.done));
    for (const [index, code] of codes.entries()) assert.equal(code, 0, initializers[index]?.err);
    const ids = initializers.map((initializer) => initializer.out.trim());
    const [winner] = ids;
    assert.ok(winner);
    assert.equal(new Set(ids).size, 1);
    assert.equal(fs.readFileSync(path.join(dirs.stateDir, "broker-id"), "utf8"), `${winner}\n`);
    assert.equal(loadOrCreateBrokerId(dirs.stateDir), winner);
  } finally {
    await Promise.all(initializers.map((initializer) => initializer.stop())); dirs.cleanup();
  }
});

test("malformed persisted ID fails standalone startup rather than silently rotating identity", async () => {
  const dirs = makeTempDirs("mesh-tailnet-corrupt-");
  const target = path.join(dirs.stateDir, "broker-id");
  try {
    for (const contents of ["", "bad id\n", "a".repeat(MAX_BROKER_ID_CHARS + 1)]) {
      fs.writeFileSync(target, contents);
      const host = standalone(dirs);
      try {
        assert.equal(await host.done, 1);
        assert.match(host.err, /broker_identity_invalid/);
        assert.equal(fs.readFileSync(target, "utf8"), contents);
      } finally { await host.stop(); }
    }
  } finally { dirs.cleanup(); }
});

test("a symlinked persisted identity fails standalone startup without accepting its target", async (t) => {
  const dirs = makeTempDirs("mesh-tailnet-symlink-id-");
  const outside = path.join(dirs.root, "outside-id");
  const target = path.join(dirs.stateDir, "broker-id");
  try {
    fs.writeFileSync(outside, "outside-broker\n");
    try {
      fs.symlinkSync(outside, target, "file");
    } catch (error) {
      if (process.platform === "win32" && error instanceof Error && "code" in error && error.code === "EPERM") {
        t.skip("Windows requires symlink privileges or developer mode");
        return;
      }
      throw error;
    }
    const host = standalone(dirs);
    try {
      assert.equal(await host.done, 1);
      assert.match(host.err, /broker_identity_invalid/);
      assert.equal(fs.readFileSync(outside, "utf8"), "outside-broker\n");
      assert.equal(fs.lstatSync(target).isSymbolicLink(), true);
      assert.equal(loadOrCreateBrokerId(dirs.stateDir, "configured-broker"), "configured-broker");
    } finally { await host.stop(); }
  } finally { dirs.cleanup(); }
});

test("identity creation is exclusive and bounded; explicit override never reads or creates state", () => {
  const dirs = makeTempDirs("mesh-tailnet-id-");
  const target = path.join(dirs.stateDir, "broker-id");
  try {
    const first = loadOrCreateBrokerId(dirs.stateDir);
    assert.equal(loadOrCreateBrokerId(dirs.stateDir), first);
    if (process.platform !== "win32") assert.equal(fs.statSync(target).mode & 0o777, 0o600);
    fs.writeFileSync(target, "a".repeat(MAX_BROKER_ID_CHARS + 1));
    assert.throws(() => loadOrCreateBrokerId(dirs.stateDir), /broker_identity_invalid/);
    assert.equal(loadOrCreateBrokerId(dirs.stateDir, "configured-broker"), "configured-broker");
    const absent = path.join(dirs.root, "not-created");
    assert.equal(loadOrCreateBrokerId(absent, "configured-broker"), "configured-broker");
    assert.equal(fs.existsSync(absent), false);
  } finally { dirs.cleanup(); }
});

test("NDJSON consumer rejects malformed, oversized and wrong-frame broker metadata", () => {
  const valid = { meshId: "team-one", brokerId: "broker-one", bootId: "boot-one" };
  const malformed: unknown[] = [
    null, [], "broker", {}, { ...valid, meshId: 1 }, { ...valid, meshId: "" },
    { ...valid, meshId: "m".repeat(65) }, { ...valid, brokerId: "bad id" },
    { ...valid, brokerId: "b".repeat(MAX_BROKER_ID_CHARS + 1) },
    { ...valid, bootId: "" }, { ...valid, bootId: "b".repeat(MAX_BROKER_ID_CHARS + 1) },
  ];
  const wire = (type: string, metadata: Record<string, unknown>) => JSON.stringify({ v: 1, type, id: "wire-id", ts: "2026-01-01T00:00:00.000Z", ...metadata });
  for (const type of ["welcome", "status_res"]) {
    const good = parseFrameLine(wire(type, { broker: valid }));
    assert.equal(good.ok, true);
    for (const broker of malformed) assert.equal(parseFrameLine(wire(type, { broker })).ok, false);
  }
  for (const type of ["hello", "msg", "ping", "error"]) {
    assert.equal(parseFrameLine(wire(type, { broker: valid })).ok, false);
  }
  assert.equal(parseFrameLine(wire("hello", { meshId: "team-one" })).ok, true);
  for (const meshId of [null, "", "bad mesh", "m".repeat(65), 1]) {
    assert.equal(parseFrameLine(wire("hello", { meshId })).ok, false);
  }
  assert.equal(parseFrameLine(wire("welcome", { meshId: "team-one" })).ok, false);
  assert.equal(parseFrameLine(wire("error", { code: "mesh_mismatch" })).ok, true);
});
