import { describe, it, type TestContext } from "node:test";
import assert from "node:assert/strict";
import childProcess, { type ChildProcess, type SpawnOptions } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import net from "node:net";
import { fileURLToPath } from "node:url";
import { createBroker, type RunningBroker } from "../src/broker/broker.js";
import { DEFAULT_POLICY } from "../src/broker/policy.js";
import { MeshClient } from "../src/client/client.js";
import { buildFrame, type BrokerInfo, type MeshFrame } from "../src/protocol/envelope.js";
import { encodeFrame } from "../src/protocol/frames.js";
import { DEFAULT_CONFIG, TAILSCALE_COMMAND_TIMEOUT_MS, TAILSCALE_MAX_OUTPUT_BYTES, TAILSCALE_MAX_SERVICE_PORTS, type MeshConfig, type TailscaleConfig } from "../src/shared/config.js";
import { clearTailnetDiscoveryCache, listTailnetServices, readTailnetStatus, resolveConnection } from "../src/tailscale/adapter.js";
import { loadConfig } from "../src/shared/config.js";
import { executeTailnetCommand } from "../src/tailscale/commands.js";
import { makeTempDirs, sleep, startTestBroker, waitFor } from "./helpers.js";

interface CliState {
  status?: unknown;
  services?: unknown;
  stderr?: string;
  exitCode?: number;
  raw?: string;
  outputBytes?: number;
  delayMs?: number;
  startedFile?: string;
}

function statusNode(id: string, hostname: string, addresses: string[], tags: string[] = [], online = true): Record<string, unknown> {
  return { ID: id, HostName: hostname, DNSName: `${hostname}.example.ts.net.`, TailscaleIPs: addresses, Tags: tags, Online: online };
}

function status(peers: Record<string, unknown> = {}, self: unknown = statusNode("self", "local", ["100.64.0.1"])): Record<string, unknown> {
  return { BackendState: "Running", Self: self, Peer: peers, CurrentTailnet: { MagicDNSEnabled: true, MagicDNSSuffix: "example.ts.net" } };
}

function service(port: number, address = "127.0.0.1", name = "svc:pi-mesh-work"): Record<string, unknown> {
  return { Name: name, DisplayName: "Mesh", Hostname: "pi-mesh-work.example.ts.net", Addrs: [address], Ports: [`tcp:${port}`] };
}

function cliFixture(t: TestContext, initial: CliState): {
  execPath: string;
  root: string;
  update: (state: CliState) => void;
  commandCount: () => number;
  cleanup: () => void;
} {
  const dirs = makeTempDirs("mesh-tailnet-cli-");
  const execPath = path.join(dirs.root, "tailscale");
  const script = fileURLToPath(new URL("./fixtures/tailscale-cli.js", import.meta.url));
  const originalSpawn = childProcess.spawn;
  const spawn = t.mock.method(childProcess, "spawn", (
    executable: string,
    args: readonly string[] = [],
    options: SpawnOptions = {},
  ): ChildProcess => {
    if (executable !== execPath) return originalSpawn(executable, args, options);
    return originalSpawn(process.execPath, [script, ...args], {
      ...options,
      env: { ...options.env, MESH_TEST_TAILSCALE_ROOT: dirs.root },
    });
  });
  t.after(() => {
    spawn.mock.restore();
    clearTailnetDiscoveryCache();
  });
  const update = (state: CliState): void => fs.writeFileSync(path.join(dirs.root, "cli-state.json"), JSON.stringify(state));
  update(initial);
  return {
    execPath,
    root: dirs.root,
    update,
    commandCount: () => {
      const callsPath = path.join(dirs.root, "calls.jsonl");
      return fs.existsSync(callsPath) ? fs.readFileSync(callsPath, "utf8").trim().split("\n").filter(Boolean).length : 0;
    },
    cleanup: dirs.cleanup,
  };
}

function config(execPath: string, port: number, mode: "service" | "node" = "service", selectors: Partial<TailscaleConfig> = {}): MeshConfig {
  return { ...DEFAULT_CONFIG, meshId: "work", brokerToken: "secret", tailscale: { mode, port, execPath, ...selectors } };
}

async function tcpBroker(meshId = "work", brokerId = "broker-work", port = 0): Promise<{ broker: RunningBroker; port: number }> {
  const broker = await createBroker({
    config: { ...DEFAULT_CONFIG, meshId, brokerId, brokerToken: "secret" },
    policy: DEFAULT_POLICY,
    tcpListen: { host: "127.0.0.1", port, tls: false },
  });
  const address = broker.server.address();
  assert.ok(address !== null && typeof address !== "string");
  return { broker, port: address.port };
}


describe("Tailscale official CLI discovery", { concurrency: false }, () => {
  it("resolves the exact Service name and its advertised TCP endpoint", async (t) => {
    const cli = cliFixture(t, { status: status(), services: [service(8712, "100.64.0.9", "svc:unrelated"), service(8712, "100.64.0.8")] });
    try {
      const target = await resolveConnection(config(cli.execPath, 8712));
      assert.equal(target.source, "tailscale-service");
      assert.equal(target.meshId, "work");
      assert.deepEqual(target.endpoint, { kind: "tcp", host: "100.64.0.8", port: 8712 });
      assert.equal(target.service?.hostname, "pi-mesh-work.example.ts.net");
    } finally { cli.cleanup(); }
  });

  it("requires the configured TCP port and rejects missing or ambiguous Services", async (t) => {
    const cli = cliFixture(t, { status: status(), services: [{ ...service(8712), Ports: ["udp:8712", "tcp:9000"] }] });
    try {
      const cfg = config(cli.execPath, 8712);
      await assert.rejects(resolveConnection(cfg), /tailscale_service_port_missing/);
      cli.update({ status: status(), services: [service(8712, "100.64.0.8", "svc:pi-mesh-work-other")] });
      await assert.rejects(resolveConnection(cfg, { refresh: true }), /tailscale_service_not_found/);
      cli.update({ status: status(), services: [service(8712), service(8712, "100.64.0.8")] });
      await assert.rejects(resolveConnection(cfg, { refresh: true }), /tailscale_service_ambiguous/);
      cli.update({ status: status(), services: [service(8712, "::1", "svc:custom")] });
      const custom = await resolveConnection(config(cli.execPath, 8712, "service", { service: "svc:custom" }), { refresh: true });
      assert.equal(custom.url, "tcp://[::1]:8712");
    } finally { cli.cleanup(); }
  });

  it("normalizes large duplicate TCP ranges before selecting a declared endpoint", { timeout: 3_000 }, async (t) => {
    const cli = cliFixture(t, {
      status: status(),
      services: [{ ...service(65_535), Ports: [...Array<string>(30_000).fill("tcp:1-65535"), "6:65535", "8710-8713", "udp:8712"] }],
    });
    try {
      const target = await resolveConnection(config(cli.execPath, 65_535));
      assert.deepEqual(target.endpoint, { kind: "tcp", host: "127.0.0.1", port: 65_535 });
      assert.equal(target.service?.ports.length, TAILSCALE_MAX_SERVICE_PORTS);
      assert.equal(target.service?.ports[0], 1);
      assert.equal(target.service?.ports.at(-1), 65_535);
      assert.equal(new Set(target.service?.ports).size, TAILSCALE_MAX_SERVICE_PORTS);
    } finally { cli.cleanup(); }
  });

  it("bounds the total expanded ports of unrelated Services with an actionable error", async (t) => {
    const cli = cliFixture(t, {
      status: status(),
      services: [
        ...Array.from({ length: 128 }, (_, index) => ({ ...service(8712, "127.0.0.1", `svc:unrelated-${index}`), Ports: ["tcp:1-65535"] })),
        service(8712),
      ],
    });
    try {
      await assert.rejects(resolveConnection(config(cli.execPath, 8712)), /tailscale_service_port_limit.*65535.*narrow/);
    } finally { cli.cleanup(); }
  });

  it("rejects malformed TCP port ranges without accepting the configured endpoint", async (t) => {
    const cli = cliFixture(t, { status: status(), services: [] });
    try {
      for (const declaration of ["tcp:0", "tcp:65536", "tcp:9000-8000", "tcp:1-999999999999999999999999999999"]) {
        cli.update({ status: status(), services: [{ ...service(8712), Ports: [declaration, "tcp:8712"] }] });
        await assert.rejects(resolveConnection(config(cli.execPath, 8712), { refresh: true }), /tailscale_invalid_json.*TCP port range/);
      }
      cli.update({ status: status(), services: [{ ...service(8712), Ports: [8712] }] });
      await assert.rejects(resolveConnection(config(cli.execPath, 8712), { refresh: true }), /tailscale_invalid_json.*string array/);
    } finally { cli.cleanup(); }
  });

  it("selects Self, exact node hostname/FQDN/IP or tags without using Online as readiness", async (t) => {
    const cli = cliFixture(t, { status: status({ a: statusNode("peer", "broker", ["100.64.0.8", "fd7a:115c:a1e0::8"], ["tag:mesh"], false) }) });
    try {
      const self = await resolveConnection(config(cli.execPath, 8712, "node", { host: "local" }));
      assert.equal(self.node?.id, "self");
      const fqdn = await resolveConnection(config(cli.execPath, 8712, "node", { host: "BROKER.EXAMPLE.TS.NET." }));
      assert.equal(fqdn.node?.id, "peer");
      assert.equal(fqdn.node?.online, false);
      const ip = await resolveConnection(config(cli.execPath, 8712, "node", { host: "fd7a:115c:a1e0::8" }));
      assert.equal(ip.url, "tcp://[fd7a:115c:a1e0::8]:8712");
      const tag = await resolveConnection(config(cli.execPath, 9000, "node", { tag: "tag:mesh" }));
      assert.deepEqual(tag.endpoint, { kind: "tcp", host: "100.64.0.8", port: 9000 });
      await assert.rejects(resolveConnection(config(cli.execPath, 8712, "node", { host: "bro" })), /tailscale_node_not_found/);
      await assert.rejects(resolveConnection(config(cli.execPath, 8712, "node")), /tailscale_node_selector_missing/);
    } finally { cli.cleanup(); }
  });

  it("fails node ambiguity including offline peers and Self", async (t) => {
    const cli = cliFixture(t, { status: status({ a: statusNode("peer", "local", ["100.64.0.8"], ["tag:mesh"], false) }, statusNode("self", "local", ["100.64.0.1"], ["tag:mesh"])) });
    try {
      await assert.rejects(resolveConnection(config(cli.execPath, 8712, "node", { host: "local" })), /tailscale_node_ambiguous/);
      await assert.rejects(resolveConnection(config(cli.execPath, 8712, "node", { tag: "tag:mesh" })), /tailscale_node_ambiguous/);
      const exact = await resolveConnection(config(cli.execPath, 8712, "node", { host: "100.64.0.8", tag: "tag:mesh" }));
      assert.equal(exact.node?.id, "peer");
    } finally { cli.cleanup(); }
  });

  it("explicit endpoints take precedence without invoking Tailscale, including invalid URLs", async (t) => {
    const cli = cliFixture(t, { exitCode: 1, stderr: "daemon unavailable" });
    try {
      const cfg = { ...config(cli.execPath, 8712), brokerUrl: "tls://example.org:9000" };
      assert.equal((await resolveConnection(cfg)).source, "explicit");
      await assert.rejects(resolveConnection({ ...cfg, brokerUrl: "https://example.org" }), /broker_endpoint_invalid/);
      assert.equal((await resolveConnection({ ...cfg, brokerUrl: undefined, tailscale: { mode: "off", port: 8712 } })).source, "local");
      assert.equal(cli.commandCount(), 0);
    } finally { cli.cleanup(); }
  });

  it("bounds commands and exposes actionable missing-command/daemon/down/JSON errors", async (t) => {
    const cli = cliFixture(t, { exitCode: 1, stderr: "failed to connect to local tailscaled; connection refused" });
    try {
      await assert.rejects(readTailnetStatus({ mode: "off", port: 8712, execPath: path.join(cli.root, "missing") }), /tailscale_command_missing.*MESH_TAILSCALE_EXEC_PATH/);
      await assert.rejects(resolveConnection(config(cli.execPath, 8712)), /tailscale_daemon_unavailable.*start/);
      cli.update({ status: { BackendState: "NeedsLogin", Self: null, Peer: null } });
      await assert.rejects(resolveConnection(config(cli.execPath, 8712)), /tailscale_down.*tailscale up/);
      clearTailnetDiscoveryCache();
      cli.update({ raw: "not json" });
      await assert.rejects(readTailnetStatus({ mode: "off", port: 8712, execPath: cli.execPath }), /tailscale_invalid_json/);
      cli.update({ status: status(), services: { "svc:pi-mesh-work": service(8712) } });
      await assert.rejects(listTailnetServices({ mode: "off", port: 8712, execPath: cli.execPath }), /service list must be an array/);
      cli.update({ outputBytes: TAILSCALE_MAX_OUTPUT_BYTES + 1 });
      await assert.rejects(readTailnetStatus({ mode: "off", port: 8712, execPath: cli.execPath }), /tailscale_output_limit/);
    } finally { cli.cleanup(); }
  });

  it("kills a CLI that exceeds the command deadline", { timeout: TAILSCALE_COMMAND_TIMEOUT_MS * 2 }, async (t) => {
    const cli = cliFixture(t, { status: status(), delayMs: TAILSCALE_COMMAND_TIMEOUT_MS * 2 });
    try {
      await assert.rejects(readTailnetStatus({ mode: "off", port: 8712, execPath: cli.execPath }), /tailscale_timeout/);
    } finally { cli.cleanup(); }
  });

  it("shares concurrent discovery and caches success until an explicit refresh", async (t) => {
    const cli = cliFixture(t, { status: status(), services: [service(8712, "100.64.0.8")], delayMs: 30 });
    try {
      const cfg = config(cli.execPath, 8712);
      const targets = await Promise.all([resolveConnection(cfg), resolveConnection(cfg), resolveConnection(cfg)]);
      assert.ok(targets.every((target) => target.url === "tcp://100.64.0.8:8712"));
      assert.equal(cli.commandCount(), 2);
      cli.update({ status: status(), services: [service(8712, "100.64.0.9")] });
      assert.equal((await resolveConnection(cfg)).url, "tcp://100.64.0.8:8712");
      assert.equal((await resolveConnection(cfg, { refresh: true })).url, "tcp://100.64.0.9:8712");
      assert.equal(cli.commandCount(), 4);
    } finally { cli.cleanup(); }
  });
});

describe("MeshClient tailnet lifecycle", { concurrency: false }, () => {
  it("authenticates a known Service and exposes welcome/status broker metadata", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status(), services: [service(port)] });
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: config(cli.execPath, port) });
    try {
      const welcome = await client.connect();
      assert.equal(welcome.broker?.meshId, "work");
      assert.equal(welcome.broker?.brokerId, "broker-work");
      assert.equal(client.connectionTarget?.source, "tailscale-service");
      assert.equal(client.connectionError, undefined);
      assert.deepEqual(client.brokerInfo, welcome.broker);
      assert.deepEqual((await client.status()).broker, welcome.broker);
    } finally { await client.close(); await broker.close(); cli.cleanup(); }
  });

  it("hot replacement retains the authenticated selection and existing reply targets", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status(), services: [service(port)] });
    const client = new MeshClient({ alias: "alice", rooms: ["work"], noReconnect: true, config: config(cli.execPath, port) });
    const peer = new MeshClient({
      alias: "bob", rooms: ["work"], noReconnect: true,
      config: { brokerUrl: `tcp://127.0.0.1:${port}`, brokerToken: "secret", meshId: "work" },
    });
    const restored = new MeshClient({
      alias: "alice", noReconnect: true, strictAlias: true,
      config: { brokerUrl: "tcp://127.0.0.1:1", brokerToken: "wrong", meshId: "wrong" },
    });
    const requests: MeshFrame[] = [];
    const answers: MeshFrame[] = [];
    client.on("inbound", (frame: MeshFrame) => requests.push(frame));
    peer.on("inbound", (frame: MeshFrame) => answers.push(frame));
    try {
      await Promise.all([client.connect(), peer.connect()]);
      const sent = await peer.send({ to: "alice", room: "work", message: "reply after code replacement" });
      assert.ok(sent.status === "delivered");
      await waitFor(() => requests.find((frame) => frame.id === sent.msgId));
      await client.close();
      restored.restoreAfterReload(client.snapshotForReload());
      const welcome = await restored.connect();
      assert.equal(welcome.broker?.brokerId, "broker-work");
      assert.equal(restored.connectionTarget?.source, "tailscale-service");
      const result = await restored.reply(sent.msgId, "authenticated old target preserved");
      assert.equal(result.status, "delivered");
      const answer = await waitFor(() => answers.find((frame) => frame.replyTo === sent.msgId));
      assert.equal(answer.body, "authenticated old target preserved");
    } finally {
      await client.close(); await restored.close(); await peer.close();
      await broker.close(); cli.cleanup();
    }
  });

  it("hot replacement refuses a different broker even when discovery advertises the same mesh", async (t) => {
    const first = await tcpBroker();
    let replacement: RunningBroker | undefined;
    const cli = cliFixture(t, { status: status(), services: [service(first.port)] });
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: config(cli.execPath, first.port) });
    const restored = new MeshClient({ alias: "alice", noReconnect: true });
    let ready = 0;
    let inbound = 0;
    restored.on("ready", () => { ready++; });
    restored.on("inbound", () => { inbound++; });
    try {
      await client.connect();
      await client.close();
      restored.restoreAfterReload(client.snapshotForReload());
      await first.broker.close();
      replacement = (await tcpBroker("work", "replacement-broker", first.port)).broker;
      cli.update({ status: status(), services: [service(first.port)] });
      clearTailnetDiscoveryCache();
      await assert.rejects(() => restored.connect(), /broker_mismatch.*expected broker-work/);
      assert.equal(ready, 0);
      assert.equal(inbound, 0);
      await waitFor(() => !replacement!.state.peers.has("alice"));
      assert.equal(replacement.state.peers.has("alice"), false);
    } finally {
      await client.close(); await restored.close();
      await first.broker.close(); await replacement?.close(); cli.cleanup();
    }
  });

  it("wrong mesh and failed authentication never emit ready or enter the local graph", async (t) => {
    const dirs = makeTempDirs("mesh-tailnet-isolation-");
    const local = await startTestBroker(dirs.runtimeDir);
    const { broker, port } = await tcpBroker("other");
    const cli = cliFixture(t, { status: status(), services: [service(port)] });
    const client = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, noReconnect: true, config: config(cli.execPath, port) });
    let ready = 0;
    client.on("ready", () => ready += 1);
    try {
      await assert.rejects(client.connect(), /mesh_mismatch/);
      assert.equal(client.brokerInfo, undefined);
      assert.equal(ready, 0);
      assert.equal(local.state.peers.size, 0);
      assert.equal(broker.state.peers.size, 0);
      await assert.rejects(client.selectTailnet({ ...config(cli.execPath, port, "service", { service: "svc:pi-mesh-work" }), meshId: "other", brokerToken: "wrong" }), /invalid_token/);
      assert.equal(ready, 0);
      assert.match(client.connectionError ?? "", /invalid_token/);
    } finally { await client.close(); await broker.close(); await local.close(); cli.cleanup(); dirs.cleanup(); }
  });

  it("no Service and no daemon never fall back to an existing local broker", async (t) => {
    const dirs = makeTempDirs("mesh-tailnet-no-fallback-");
    const local = await startTestBroker(dirs.runtimeDir);
    const cli = cliFixture(t, { status: status(), services: [] });
    const client = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir, noReconnect: true, config: config(cli.execPath, 8712) });
    try {
      await assert.rejects(client.connect(), /tailscale_service_not_found/);
      cli.update({ exitCode: 1, stderr: "failed to connect to tailscaled" });
      await assert.rejects(client.selectTailnet(config(cli.execPath, 8712)), /tailscale_daemon_unavailable/);
      assert.equal(local.state.peers.size, 0);
      assert.equal(client.brokerInfo, undefined);
    } finally { await client.close(); await local.close(); cli.cleanup(); dirs.cleanup(); }
  });

  it("requires matching metadata on tailnet but preserves explicit legacy compatibility", async (t) => {
    const sockets = new Set<net.Socket>();
    let advertised: BrokerInfo | undefined;
    const server = net.createServer((socket) => {
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
      // Rejecting metadata closes with unread queued data; Darwin reports this as a reset.
      socket.on("error", (error: NodeJS.ErrnoException) => assert.equal(error.code, "ECONNRESET"));
      socket.once("data", () => {
        socket.write(encodeFrame(buildFrame({ type: "welcome", rooms: ["default"], peers: [], broker: advertised }), DEFAULT_CONFIG.maxFrameBytes));
        socket.write(encodeFrame(buildFrame({ type: "mailbox", from: "old-peer", to: "alice", room: "default", body: "must not be injected" }), DEFAULT_CONFIG.maxFrameBytes));
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const addr = server.address();
    assert.ok(addr !== null && typeof addr !== "string");
    const cli = cliFixture(t, { status: status(), services: [service(addr.port)] });
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: config(cli.execPath, addr.port) });
    let inbound = 0;
    let ready = 0;
    client.on("inbound", () => inbound += 1);
    client.on("ready", () => ready += 1);
    try {
      await assert.rejects(client.connect(), /broker_metadata_missing/);
      assert.equal(inbound, 0);
      assert.equal(ready, 0);
      assert.equal(client.brokerInfo, undefined);
      advertised = { meshId: "other", brokerId: "unknown-broker", bootId: "boot-other" };
      await assert.rejects(client.connect(), /mesh_mismatch.*expected work/);
      assert.equal(inbound, 0);
      assert.equal(ready, 0);
      advertised = undefined;
      const generic = new MeshClient({ alias: "generic", noReconnect: true, config: { brokerUrl: `tcp://127.0.0.1:${addr.port}` } });
      try {
        const welcome = await generic.connect();
        assert.equal(welcome.broker, undefined);
        assert.equal(generic.connectionTarget?.source, "explicit");
      } finally { await generic.close(); }
    } finally {
      await client.close();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      cli.cleanup();
    }
  });

  it("recovers automatically when an initially absent Service becomes available", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status(), services: [] });
    const client = new MeshClient({ alias: "alice", config: config(cli.execPath, port) });
    try {
      await assert.rejects(client.connect(), /tailscale_service_not_found/);
      assert.match(client.connectionError ?? "", /tailscale_service_not_found/);
      cli.update({ status: status(), services: [service(port)] });
      await waitFor(() => client.brokerInfo?.brokerId === "broker-work");
      assert.equal(client.connectionError, undefined);
      assert.ok((await client.status()).peers.some((peer) => peer.alias === "alice"));
    } finally { await client.close(); await broker.close(); cli.cleanup(); }
  });

  it("refreshes discovery after a rejected handshake rather than retrying cached endpoint metadata", async (t) => {
    const { broker, port } = await tcpBroker();
    const rejected = net.createServer((socket) => socket.once("data", () => socket.end()));
    await new Promise<void>((resolve) => rejected.listen(port, "::1", resolve));
    const cli = cliFixture(t, { status: status(), services: [service(port, "::1")] });
    const client = new MeshClient({ alias: "alice", config: config(cli.execPath, port) });
    try {
      await assert.rejects(client.connect(), /broker_unavailable/);
      cli.update({ status: status(), services: [service(port)] });
      await waitFor(() => client.brokerInfo?.brokerId === "broker-work");
      assert.equal(client.connectionTarget?.url, `tcp://127.0.0.1:${port}`);
    } finally {
      await client.close(); await broker.close();
      await new Promise<void>((resolve) => rejected.close(() => resolve()));
      cli.cleanup();
    }
  });

  it("detects broker restart, clears stale peer claims/activity, and re-declares own identity", async (t) => {
    const first = await tcpBroker();
    let broker = first.broker;
    const cli = cliFixture(t, { status: status(), services: [service(first.port)] });
    const claim = { pattern: "src/client/**", since: new Date().toISOString() };
    const client = new MeshClient({ alias: "alice", rooms: ["work"], initialReservations: [claim], config: config(cli.execPath, first.port) });
    const peer = new MeshClient({ alias: "bob", rooms: ["work"], noReconnect: true, initialReservations: [{ pattern: "stale/**", since: claim.since }], config: { brokerUrl: `tcp://127.0.0.1:${first.port}`, brokerToken: "secret" } });
    let restart: { previous: BrokerInfo; current: BrokerInfo } | undefined;
    client.on("broker_restart", (event: { previous: BrokerInfo; current: BrokerInfo }) => restart = event);
    try {
      await peer.connect();
      peer.sendActivity("busy");
      await client.connect();
      await waitFor(() => client.activityOf("bob")?.state === "busy");
      assert.equal(client.reservationsOf("bob")[0]?.pattern, "stale/**");
      const oldBoot = client.brokerInfo?.bootId;
      await broker.close();
      broker = (await tcpBroker("work", "broker-work", first.port)).broker;
      await waitFor(() => restart);
      assert.equal(restart?.previous.bootId, oldBoot);
      assert.notEqual(restart?.current.bootId, oldBoot);
      assert.equal(client.knowsPeer("bob"), false);
      assert.deepEqual(client.reservationsOf("bob"), []);
      assert.equal(client.activityOf("bob"), undefined);
      const own = (await client.status()).peers.find((entry) => entry.alias === "alice");
      assert.deepEqual(own?.rooms, ["work"]);
      assert.equal(own?.reservations?.[0]?.pattern, claim.pattern);
    } finally { await client.close(); await peer.close(); await broker.close(); cli.cleanup(); }
  });

  it("pins the first broker identity and rejects a replacement or explicit expected-ID mismatch", async (t) => {
    const first = await tcpBroker();
    let broker = first.broker;
    const cli = cliFixture(t, { status: status(), services: [service(first.port)] });
    const client = new MeshClient({ alias: "alice", config: config(cli.execPath, first.port) });
    let ready = 0;
    client.on("ready", () => ready += 1);
    try {
      await client.connect();
      await broker.close();
      broker = (await tcpBroker("work", "different-broker", first.port)).broker;
      await waitFor(() => client.connectionError?.includes("broker_mismatch"));
      assert.equal(ready, 1);
      assert.equal(client.brokerInfo?.brokerId, "broker-work");
      const expected = new MeshClient({ alias: "charlie", noReconnect: true, config: { ...config(cli.execPath, first.port), brokerId: "expected-broker" } });
      try { await assert.rejects(expected.connect(), /broker_mismatch.*expected-broker/); }
      finally { await expected.close(); }
    } finally { await client.close(); await broker.close(); cli.cleanup(); }
  });

  it("switches graphs preserving alias/rooms/own claims but forgetting old message targets", async (t) => {
    const first = await tcpBroker();
    const second = await tcpBroker("next", "broker-next");
    const cli = cliFixture(t, { status: status(), services: [service(first.port), service(second.port, "127.0.0.1", "svc:pi-mesh-next")] });
    const client = new MeshClient({ alias: "alice", rooms: ["work"], noReconnect: true, config: config(cli.execPath, first.port) });
    const peer = new MeshClient({ alias: "bob", rooms: ["work"], noReconnect: true, config: { brokerUrl: `tcp://127.0.0.1:${first.port}`, brokerToken: "secret" } });
    const frames: MeshFrame[] = [];
    client.on("inbound", (frame: MeshFrame) => frames.push(frame));
    try {
      await client.connect();
      await client.reserve(["owned/**"]);
      await peer.connect();
      const sent = await peer.send({ to: "alice", room: "work", message: "old graph payload" });
      await waitFor(() => frames[0]);
      assert.ok(sent.msgId);
      const welcome = await client.selectTailnet({ ...config(cli.execPath, second.port), meshId: "next" });
      assert.equal(welcome.broker?.brokerId, "broker-next");
      assert.equal(client.alias, "alice");
      assert.deepEqual(client.rooms, ["work"]);
      assert.equal(client.reservations[0]?.pattern, "owned/**");
      assert.equal(client.knowsPeer("bob"), false);
      assert.ok(!client.transcript.some((frame) => frame.body === "old graph payload"));
      assert.deepEqual(await client.reply(sent.msgId, "must not cross graphs"), { status: "error", msgId: sent.msgId, reason: "reply_without_target" });
      const own = (await client.status()).peers.find((entry) => entry.alias === "alice");
      assert.equal(own?.reservations?.[0]?.pattern, "owned/**");
      assert.equal(first.broker.state.peers.has("alice"), false);
    } finally { await client.close(); await peer.close(); await first.broker.close(); await second.broker.close(); cli.cleanup(); }
  });

  it("joins the selected graph when connect and send immediately follow selection", { timeout: 3_000 }, async () => {
    const first = await tcpBroker("graph-a", "graph-a");
    const second = await tcpBroker("graph-b", "graph-b");
    const firstConfig: MeshConfig = { ...DEFAULT_CONFIG, meshId: "graph-a", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${first.port}` };
    const secondConfig: MeshConfig = { ...DEFAULT_CONFIG, meshId: "graph-b", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${second.port}` };
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: firstConfig });
    const oldPeer = new MeshClient({ alias: "bob", noReconnect: true, config: firstConfig });
    const newPeer = new MeshClient({ alias: "bob", noReconnect: true, config: secondConfig });
    const oldMessages: MeshFrame[] = [];
    const newMessages: MeshFrame[] = [];
    oldPeer.on("inbound", (frame: MeshFrame) => oldMessages.push(frame));
    newPeer.on("inbound", (frame: MeshFrame) => newMessages.push(frame));
    try {
      await Promise.all([client.connect(), oldPeer.connect(), newPeer.connect()]);
      const selecting = client.selectTailnet(secondConfig);
      const connecting = client.connect();
      const sending = client.send({ to: "bob", message: "selected graph only" });
      const [selected, connected, sent] = await Promise.all([selecting, connecting, sending]);
      assert.equal(selected.broker?.brokerId, "graph-b");
      assert.equal(connected.broker?.brokerId, "graph-b");
      assert.equal(client.brokerInfo?.brokerId, "graph-b");
      assert.equal(sent.status, "delivered");
      await waitFor(() => newMessages.some((frame) => frame.body === "selected graph only"));
      assert.deepEqual(oldMessages, []);
      assert.equal(first.broker.state.peers.has("alice"), false);
    } finally {
      await Promise.all([client.close(), oldPeer.close(), newPeer.close()]);
      await Promise.all([first.broker.close(), second.broker.close()]);
    }
  });

  it("cancels a superseded selection and its connect caller before joining the newest graph", { timeout: 3_000 }, async () => {
    const first = await tcpBroker("graph-a", "graph-a");
    const second = await tcpBroker("graph-b", "graph-b");
    const third = await tcpBroker("graph-c", "graph-c");
    const firstConfig: MeshConfig = { ...DEFAULT_CONFIG, meshId: "graph-a", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${first.port}` };
    const secondConfig: MeshConfig = { ...DEFAULT_CONFIG, meshId: "graph-b", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${second.port}` };
    const thirdConfig: MeshConfig = { ...DEFAULT_CONFIG, meshId: "graph-c", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${third.port}` };
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: firstConfig });
    try {
      await client.connect();
      const previous = client.selectTailnet(secondConfig);
      const previousConnect = client.connect();
      const cancelled = Promise.all([
        assert.rejects(previous, /connection_cancelled/),
        assert.rejects(previousConnect, /connection_cancelled/),
      ]);
      const latest = client.selectTailnet(thirdConfig);
      const latestConnect = client.connect();
      const [selected, connected] = await Promise.all([latest, latestConnect]);
      await cancelled;
      assert.equal(selected.broker?.brokerId, "graph-c");
      assert.equal(connected.broker?.brokerId, "graph-c");
      assert.equal(client.brokerInfo?.brokerId, "graph-c");
      assert.equal(first.broker.state.peers.has("alice"), false);
      assert.equal(second.broker.state.peers.has("alice"), false);
    } finally {
      await client.close();
      await Promise.all([first.broker.close(), second.broker.close(), third.broker.close()]);
    }
  });

  it("cancels every old graph wait with its intact summary and delivers new LAUNCH answers", { timeout: 3_000 }, async () => {
    const first = await tcpBroker("graph-a", "graph-a");
    const second = await tcpBroker("graph-b", "graph-b");
    const firstConfig: MeshConfig = { ...DEFAULT_CONFIG, meshId: "graph-a", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${first.port}` };
    const secondConfig: MeshConfig = { ...DEFAULT_CONFIG, meshId: "graph-b", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${second.port}` };
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: firstConfig });
    const oldPeer = new MeshClient({ alias: "bob", noReconnect: true, config: firstConfig });
    const newPeer = new MeshClient({ alias: "bob", noReconnect: true, config: secondConfig });
    const inbound: MeshFrame[] = [];
    client.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    oldPeer.on("inbound", (frame: MeshFrame) => {
      if (frame.type === "msg" && frame.body === "answered old mission") void oldPeer.reply(frame.id, "old answer");
    });
    newPeer.on("inbound", (frame: MeshFrame) => {
      if (frame.type === "msg") void newPeer.reply(frame.id, "new graph answer");
    });
    try {
      await Promise.all([client.connect(), oldPeer.connect(), newPeer.connect()]);
      const answered = await client.send({ to: "bob", message: "answered old mission", awaitReply: true, block: false, timeoutMs: 30_000 });
      await waitFor(() => client.missionStatus().some((mission) => mission.msgId === answered.msgId && mission.answered));
      const missing = await client.send({ to: "bob", message: "missing old mission", awaitReply: true, block: false, timeoutMs: 30_000 });
      assert.ok(answered.msgId);
      assert.ok(missing.msgId);
      const waiting = [client.waitAll(30_000), client.waitAll(30_000)];
      await client.selectTailnet(secondConfig);
      for (const summary of await Promise.all(waiting)) {
        assert.equal(summary.status, "cancelled");
        assert.equal(summary.total, 2);
        assert.equal(summary.answered, 1);
        assert.deepEqual(summary.answers, [{ msgId: answered.msgId, to: "bob", response: "old answer" }]);
        assert.deepEqual(summary.missing, [{ msgId: missing.msgId, to: "bob" }]);
      }
      assert.deepEqual(client.missionStatus(), []);
      const launched = await client.send({ to: "bob", message: "new graph mission", awaitReply: true, block: false, timeoutMs: 30_000 });
      assert.equal(launched.status, "delivered");
      await waitFor(() => inbound.some((frame) => frame.replyTo === launched.msgId && frame.body === "new graph answer"));
      assert.equal(inbound.filter((frame) => frame.body === "new graph answer").length, 1);
    } finally {
      await Promise.all([client.close(), oldPeer.close(), newPeer.close()]);
      await Promise.all([first.broker.close(), second.broker.close()]);
    }
  });

  it("keeps simultaneous waits independent under ESC and resumes answer wake after completion", { timeout: 3_000 }, async () => {
    const { broker, port } = await tcpBroker();
    const cfg: MeshConfig = { ...DEFAULT_CONFIG, meshId: "work", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${port}` };
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: cfg });
    const peer = new MeshClient({ alias: "bob", noReconnect: true, config: cfg });
    const received: MeshFrame[] = [];
    const inbound: MeshFrame[] = [];
    peer.on("inbound", (frame: MeshFrame) => received.push(frame));
    client.on("inbound", (frame: MeshFrame) => inbound.push(frame));
    const abort = new AbortController();
    try {
      await Promise.all([client.connect(), peer.connect()]);
      const first = await client.send({ to: "bob", message: "first mission", awaitReply: true, block: false, timeoutMs: 30_000 });
      const interrupted = client.waitAll(30_000, abort.signal);
      const remaining = client.waitAll(30_000);
      abort.abort();
      const cancelled = await interrupted;
      assert.equal(cancelled.status, "cancelled");
      assert.deepEqual(cancelled.missing, [{ msgId: first.msgId, to: "bob" }]);
      await waitFor(() => received.some((frame) => frame.id === first.msgId));
      assert.equal((await peer.reply(first.msgId!, "first answer")).status, "delivered");
      const summary = await remaining;
      assert.equal(summary.status, "complete");
      assert.deepEqual(summary.answers, [{ msgId: first.msgId, to: "bob", response: "first answer" }]);
      assert.equal(inbound.length, 0);
      const second = await client.send({ to: "bob", message: "second mission", awaitReply: true, block: false, timeoutMs: 30_000 });
      await waitFor(() => received.some((frame) => frame.id === second.msgId));
      await peer.reply(second.msgId!, "second answer");
      await waitFor(() => inbound.some((frame) => frame.replyTo === second.msgId && frame.body === "second answer"));
    } finally { abort.abort(); await Promise.all([client.close(), peer.close()]); await broker.close(); }
  });

  it("close cancels a pending wait immediately with its old missing mission", { timeout: 3_000 }, async () => {
    const { broker, port } = await tcpBroker();
    const cfg: MeshConfig = { ...DEFAULT_CONFIG, meshId: "work", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${port}` };
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: cfg });
    const peer = new MeshClient({ alias: "bob", noReconnect: true, config: cfg });
    try {
      await Promise.all([client.connect(), peer.connect()]);
      const sent = await client.send({ to: "bob", message: "close my wait", awaitReply: true, block: false, timeoutMs: 30_000 });
      const waiting = client.waitAll(30_000);
      await client.close();
      const summary = await waiting;
      assert.equal(summary.status, "cancelled");
      assert.equal(summary.total, 1);
      assert.equal(summary.answered, 0);
      assert.deepEqual(summary.missing, [{ msgId: sent.msgId, to: "bob" }]);
    } finally { await Promise.all([client.close(), peer.close()]); await broker.close(); }
  });

  it("explicit close cancels discovery selection and the connect/send callers joined to it", { timeout: 3_000 }, async (t) => {
    const first = await tcpBroker("graph-a", "graph-a");
    const second = await tcpBroker("graph-b", "graph-b");
    const cli = cliFixture(t, { status: status(), services: [] });
    const marker = path.join(cli.root, "selection-started");
    cli.update({ status: status(), services: [service(second.port, "127.0.0.1", "svc:pi-mesh-graph-b")], delayMs: 150, startedFile: marker });
    const client = new MeshClient({
      alias: "alice",
      noReconnect: true,
      config: { ...DEFAULT_CONFIG, meshId: "graph-a", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${first.port}` },
    });
    const ready: string[] = [];
    client.on("ready", () => ready.push(client.brokerInfo?.brokerId ?? ""));
    try {
      await client.connect();
      const selecting = client.selectTailnet({ ...config(cli.execPath, second.port), meshId: "graph-b" });
      const connecting = client.connect();
      const sending = client.send({ to: "bob", message: "must not escape cancelled selection" });
      const cancelled = Promise.all([
        assert.rejects(selecting, /connection_cancelled/),
        assert.rejects(connecting, /connection_cancelled/),
      ]);
      await waitFor(() => fs.existsSync(marker));
      await client.close();
      await cancelled;
      const result = await sending;
      assert.equal(result.status, "error");
      if (result.status === "error") {
        assert.equal(result.reason, "closed");
        assert.equal(result.tracked, false);
      }
      await sleep(350);
      assert.deepEqual(ready, ["graph-a"]);
      assert.equal(second.broker.state.peers.size, 0);
    } finally { await client.close(); await Promise.all([first.broker.close(), second.broker.close()]); cli.cleanup(); }
  });

  it("supersedes an already-started discovery selection without joining its old endpoint", { timeout: 3_000 }, async (t) => {
    const first = await tcpBroker("graph-a", "graph-a");
    const second = await tcpBroker("graph-b", "graph-b");
    const third = await tcpBroker("graph-c", "graph-c");
    const cli = cliFixture(t, { status: status(), services: [] });
    const marker = path.join(cli.root, "previous-selection-started");
    cli.update({ status: status(), services: [service(second.port, "127.0.0.1", "svc:pi-mesh-graph-b")], delayMs: 150, startedFile: marker });
    const client = new MeshClient({
      alias: "alice",
      noReconnect: true,
      config: { ...DEFAULT_CONFIG, meshId: "graph-a", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${first.port}` },
    });
    const ready: string[] = [];
    client.on("ready", () => ready.push(client.brokerInfo?.brokerId ?? ""));
    try {
      await client.connect();
      const previous = client.selectTailnet({ ...config(cli.execPath, second.port), meshId: "graph-b" });
      const previousConnect = client.connect();
      const cancelled = Promise.all([
        assert.rejects(previous, /connection_cancelled/),
        assert.rejects(previousConnect, /connection_cancelled/),
      ]);
      await waitFor(() => fs.existsSync(marker));
      const latest = client.selectTailnet({ ...DEFAULT_CONFIG, meshId: "graph-c", brokerToken: "secret", brokerUrl: `tcp://127.0.0.1:${third.port}` });
      const latestConnect = client.connect();
      const [selected, connected] = await Promise.all([latest, latestConnect]);
      await cancelled;
      assert.equal(selected.broker?.brokerId, "graph-c");
      assert.equal(connected.broker?.brokerId, "graph-c");
      await sleep(350);
      assert.deepEqual(ready, ["graph-a", "graph-c"]);
      assert.equal(second.broker.state.peers.size, 0);
    } finally {
      await client.close();
      await Promise.all([first.broker.close(), second.broker.close(), third.broker.close()]);
      cli.cleanup();
    }
  });

  it("closing during discovery rejects the connect and cannot send hello or resurrect readiness", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status(), services: [service(port)], delayMs: 150 });
    const marker = path.join(cli.root, "started");
    cli.update({ status: status(), services: [service(port)], delayMs: 150, startedFile: marker });
    const client = new MeshClient({ alias: "alice", config: config(cli.execPath, port) });
    let ready = 0;
    client.on("ready", () => ready += 1);
    try {
      const connecting = client.connect();
      const cancelled = assert.rejects(connecting, /connection_cancelled/);
      await waitFor(() => fs.existsSync(marker));
      await client.close();
      await cancelled;
      await sleep(500);
      assert.equal(ready, 0);
      assert.equal(client.brokerInfo, undefined);
      assert.equal(client.connectionTarget, null);
      assert.equal(broker.state.peers.size, 0);
    } finally { await client.close(); await broker.close(); cli.cleanup(); }
  });

  it("shutdown interrupts an in-flight graph selection instead of re-arming connect", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status(), services: [service(port)] });
    const client = new MeshClient({ alias: "alice", config: config(cli.execPath, port) });
    let ready = 0;
    client.on("ready", () => ready += 1);
    try {
      const selecting = client.selectTailnet(config(cli.execPath, port));
      const cancelled = assert.rejects(selecting, /connection_cancelled/);
      await client.close();
      await cancelled;
      assert.equal(ready, 0);
      assert.equal(broker.state.peers.size, 0);
      assert.equal(cli.commandCount(), 0);
    } finally { await client.close(); await broker.close(); cli.cleanup(); }
  });

  it("noReconnect stays offline after an initial discovery error", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status(), services: [] });
    const client = new MeshClient({ alias: "alice", noReconnect: true, config: config(cli.execPath, port) });
    try {
      await assert.rejects(client.connect(), /tailscale_service_not_found/);
      cli.update({ status: status(), services: [service(port)] });
      await sleep(500);
      assert.equal(client.brokerInfo, undefined);
      assert.equal(broker.state.peers.size, 0);
      assert.equal(cli.commandCount(), 2);
    } finally { await client.close(); await broker.close(); cli.cleanup(); }
  });
});

describe("Tailnet selection persistence", { concurrency: false }, () => {
  it("new sessions join the authenticated selection without losing unrelated settings or exposing tokens", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status({ broker: statusNode("broker", "broker", ["127.0.0.1"]) }) });
    const dirs = makeTempDirs("mesh-tailnet-selection-");
    const target = path.join(dirs.stateDir, "config.json");
    fs.writeFileSync(target, JSON.stringify({ alias: "persisted", brokerToken: "secret", rooms: ["team"], custom: { retained: true } }));
    const cfg = { ...loadConfig(dirs.stateDir, {}), tailscale: { mode: "off" as const, port, execPath: cli.execPath } };
    let client: MeshClient | undefined;
    try {
      const result = await executeTailnetCommand({ action: "join", selection: { meshId: "work", host: "broker", port } }, cfg, dirs.stateDir, dirs.runtimeDir, undefined, {});
      assert.equal(result.ok, true, result.text);
      const saved = loadConfig(dirs.stateDir, {});
      client = new MeshClient({ alias: saved.alias, rooms: saved.rooms, noReconnect: true, config: saved });
      const welcome = await client.connect();
      assert.equal(welcome.broker?.meshId, "work");
      assert.equal(client.connectionTarget?.source, "tailscale-node");
      assert.equal(welcome.alias, "persisted");
      assert.deepEqual(welcome.rooms, ["team"]);
      assert.deepEqual(JSON.parse(fs.readFileSync(target, "utf8")).custom, { retained: true });
      assert.equal(JSON.stringify(result.details).includes("secret"), false);
      assert.equal(JSON.stringify(result.details).includes("brokerToken"), false);
    } finally { await client?.close(); await broker.close(); cli.cleanup(); dirs.cleanup(); }
  });

  it("failed authentication, wrong graph and explicit overrides never persist an unverified selection", async (t) => {
    const { broker, port } = await tcpBroker();
    const cli = cliFixture(t, { status: status({ broker: statusNode("broker", "broker", ["127.0.0.1"]) }) });
    const dirs = makeTempDirs("mesh-tailnet-selection-refusal-");
    const target = path.join(dirs.stateDir, "config.json");
    const original = JSON.stringify({ rooms: ["private"], meshId: "old-graph" });
    fs.writeFileSync(target, original);
    const cfg = { ...DEFAULT_CONFIG, brokerToken: "secret", tailscale: { mode: "off" as const, port, execPath: cli.execPath } };
    const command = { action: "join" as const, selection: { meshId: "work", host: "broker", port } };
    try {
      const wrongToken = await executeTailnetCommand(command, { ...cfg, brokerToken: "wrong" }, dirs.stateDir, dirs.runtimeDir, undefined, {});
      assert.equal(wrongToken.ok, false);
      assert.match(wrongToken.text, /invalid_token/);
      const wrongMesh = await executeTailnetCommand({ ...command, selection: { ...command.selection, meshId: "wrong-graph" } }, cfg, dirs.stateDir, dirs.runtimeDir, undefined, {});
      assert.equal(wrongMesh.ok, false);
      assert.match(wrongMesh.text, /mesh_mismatch/);
      const explicit = await executeTailnetCommand(command, { ...cfg, brokerUrl: `tcp://127.0.0.1:${port}` }, dirs.stateDir, dirs.runtimeDir, undefined, {});
      assert.equal(explicit.ok, false);
      assert.match(explicit.text, /tailscale_selection_overridden/);
      const envOverride = await executeTailnetCommand(command, cfg, dirs.stateDir, dirs.runtimeDir, undefined, { MESH_ID: "old-graph" });
      assert.equal(envOverride.ok, false);
      assert.match(envOverride.text, /MESH_ID overrides/);
      assert.equal(fs.readFileSync(target, "utf8"), original);
    } finally { await broker.close(); cli.cleanup(); dirs.cleanup(); }
  });

  it("a session invalidated during authentication cannot overwrite a newer saved selection", async (t) => {
    const { broker, port } = await tcpBroker();
    const dirs = makeTempDirs("mesh-tailnet-stale-selection-");
    const started = path.join(dirs.root, "cli-started");
    const cli = cliFixture(t, { status: status({ broker: statusNode("broker", "broker", ["127.0.0.1"]) }), startedFile: started, delayMs: 100 });
    const target = path.join(dirs.stateDir, "config.json");
    const replacement = JSON.stringify({ meshId: "replacement", rooms: ["replacement-room"] });
    const cfg = { ...DEFAULT_CONFIG, brokerToken: "secret", tailscale: { mode: "off" as const, port, execPath: cli.execPath } };
    let current = true;
    try {
      const selecting = executeTailnetCommand(
        { action: "join", selection: { meshId: "work", host: "broker", port } },
        cfg, dirs.stateDir, dirs.runtimeDir, undefined, {}, () => current,
      );
      await waitFor(() => fs.existsSync(started));
      current = false;
      fs.writeFileSync(target, replacement);
      const result = await selecting;
      assert.equal(result.ok, false);
      assert.match(result.text, /selection_cancelled/);
      assert.equal(fs.readFileSync(target, "utf8"), replacement);
      assert.equal(broker.state.peers.size, 0);
    } finally { await broker.close(); cli.cleanup(); dirs.cleanup(); }
  });
});
