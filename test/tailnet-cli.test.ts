import assert from "node:assert/strict";
import net from "node:net";
import { once } from "node:events";
import { test } from "node:test";
import { createBroker } from "../src/broker/broker.js";
import { DEFAULT_POLICY } from "../src/broker/policy.js";
import { DEFAULT_CONFIG } from "../src/shared/config.js";
import { makeTempDirs, spawnCli, startTestBroker } from "./helpers.js";

function cliEnvironment(runtimeDirectory: string, stateDirectory: string, overrides: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env = { ...process.env };
  for (const key of Object.keys(env)) if (key.startsWith("MESH_")) delete env[key];
  return { ...env, MESH_RUNTIME_DIR: runtimeDirectory, MESH_STATE_DIR: stateDirectory, ...overrides };
}

test("doctor refuses a reachable broker with wrong credentials and succeeds only after authentication", async () => {
  const dirs = makeTempDirs("mesh-doctor-auth-");
  const broker = await createBroker({ config: { ...DEFAULT_CONFIG, meshId: "work", brokerToken: "private-diagnostic-token" }, policy: DEFAULT_POLICY, tcpListen: { host: "127.0.0.1", port: 0, tls: false } });
  const address = broker.server.address();
  assert.ok(address !== null && typeof address !== "string");
  const env = cliEnvironment(dirs.runtimeDir, dirs.stateDir, { MESH_ID: "work", MESH_BROKER_URL: `tcp://127.0.0.1:${address.port}`, MESH_BROKER_TOKEN: "wrong" });
  const refused = spawnCli(["doctor"], env);
  try {
    assert.equal(await refused.done, 1, refused.err);
    assert.match(refused.out, /reachable=true/);
    assert.match(refused.out, /authenticated: false/);
    assert.match(refused.out, /invalid_token/);
    const accepted = spawnCli(["doctor"], { ...env, MESH_BROKER_TOKEN: "private-diagnostic-token" });
    try {
      assert.equal(await accepted.done, 0, accepted.err);
      assert.match(accepted.out, /authenticated: true/);
      assert.equal(accepted.out.includes("private-diagnostic-token"), false);
    } finally { await accepted.cleanup(); }
  } finally { await refused.cleanup(); await broker.close(); dirs.cleanup(); }
});

test("tailnet doctor JSON distinguishes a non-mesh TCP listener from an authenticated broker", async () => {
  const dirs = makeTempDirs("mesh-doctor-protocol-");
  const server = net.createServer((socket) => {
    socket.once("data", () => socket.end("HTTP/1.1 200 OK\r\n\r\n"));
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  assert.ok(address !== null && typeof address !== "string");
  const cli = spawnCli(["tailscale", "doctor", "--json"], cliEnvironment(dirs.runtimeDir, dirs.stateDir, { MESH_BROKER_URL: `tcp://127.0.0.1:${address.port}` }));
  try {
    assert.equal(await cli.done, 1, cli.err);
    const output = JSON.parse(cli.out);
    assert.equal(output.ok, false);
    assert.equal(output.diagnostic.resolved, true);
    assert.equal(output.diagnostic.reachable, true);
    assert.equal(output.diagnostic.authenticated, false);
    assert.equal(output.diagnostic.target.source, "explicit");
    assert.equal(output.diagnostic.broker, undefined);
  } finally {
    await cli.cleanup();
    server.close();
    await once(server, "close");
    dirs.cleanup();
  }
});

test("invalid explicit broker URLs cannot silently join an available local graph", async () => {
  const dirs = makeTempDirs("mesh-doctor-no-fallback-");
  const local = await startTestBroker(dirs.runtimeDir);
  const cli = spawnCli(["doctor"], cliEnvironment(dirs.runtimeDir, dirs.stateDir, { MESH_BROKER_URL: "https://not-a-mesh.example:8712" }));
  try {
    assert.equal(await cli.done, 1);
    assert.match(cli.err, /broker_endpoint_invalid/);
    assert.equal(local.state.peers.size, 0);
  } finally { await cli.cleanup(); await local.close(); dirs.cleanup(); }
});

test("JSON diagnostics preserve a parseable failure envelope for invalid configuration", async () => {
  const dirs = makeTempDirs("mesh-doctor-json-config-");
  try {
    for (const [overrides, errorCode] of [
      [{ MESH_BROKER_URL: "invalid" }, "broker_endpoint_invalid"],
      [{ MESH_TAILSCALE_MODE: "service" }, "tailscale_config_invalid"],
    ] as const) {
      const cli = spawnCli(["tailscale", "doctor", "--json"], cliEnvironment(dirs.runtimeDir, dirs.stateDir, overrides));
      try {
        assert.equal(await cli.done, 1);
        const result = JSON.parse(cli.out);
        assert.equal(result.ok, false);
        assert.match(result.error, new RegExp(errorCode));
        assert.equal(result.schema, "mesh.tailnet.v1");
      } finally { await cli.cleanup(); }
    }
  } finally { dirs.cleanup(); }
});
