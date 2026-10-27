// test/fileserve.test.ts — mesh_file_serve end-to-end over a real broker:
// offer announced as an ordinary message, one-shot token download, sha256
// verification, TTL expiry, size cap, name sanitization, tailnet-first
// candidate ordering, and supersede/close semantics.
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { get } from "node:http";
import { MeshClient } from "../src/client/client.js";
import {
  FILE_SERVE_MAX_BYTES,
  FILE_SERVE_MAX_TTL_MS,
  FILE_SERVE_MIN_TTL_MS,
} from "../src/shared/config.js";
import {
  sanitizeServeName,
  serveFileOnce,
  FileServeError,
  type FileServeHandle,
} from "../src/fileserve/serve.js";
import { serveHostCandidates } from "../src/extension/tools.js";
import {
  makeTempDirs,
  startTestBroker,
  waitFor,
  type TempDirs,
} from "./helpers.js";

interface HttpGetResult {
  status: number;
  body: Buffer;
  headers: Record<string, string | string[] | undefined>;
}

function httpGet(url: string): Promise<HttpGetResult> {
  return new Promise((resolve, reject) => {
    const req = get(url, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (c) => chunks.push(c as Buffer));
      res.on("end", () => resolve({
        status: res.statusCode ?? 0,
        body: Buffer.concat(chunks),
        headers: res.headers,
      }));
    });
    req.on("error", reject);
  });
}

function makeFile(dir: string, name: string, bytes: number): string {
  const path = join(dir, name);
  writeFileSync(path, Buffer.alloc(bytes, 0x61));
  return path;
}

describe("serveFileOnce (unit, real listener)", () => {
  const dir = mkdtempSync(join(tmpdir(), "mesh-serve-unit-"));

  after(() => rmSync(dir, { recursive: true, force: true }));

  it("sanitizes names: basename only, control chars stripped, refuses empty", () => {
    assert.equal(sanitizeServeName("/etc/passwd"), "passwd");
    assert.equal(sanitizeServeName("a/b/../c\n\t.txt"), "c.txt");
    assert.throws(() => sanitizeServeName("   "), FileServeError);
  });

  it("serves the exact bytes once with sha256 header, then 409s", async () => {
    const path = makeFile(dir, "artifact.bin", 4096);
    const handle = await serveFileOnce({ path, hostCandidates: ["127.0.0.1"] });
    try {
      const res = await httpGet(handle.fields.url);
      assert.equal(res.status, 200);
      assert.equal(res.body.length, 4096);
      assert.equal(res.headers["x-mesh-sha256"], handle.fields.sha256);
      const again = await httpGet(handle.fields.url);
      assert.equal(again.status, 409);
      assert.equal(await handle.onceServed, "downloaded");
    } finally {
      handle.close();
    }
  });

  it("wrong path → 404 without burning the offer", async () => {
    const path = makeFile(dir, "guarded.txt", 16);
    const handle = await serveFileOnce({ path, hostCandidates: ["127.0.0.1"] });
    try {
      const wrong = await httpGet(handle.fields.url.replace(/\/mesh-file\/.*/, "/mesh-file/deadbeef"));
      assert.equal(wrong.status, 404);
      const right = await httpGet(handle.fields.url);
      assert.equal(right.status, 200);
    } finally {
      handle.close();
    }
  });

  it("TTL expiry closes the listener (expired, not downloaded)", async () => {
    const path = makeFile(dir, "slow.txt", 16);
    const handle = await serveFileOnce({ path, hostCandidates: ["127.0.0.1"], ttlMs: FILE_SERVE_MIN_TTL_MS });
    const outcome = await handle.onceServed;
    assert.equal(outcome, "expired");
    await assert.rejects(() => httpGet(handle.fields.url));
  });

  it("close() burns the offer: expired settle, further downloads refused", async () => {
    const path = makeFile(dir, "abort.bin", 1024);
    const handle = await serveFileOnce({ path, hostCandidates: ["127.0.0.1"], ttlMs: FILE_SERVE_MAX_TTL_MS });
    handle.close();
    assert.equal(await handle.onceServed, "expired");
    await assert.rejects(() => httpGet(handle.fields.url));
  });

  it("second concurrent request gets an honest 409 during the grace window", async () => {
    const path = makeFile(dir, "raced.bin", 2048);
    const handle = await serveFileOnce({ path, hostCandidates: ["127.0.0.1"] });
    try {
      await httpGet(handle.fields.url);
      const second = await httpGet(handle.fields.url);
      assert.equal(second.status, 409);
    } finally {
      handle.close();
    }
  });

  it("rejects oversize files and unreadable paths before binding", async () => {
    await assert.rejects(
      serveFileOnce({ path: join(dir, "missing.bin"), hostCandidates: ["127.0.0.1"] }),
      (e: unknown) => e instanceof FileServeError && e.code === "file_unreadable",
    );
    const big = makeFile(dir, "big.bin", FILE_SERVE_MAX_BYTES + 1);
    await assert.rejects(
      serveFileOnce({ path: big, hostCandidates: ["127.0.0.1"] }),
      (e: unknown) => e instanceof FileServeError && e.code === "too_large",
    );
  });

  it("non-IP candidates are refused honestly", async () => {
    const path = makeFile(dir, "x.txt", 8);
    await assert.rejects(
      serveFileOnce({ path, hostCandidates: ["mybox.tailnet.ts.net."] }),
      (e: unknown) => e instanceof FileServeError && e.code === "no_bindable_host",
    );
  });

  it("announcement carries name, size, sha256, url, expiry", async () => {
    const path = makeFile(dir, "report.log", 2048);
    const handle = await serveFileOnce({ path, hostCandidates: ["127.0.0.1"] });
    try {
      assert.match(handle.announcement, /\[file-offer\] report\.log \(2048 B\)/);
      assert.match(handle.announcement, new RegExp(`sha256=${handle.fields.sha256}`));
      assert.match(handle.announcement, new RegExp(`url=${handle.fields.url.replace(/\//g, "\\/")}`));
      assert.equal(handle.fields.size, statSync(path).size);
    } finally {
      handle.close();
    }
  });
});

describe("serveHostCandidates (tailnet ordering)", () => {
  it("tailnet self addresses lead, LAN follows, loopback/internal/link-local excluded", () => {
    const candidates = serveHostCandidates({
      self: { addresses: ["100.64.15.9", "fd7a:115c:a1e0::1234"] },
    }, "IPv4");
    assert.equal(candidates[0], "100.64.15.9");
    const lan = candidates.filter((c) => c.startsWith("192.168.") || c.startsWith("10.") || c.startsWith("172."));
    assert.ok(lan.length >= 0);
    assert.ok(!candidates.includes("127.0.0.1"));
    assert.ok(!candidates.some((c) => c.startsWith("169.254.")));
  });

  it("no tailnet → LAN candidates only", () => {
    const candidates = serveHostCandidates(undefined, "IPv4");
    assert.ok(!candidates.includes("127.0.0.1"));
    assert.ok(candidates.every((c) => c.includes(".")));
  });
});

describe("mesh_file_serve over the broker (e2e)", () => {
  let dirs: TempDirs;
  let broker: Awaited<ReturnType<typeof startTestBroker>>;
  let alice: MeshClient;
  let bob: MeshClient;
  const bodies: string[] = [];
  let servedFile: string | undefined;
  let servedHandle: FileServeHandle | undefined;

  before(async () => {
    dirs = makeTempDirs("mesh-fileserve-");
    broker = await startTestBroker(dirs.runtimeDir);
    alice = new MeshClient({ alias: "alice", runtimeDir: dirs.runtimeDir });
    bob = new MeshClient({ alias: "bob", runtimeDir: dirs.runtimeDir });
    await alice.connect();
    await bob.connect();
    bob.on("inbound", (frame) => {
      if (frame.body !== undefined) bodies.push(frame.body);
    });
  });

  after(async () => {
    servedHandle?.close();
    await alice.close().catch(() => {});
    await bob.close().catch(() => {});
    await broker.close();
    rmSync(dirs.root, { recursive: true, force: true });
  });

  it("offer announced through the broker arrives with url+sha256; download matches", async () => {
    servedFile = makeFile(dirs.root, "build-output.log", 8192);
    const handle = await serveFileOnce({ path: servedFile, hostCandidates: ["127.0.0.1"] });
    servedHandle = handle;
    const res = await alice.send({ to: "bob", message: handle.announcement });
    assert.equal(res.status, "delivered");
    const body = await waitFor(() => bodies.find((b) => b.includes("[file-offer]")));
    assert.match(body, new RegExp(`sha256=${handle.fields.sha256}`));
    const dl = await httpGet(handle.fields.url);
    assert.equal(dl.status, 200);
    assert.equal(dl.body.length, 8192);
    assert.equal(dl.headers["x-mesh-sha256"], handle.fields.sha256);
    assert.equal(await handle.onceServed, "downloaded");
  });

  it("offer from a CLOSED listener fails honestly at download time", async () => {
    const path = makeFile(dirs.root, "gone.txt", 32);
    const handle = await serveFileOnce({ path, hostCandidates: ["127.0.0.1"], ttlMs: FILE_SERVE_MIN_TTL_MS });
    const res = await alice.send({ to: "bob", message: handle.announcement });
    assert.equal(res.status, "delivered");
    await handle.onceServed; // TTL expiry
    await assert.rejects(() => httpGet(handle.fields.url));
  });
});
