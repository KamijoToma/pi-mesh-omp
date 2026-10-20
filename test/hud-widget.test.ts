// Exercise the real TUI container and pending lifecycle, not widget payload shape.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Container, visibleWidth } from "@mariozechner/pi-tui";
import * as Tui from "@mariozechner/pi-tui";
import { configureHostTui } from "../src/extension/tui.js";
import { MeshClient } from "../src/client/client.js";
import { attachClientListeners } from "../src/extension/attach.js";
import { MeshHud } from "../src/extension/hud.js";
import type { SessionContext } from "../src/extension/pi-types.js";
import type { MeshRuntime } from "../src/extension/tools.js";
import { makeTempDirs, startTestBroker, waitFor } from "./helpers.js";

configureHostTui(Tui);

function surface(rt: MeshRuntime) {
  const state: { widget?: Parameters<SessionContext["ui"]["setWidget"]>[1] } = {};
  const ctx: SessionContext = {
    cwd: "/tmp", mode: "tui",
    ui: {
      notify: () => {},
      setWidget: (_id, content) => { state.widget = content; },
      setStatus: () => {},
      theme: { fg: (_color, text) => `\u001b[32m${text}\u001b[0m` },
    },
  };
  const hud = new MeshHud({ getRuntime: () => rt });
  hud.attach(ctx);
  const render = (width: number): string[] => {
    const container = new Container();
    if (typeof state.widget !== "function") throw new Error("Expected an interactive component");
    container.addChild(state.widget());
    return container.render(width);
  };
  return { ctx, hud, render, state };
}

describe("compact HUD component", () => {
  it("keeps alerts ahead of long details and never wraps on resize", () => {
    const rt = {
      client: {
        alias: "agent-" + "x".repeat(80), rooms: ["ops", "review"],
        isOnline: () => true, pendingCount: 2,
      },
      deferredInbox: { count: 4 }, transcript: { isEnabled: () => true },
      ledgerFailures: 1, transcriptFailures: 2, injectionFailures: 0,
    } as unknown as MeshRuntime;
    const { hud, render } = surface(rt);
    try {
      for (const width of [1, 8, 44, 86, 180]) {
        const lines = render(width);
        assert.equal(lines.length, 1, `wrapped at ${width} columns`);
        assert.ok(visibleWidth(lines[0]!) <= width);
      }
      const narrow = render(44)[0]!;
      assert.ok(narrow.includes("!err:3"));
      assert.ok(narrow.includes("wait:2"));
      assert.ok(narrow.includes("inbox:4"));
      const wide = render(180)[0]!;
      assert.ok(wide.includes(rt.client.alias));
      assert.ok(wide.includes("@ops+1"));
    } finally {
      hud.detach();
    }
  });

  it("hides the row while the session joins no room", () => {
    let rooms: string[] = [];
    const rt = {
      client: {
        alias: "self",
        get rooms() { return rooms; },
        isOnline: () => true,
        pendingCount: 0,
      },
      deferredInbox: { count: 0 }, transcript: { isEnabled: () => false },
      ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0,
    } as unknown as MeshRuntime;
    const { hud, state } = surface(rt);
    try {
      assert.equal(state.widget, undefined);
      rooms = ["default"];
      hud.onLocalChange();
      assert.equal(typeof state.widget, "function");
      rooms = [];
      hud.onLocalChange();
      assert.equal(state.widget, undefined);
    } finally {
      hud.detach();
    }
  });

  it("shows waits immediately and clears them on answer, abort and expiry", async () => {
    const dirs = makeTempDirs("mesh-hud-");
    const broker = await startTestBroker(dirs.runtimeDir);
    const client = new MeshClient({ alias: "self", runtimeDir: dirs.runtimeDir, noReconnect: true });
    const peer = new MeshClient({ alias: "peer", runtimeDir: dirs.runtimeDir, noReconnect: true });
    const rt = {
      client, transcript: { isEnabled: () => false },
      ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0,
    } as MeshRuntime;
    const { ctx, hud, render } = surface(rt);
    rt.ctx = ctx;
    const text = () => render(100).join("\n");
    try {
      await Promise.all([client.connect(), peer.connect()]);
      attachClientListeners({ sendMessage: () => {}, appendEntry: () => {} } as never,
        rt, () => hud, ctx, client, () => {});
      const sent = await client.send({ to: "peer", message: "answer", awaitReply: true, block: false, timeoutMs: 5000 });
      if (sent.status !== "delivered") assert.fail(`Send failed: ${sent.status}`);
      assert.ok(text().includes("wait:1"));
      await peer.reply(sent.msgId, "done");
      await waitFor(() => !text().includes("wait:"));

      const controller = new AbortController();
      const pending = client.send({ to: "peer", message: "cancel", awaitReply: true, timeoutMs: 5000, signal: controller.signal });
      await waitFor(() => text().includes("wait:1"));
      controller.abort();
      const cancelled = await pending;
      assert.equal(cancelled.status, "error");
      assert.ok(!text().includes("wait:"));

      await client.send({ to: "peer", message: "expire", awaitReply: true, block: false, timeoutMs: 1000 });
      assert.ok(text().includes("wait:1"));
      await waitFor(() => !text().includes("wait:"));
    } finally {
      rt.markDetached?.();
      hud.detach();
      await Promise.all([client.close(), peer.close()]);
      await broker.close();
      dirs.cleanup();
    }
  });
});
