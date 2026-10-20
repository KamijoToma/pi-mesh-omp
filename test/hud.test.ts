// Compact status boundaries and room visibility.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { computePeerStatus } from "../src/client/client.js";
import { renderHudLines, selfRooms, visiblePeerCount, type HudState } from "../src/extension/hud.js";
import { formatPeerActivity } from "../src/extension/tools.js";

const NOW = 1_700_000_000_000;

function base(over: Partial<HudState> = {}): HudState {
  return {
    connected: true,
    connecting: false,
    alias: "alice",
    rooms: ["default"],
    peerCount: 7,
    pending: 0,
    inbox: 0,
    transcriptOn: false,
    ledgerFailures: 0,
    transcriptFailures: 0,
    injectionFailures: 0,
    ...over,
  };
}

describe("compact HUD", () => {
  it("does not present stale peer counts while offline or reconnecting", () => {
    for (const connecting of [false, true]) {
      const [line] = renderHudLines(base({ connected: false, connecting, peerCount: 42 }));
      assert.ok(!line.includes("peers:"));
      assert.ok(line.includes(connecting ? "◐" : "○"));
    }
    const [online] = renderHudLines(base({ connected: true, connecting: true }));
    assert.ok(online.includes("●"));
    assert.ok(online.includes("peers:7"));
    assert.ok(!online.includes("connecting"));
  });

  it("aggregates failures ahead of waits, inbox and identity", () => {
    const [line] = renderHudLines(base({
      pending: 2, inbox: 4, ledgerFailures: 1, transcriptFailures: 2, injectionFailures: 3,
    }));
    assert.ok(line.includes("!err:6"));
    assert.ok(line.indexOf("!err:6") < line.indexOf("peers:7"));
    assert.ok(line.indexOf("wait:2") < line.indexOf("inbox:4"));
    assert.ok(line.indexOf("inbox:4") < line.indexOf("alice"));
  });

  it("omits the default room and summarizes additional rooms", () => {
    assert.ok(!renderHudLines(base())[0].includes("@default"));
    assert.ok(renderHudLines(base({ rooms: ["default", "ops", "review"] }))[0].includes("@ops+2"));
  });
});

describe("peer activity detail", () => {
  it("announced busy, limited and blocked states override an old lastSeenAt", () => {
    const peer = { lastSeenAt: new Date(NOW - 1_800_000).toISOString(), reservations: [{}] };
    for (const [state, expected] of [
      ["busy", "working"], ["rate_limited", "rate-limited"], ["blocked", "blocked"],
    ] as const) {
      const text = formatPeerActivity({ ...peer, activity: { state, at: new Date(NOW).toISOString() } }, 120_000, 900_000, NOW);
      assert.ok(text.includes(expected));
      assert.ok(!text.includes("stuck"));
    }
  });

  it("legacy stuck detection requires a reservation and respects its threshold", () => {
    const lastSeenAt = new Date(NOW - 900_000).toISOString();
    assert.ok(formatPeerActivity({ lastSeenAt, reservations: [{}] }, 120_000, 900_000, NOW).includes("stuck"));
    assert.ok(!formatPeerActivity({ lastSeenAt }, 120_000, 900_000, NOW).includes("stuck"));
    assert.ok(!formatPeerActivity({ lastSeenAt, reservations: [{}] }, 120_000, 900_001, NOW).includes("stuck"));
  });
});

describe("selfRooms (D27): HUD shows the session's rooms, not the mesh-wide list", () => {
  it("returns the SELF peer's rooms from the snapshot", () => {
    const snap = {
      rooms: ["cs-room", "voice"], // broker-wide — must NOT be used
      peers: [
        { alias: "main", rooms: ["voice"] },
        { alias: "agent-1", rooms: ["cs-room"] },
      ],
    };
    assert.deepEqual(selfRooms(snap, "main", ["default"]), ["voice"]);
  });

  it("falls back to local joinedRooms before the first snapshot", () => {
    assert.deepEqual(selfRooms(null, "main", ["default", "voice"]), ["default", "voice"]);
  });

  it("empty self rooms stay empty (peer left every room)", () => {
    const snap = {
      rooms: ["cs-room"],
      peers: [{ alias: "main", rooms: [] }],
    };
    assert.deepEqual(selfRooms(snap, "main", ["voice"]), []);
  });
});

describe("visiblePeerCount: only other peers sharing a room are counted", () => {
  const snap = {
    rooms: ["cs-room", "voice"],
    peers: [
      { alias: "main", rooms: ["voice"] },
      { alias: "agent-1", rooms: ["cs-room"] },
      { alias: "agent-2", rooms: ["cs-room", "voice"] },
    ],
  };

  it("a session alone in voice sees only peers sharing voice", () => {
    assert.equal(visiblePeerCount(snap, "main", ["voice"]), 1);
  });

  it("a cs-room session sees only the peers sharing cs-room", () => {
    // main (voice only) shares NO room with agent-1 → not visible
    assert.equal(visiblePeerCount(snap, "agent-1", ["cs-room"]), 1);
  });

  it("no snapshot → no peers yet", () => {
    assert.equal(visiblePeerCount(null, "main", ["voice"]), 0);
  });

  it("zero rooms → no visible peers (cannot talk to anyone)", () => {
    assert.equal(visiblePeerCount(snap, "main", []), 0);
  });

  it("counts a peer only once when multiple rooms overlap", () => {
    assert.equal(visiblePeerCount(snap, "main", ["cs-room", "voice"]), 2);
  });
});

describe("computePeerStatus (D32)", () => {
  it("active under the idle threshold", () => {
    const s = computePeerStatus(new Date(Date.now() - 10_000).toISOString(), false, 120_000, 900_000);
    assert.equal(s.status, "active");
  });

  it("idle after the threshold", () => {
    const s = computePeerStatus(new Date(Date.now() - 300_000).toISOString(), false, 120_000, 900_000);
    assert.equal(s.status, "idle");
    assert.ok(s.idleFor);
  });

  it("STUCK only when idle past stuckMs AND holding reservations", () => {
    const s1 = computePeerStatus(new Date(Date.now() - 1_800_000).toISOString(), true, 120_000, 900_000);
    assert.equal(s1.status, "stuck");
    const s2 = computePeerStatus(new Date(Date.now() - 1_800_000).toISOString(), false, 120_000, 900_000);
    assert.equal(s2.status, "idle", "no reservations → idle, not stuck");
  });
});

