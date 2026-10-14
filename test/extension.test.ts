// test/extension.test.ts — T-B1 regression: inbound handler injection is
// UNCONDITIONAL. A ledger/transcript disk failure (ENOSPC/EACCES/EROFS) must
// never suppress session injection (broker already acked delivered — I4), and
// failures are counted, not swallowed silently.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  handleInboundFrame,
  type InboundFailureCounters,
} from "../src/extension/inbound.js";
import type { InboundMessage } from "../src/extension/pi-types.js";
import { buildFrame, type MeshFrame } from "../src/protocol/envelope.js";

function counters(): InboundFailureCounters {
  return { ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0 };
}

function fakePi(sent: InboundMessage[]): {
  sendMessage: (msg: InboundMessage) => void;
} {
  return {
    sendMessage: (msg) => {
      sent.push(msg);
    },
  };
}

const msgFrame = (): MeshFrame =>
  buildFrame({ type: "msg", from: "bob", to: "alice", room: "default", body: "hi" });

describe("extension inbound handler (T-B1)", () => {
  it("ledger.append throwing does NOT suppress injection; failure is counted", () => {
    const sent: InboundMessage[] = [];
    const c = counters();
    handleInboundFrame(fakePi(sent), null, msgFrame(), {
      ledger: {
        append: () => {
          throw new Error("ENOSPC: no space left on device");
        },
      },
      transcript: { record: () => {} },
      selfAlias: "alice",
      counters: c,
    });
    assert.equal(sent.length, 1, "injection still happens despite ledger failure");
    assert.equal(sent[0]?.customType, "mesh-inbound");
    assert.equal(c.ledgerFailures, 1);
    assert.equal(c.transcriptFailures, 0);
    assert.equal(c.injectionFailures, 0);
  });

  it("transcript.record throwing does NOT suppress injection or ledger; failure is counted", () => {
    const sent: InboundMessage[] = [];
    const c = counters();
    let ledgerAppends = 0;
    handleInboundFrame(fakePi(sent), null, msgFrame(), {
      ledger: {
        append: () => {
          ledgerAppends += 1;
          return {} as never;
        },
      },
      transcript: {
        record: () => {
          throw new Error("EACCES: permission denied");
        },
      },
      selfAlias: "alice",
      counters: c,
    });
    assert.equal(sent.length, 1, "injection still happens despite transcript failure");
    assert.equal(ledgerAppends, 1, "ledger still appended after transcript failure");
    assert.equal(c.transcriptFailures, 1);
    assert.equal(c.ledgerFailures, 0);
  });

  it("an injection failure is caught + counted — one bad frame cannot kill the loop", async () => {
    const c = counters();
    let ledgerAppends = 0;
    const throwingPi = {
      sendMessage: (): void => {
        throw new Error("host exploded");
      },
    };
    handleInboundFrame(throwingPi, null, msgFrame(), {
      ledger: {
        append: () => {
          ledgerAppends += 1;
          return {} as never;
        },
      },
      transcript: { record: () => {} },
      selfAlias: "alice",
      counters: c,
      retryMs: 1,
    });
  // the count now lands AFTER the one bounded retry — a silent drop is
  // the worst outcome once the broker already acked delivered
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(c.injectionFailures, 1);
    assert.equal(ledgerAppends, 0, "a failed injection is not a delivered inbound record");
    // the handler is still usable for the NEXT frame
    const sent: InboundMessage[] = [];
    handleInboundFrame(fakePi(sent), null, msgFrame(), {
      ledger: {
        append: () => {
          ledgerAppends += 1;
          return {} as never;
        },
      },
      transcript: { record: () => {} },
      selfAlias: "alice",
      counters: c,
    });
    assert.equal(sent.length, 1);
    assert.equal(c.injectionFailures, 1, "no new injection failure");
  });

  it("happy path: injection + ledger + transcript, zero failures", () => {
    const sent: InboundMessage[] = [];
    const c = counters();
    let ledgerAppends = 0;
    let transcriptRecords = 0;
    handleInboundFrame(fakePi(sent), null, msgFrame(), {
      ledger: {
        append: () => {
          ledgerAppends += 1;
          return {} as never;
        },
      },
      transcript: {
        record: () => {
          transcriptRecords += 1;
        },
      },
      selfAlias: "alice",
      counters: c,
    });
    assert.equal(sent.length, 1);
    assert.equal(ledgerAppends, 1);
    assert.equal(transcriptRecords, 1);
    assert.deepEqual(c, { ledgerFailures: 0, transcriptFailures: 0, injectionFailures: 0 });
  });
});
