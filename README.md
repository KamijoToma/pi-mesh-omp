# pi-mesh — live agent-to-agent communication for Pi and omp

![CI](https://github.com/KamijoToma/pi-mesh-omp/actions/workflows/ci.yml/badge.svg)
![Release](https://github.com/KamijoToma/pi-mesh-omp/actions/workflows/release.yml/badge.svg)
![License](https://img.shields.io/npm/l/pi-mesh-extension)

**pi-mesh** is a standalone **Pi/omp extension** for live agent-to-agent
communication. Local agents talk to each other through a tiny
local broker (NDJSON frames over a unix socket or named pipe, protocol
`mesh.v1`). Presence is **observed** (live sockets), statuses are **honest**
(`delivered` ≠ `read` ≠ `answered`), and the durable ledger is **hash-only**
(message bodies are never persisted). Zero runtime dependencies, Node ≥ 20.

Compatibility fork of [cgarrot/pi-mesh](https://github.com/cgarrot/pi-mesh).
This repository distributes the omp-compatible build through GitHub, not the
upstream npm package.

```
                ┌────────────────────────────────────────────┐
                │  broker (detached) $TMPDIR/mesh-<uid>/     │
                │  peers / rooms / mailbox / rates (memory) │
                └───────▲───────────────▲─────────────▲──────┘
                        │ connexions persistantes NDJSON │
        ┌───────────────┴───┐   ┌───────┴────────┐   ┌┴───────────────┐
        │ client (agent A)  │   │ client (agent B)│   │ CLI mesh       │
        └───────▲───────────┘   └───────▲────────┘   └────────────────┘
        ┌───────┴───────────┐   ┌────────┴────────┐
        │ extension Pi A    │   │ extension Pi B  │
        │ mesh_send/reply/… │   │ injection       │
        │ ledger hash-only  │   │ followUp/steer/ │
        │ transcript opt-in │   │ abort+steer     │
        └───────────────────┘   └─────────────────┘
```

## Features

- **Honest statuses** — `delivered` = written on the recipient socket (or its
  mailbox), `read` = injected into the recipient session, `answered` = an
  explicit `mesh_reply` arrived. `expired` explicitly says late replies are
  still delivered. Never a completion. When a queued message later leaves
  the mailbox without being delivered (TTL expiry or cap eviction), the
  sender receives an async `ack(dropped_offline)` carrying the original
  msg id — a live `awaitReply` mission settles immediately instead of
  burning its whole timeout, and the session gets an inline re-send hint.
- **Rooms & roles** — presence per room, `member` / `observer` roles,
  declarative policy (allow/deny lists, `force` authorization, rate limits).
- **Offline mailbox** — per-alias queue (cap 100, TTL 1 h) flushed at the next
  hello; senders get the honest `queued_offline` status.
- **Broadcast & reply variants** — `broadcast: true` fans out to a whole
  room (honest `deliveredCount/totalCount`), `mesh_reply` supports
  `replyAll` and targeted `to:` replies. `mesh_send` `replyTo: [aliases]`
  designates WHO receives the reply instead of the sender (single or
  several; default: the sender).
- **Group orchestration** — `mesh_wait_all` + launch mode (`awaitReply: true,
  block: false`): send a mission burst, then get ONE honest group verdict
  (who answered with the answer, who is missing). No sleep, no polling.
  Each answer is ALSO delivered to the session as a `[mesh]` reply event
  (wake-on-answer): an idle sender wakes the moment a mission is answered,
  and the answer frame lands in the inbox so it can itself be replied to.
  While a `mesh_wait_all` is in flight the verdict carries the batch instead
  (no double delivery). A blocking `mesh_send` (default) can be aborted with
  ESC — the pending settles immediately as `cancelled` and a late reply is
  still injected.
- **Inbound batching** — bursts are held while the agent is busy (long tool
  call) and injected as ONE batched message; live preview entries show the
  burst in real time while it happens (zero LLM tokens).
- **Per-agent colors** — every alias gets a stable color; messages, batches
  and live entries render inside the pi custom-message box with the sender's
  color, so agents are recognizable at a glance.
- **Read receipts & activity** — `mesh_status` shows who read your messages
  and who is `● working` / `○ idle` / `✕ stuck` (announced turn state + idle
  heuristic), plus a `likely done` summary.
- **File reservations** — claim repo paths before editing; other agents'
  `edit`/`write` calls on those paths are blocked with the holder's name.
  Reservations live with the connection and expire via TTL.
- **Identity persistence** — alias, rooms and reservations survive `/reload`
  (one file per pi session, never overwritten); `/mesh new` and pi `fork`
  hand the identity over; `/mesh reset` factory-resets in place.
- **Multi-machine** — TCP/TLS broker with a shared token; everything works
  unchanged across machines (VPS, LAN, Tailscale…).
- **Tailnet discovery (unreleased)** — opt-in Tailscale Services or exact
  device/tag discovery, authenticated mesh selection, and restart-aware reconnects.
- **Hash-only ledger** — durable history with bodies never stored, plus an
  opt-in redacted transcript. Zero loops: rate caps, anti-duplicate window,
  self-send block, reply dedup, ack-of-ack protection.

## Install

Install the GitHub fork directly; the extension auto-loads on the next session:

```bash
omp install github:KamijoToma/pi-mesh-omp
```

For a pinned release:

```bash
omp install 'github:KamijoToma/pi-mesh-omp#v0.7.2-omp.2'
```

The package keeps the `pi-mesh-extension` identity, so use this fork instead of
the upstream npm install rather than loading both copies. Git checkouts contain
prebuilt `dist/src` modules, the broker and CLI, and the declared skills: no
dependency build hook or host SDK installation is needed.

**From source:**

```bash
git clone https://github.com/KamijoToma/pi-mesh-omp.git
cd pi-mesh-omp
npm install
npm run build
```

Pi auto-loads the project extension. Open **two Pi sessions** in this
directory (each session gets its own alias):

```bash
# session 1            # session 2
pi                      pi
> /mesh alias           > /mesh alias
# → @agent-a1b2c3       # → @agent-d4e5f6
```

Then, in session 1 (tool call by the agent, or ask it):

```
mesh_send { "to": "agent-d4e5f6", "message": "hello from A" }
# → "delivered m_lxyz_ab12cd34"
```

Session 2 receives `[mesh] @agent-a1b2c3 14:32:05 hello from A (m_lxyz_ab12cd34)`
as a follow-up turn and answers with
`mesh_reply { "msgId": "m_lxyz_ab12cd34", "message": "hi A" }`.
(The short format is the v0.5 default — `contextVerbosity: "full"` restores
the legacy `[mesh] @from (room X, priority, HH:MM:SS) body` prefix.)

The broker **auto-spawns** on first use (lockfile in `$TMPDIR/mesh-<uid>/`).
No daemon management needed. Try `npm run smoke` for a full headless demo
(2 clients, mailbox, broker-kill recovery).

## omp compatibility

This compatibility fork preserves modern Pi's native lifecycle and
uses omp's public session, model and managed-timer APIs. It does not import
omp's private IRC/mesh implementation or change the `mesh.v1` protocol.

Build the checkout, then load its entry explicitly:

```bash
npm install
bun check
npm run build
omp --no-extensions -e /absolute/path/to/pi-mesh-omp/index.ts
```

This explicit-only command avoids loading an already-installed upstream copy
at the same time. For normal use with other extensions, replace or disable the
old mesh entry before configuring the fork.

Open a second omp or Pi session with the same `MESH_RUNTIME_DIR` and
`MESH_ROOMS` to share a broker. `MESH_STATE_DIR` controls per-session identity
and ledger storage; defaults are unchanged. Pi's normal package manifest and
entry remain supported.

**Broker executable:** Node and source Bun hosts reuse their JS runtime.
Bun-compiled applications instead locate a real `node`, then `bun`, on PATH;
the compiled application itself is never used as a script interpreter.
Override selection when required:

```bash
MESH_BROKER_EXEC_PATH=/absolute/path/to/node \
  omp --no-extensions -e /absolute/path/to/pi-mesh-omp/index.ts
```

`MESH_BROKER_EXEC_PATH` selects the executable; `MESH_BROKER_ENTRY` selects
the broker script. Both automatic startup and `pimesh broker start` use the
same readiness check. Missing runtimes, script failures and startup timeouts
return actionable diagnostics, not success based only on a child PID.

**Session behavior:** `/mesh new --history` transfers alias, rooms,
reservations and recent history to the new session ID. Ordinary new sessions
start with their own identity; resume restores the target session's identity,
and fork/branch handoffs preserve the outgoing identity. Outgoing callbacks,
deferred messages and provider
holds cannot leak into the replacement session. Model observation is active
only during an omp provider-error hold; switching to a working model releases
the queued messages without requiring another human prompt.

Verified with standalone omp 18.10.3, an omp source checkout reporting 18.4.2,
and Pi 1.2.0. Modern Pi's `agent_settled`, `model_select` and replacement
contexts remain native; older Pi versions are not covered by this matrix.
omp versions without `registerEntryRenderer` omit Pi's display-only live
entry previews, while final inbound rendering and the HUD remain available.


## Tools (Pi/omp extension)

| tool | params | returns (honest one-liner + `details`) |
|---|---|---|
| `mesh_send` | `to?`, `message`, `room?`, `broadcast?`, `priority?`, `reason?`, `awaitReply?`, `block?`, `timeoutMs?`, `refs?`, `replyTo?` | `delivered` / `queued_offline` / `reply: …` / `expired` / `blocked: …` — impossible targets (`"*"`, `"<room>-broadcast"`) refused **locally**; unseen aliases get a soft warning; `awaitReply` toward a peer busy longer than the timeout gets a burst-pattern advisory |
| `mesh_reply` | `msgId`, `message`, `replyAll?`, `to?`, `refs?` | `delivered` or `blocked: reply_without_target` |
| `mesh_wait_all` | `timeoutMs?` | block the turn until every awaited mission is answered (or timeout) — group verdict: who answered (with the answer), who is missing |
| `mesh_status` | `room?`, `all?` | live broker snapshot — peers sharing a room, per-peer version (`⚠` on skew), turn state (`● working / ○ idle / ✕ stuck`), `likely done` summary, read receipts, missions, broker counters |
| `mesh_ledger` | `limit?`, `from?`, `to?`, `room?`, `event?` | durable **hash-only** history — bodies never stored, survives restarts |
| `mesh_history` | `limit?`, `withBodies?` | local **memory ring** (debug — never the ledger) |
| `mesh_reserve` | `paths`, `reason?`, `autoReleaseMs?` | reserve files/dirs — peers' `edit`/`write` get blocked on them; claims expire for conflict checks after `reservationTtlMs` (default 6 h, re-reserve to renew) and can self-release (`autoReleaseMs`) |
| `mesh_release` | `paths?` (omit = all) | release reservations, peers notified immediately |

**The orchestrator pattern** (injected in every session's identity context
and in the bundled skill):

1. Launch the burst: `mesh_send(..., awaitReply: true, block: false)` per
   mission — each returns `delivered` immediately, the mission stays tracked
   in the background (reminders, expiry, answer capture). Every answer
   arrives as a `[mesh]` reply event the moment it lands (the session wakes
   if idle; keep working in the meantime), so `mesh_wait_all` is only needed
   when you must collect the whole batch before continuing.
2. One `mesh_wait_all` for the group verdict — fast answers that arrived
   before the call are included; already-verdict'd missions are never
   re-listed. The verdict is ALSO rendered in the conversation as a colored
   entry: every line with the answering agent's color as the full-width
   background and ADAPTIVE text (dark on light backgrounds, light on dark
   ones — always readable), separated by empty lines (display-only, zero
   LLM tokens).
3. Re-send ONLY to the missing (`✗ NOT ANSWERED`). Never poll with
   `mesh_history`.

**Delivery modes** — `normal` → followUp · `urgent` → steer (interrupts the
current reflection) · `force` → controlled abort of the recipient's turn +
delivery once it settles (requires a `reason`, hashed, never persisted).
Replies always steer. Reply-à-reply (ack-of-ack chains) is delivered as
followUp with an INFO ONLY label — the LLM decides whether it matters.
Reminders arrive with an explicit "reply due for msgId" instruction.

**Read receipts** — when a message is injected into a session, the client
sends a `read` frame back to the sender; `mesh_status` shows
`reads: m_xxx → @agent-2 at 10:22`. This completes the honest-status
promise: `delivered ≠ read ≠ answered`. Since the standalone CLI peer
(`pimesh attach`), `read` also covers the terminal: an interactive TTY render
is the same "reached the recipient's attention surface" as a session
injection — pipes/`--json` never read. Read frames are online-only and
silent (never acked, never mailboxed): a read toward an offline sender is
lost without error.

## CLI (`pimesh`)

Installed as the `pimesh` bin with the package (`npm i -g pi-mesh-extension`),
runnable without a global install via `npx pi-mesh-extension …` (single bin).
When the package is installed through `pi install`, use `npx` or the
`node dist/src/cli/mesh.js` path from the package directory (pi installs do
not put npm bins on PATH). The CLI honors `.mesh/config.json` + env
(`MESH_BROKER_URL`/`MESH_BROKER_TOKEN`…) exactly like extension clients —
remote machines can debug with `pimesh doctor`.

```
pimesh broker start|stop|status          # local broker lifecycle (detached, lockfile)
pimesh peers [--room R]                  # compact snapshot (legacy format)
pimesh status [room] [--all] [--reservations] [--json]
                                         # ●/○/✕/⛔ activity, via= origins, versions,
                                         # reservations + TTL state (stale = alias)
pimesh send [alias] <text…>              # full SendOpts: --priority --reason --refs
                                         #   --reply-to --broadcast --await --launch
                                         #   --timeout --alias --require-online
                                         #   trailing `-` reads the body from stdin
pimesh reply <msgId> <text…> (--to A | --reply-all) --room R [--refs A,B]
pimesh ping <alias> [--timeout MS]       # expired (exit 3) ≠ peer down
pimesh wait [--timeout MS]               # honest one-shot limit: missions live in
                                         #   the launching process
pimesh join <room> [observer]            # debug-only: membership dies with the process
pimesh leave <room>
pimesh reserve <path>… [--reason R] [--hold MS]
                                         # dry-run conflicts by default (exit 4);
                                         #   --hold keeps the claim alive (Ctrl-C releases)
pimesh release [<pattern>…] [--all]      # honest no-op one-shot (claims are
                                         #   connection-scoped)
pimesh ledger [--limit N] [--from --to --room --event] [--json]
                                         # local hash-only ledger + rotations
pimesh tail [-f] [--limit N]             # last N lines, or follow (rotation-safe)
pimesh sessions [--json]                 # persisted identities (adoption targets,
                                         #   current stateDir scope only)
pimesh attach [alias] [--session ID] [--room R] [--json] [--no-read]
                                         # STANDALONE PEER: a full mesh member with
                                         #   no session — REPL or NDJSON script pipe
pimesh config show                       # resolved config (token masked)
pimesh doctor                            # endpoint/lock/config/version diagnostics
```

**Exit codes** — `0` success · `1` mesh failure (`blocked`/`error`) · `2`
usage · `3` `expired`/timeout (a late reply is still delivered) · `4` honest
partial (`queued_offline`, broadcast `N/M`, conflicts, wait timeout with
answers).

**Standalone peer (`attach`)** — use or create a mesh without a Pi session:
receive messages WITH bodies (you are the recipient), send/reply/reserve
from a REPL (`/help` inside) or a scriptable NDJSON pipe (`--json`: stdout
events / stdin commands with correlated `ref`). `attach <alias>` **adopts a
dead session's identity** — rooms, fresh reservations (since refreshed) and
its queued mailbox; if the original session returns it gets a fresh alias
(a documented steal). Read receipts are emitted ONLY in interactive TTY
mode at render time (`--no-read` opts out; `--json`/pipes never read).
No activity announcements: peers see the idle heuristic.

**Observer (`watch`)** — `pimesh watch [room]` joins as an observer and
streams frames live; bodies are NEVER shown, only `bodyHash` (text and
`--json` alike — safe to `| tee`).

The one-shot CLI is not a session: apart from interactive `attach`, it
never emits read receipts.

## Unblocking a stuck agent (force + interrupt)

An agent can get stuck inside a long or hung command (`sleep 600`, an
interactive prompt, a process that ignores everything). `mesh_send` has a
last-resort lever for exactly that:

```jsonc
mesh_send {
  "to": "stuck-agent", "message": "stop the render, deploy first",
  "priority": "force", "reason": "stuck 20 min on a dead command",
  "interrupt": true }
```

`interrupt` **aborts the recipient's blocked turn** — the host abort kills
the running tool's process tree (the same thing ESC does for a human) —
then delivers the message as a prioritized steer. Aborts are **retried a
bounded number of times** (an abort can be swallowed by an end-of-turn
race) within a 10 s window; after that the message is queued anyway.

**Precautions (by design)**: `interrupt` requires `priority: force` AND a
`reason`; it is rate-limited like force (1/min) and **gated by the same
policy**. Force is denied by default — the recipient's mesh must opt in:

```jsonc
// .mesh/policy.json on the RECIPIENT's machine
{ "allow": [{ "from": "*", "to": "*", "room": "*" }],
  "forceAllowedFrom": ["lead", "operator-*"] }
```

**Honest receipts**: the recipient answers with a machine receipt —
`turn aborted, message delivered`, `already idle`, `still busy after N
aborts` or `abort unavailable on this host` — rendered INFO ONLY (it
never wakes the sender, never settles an awaited mission, never
cascades). The recipient also sees a local notification. Receipts can be
disabled with `"interruptReceipts": false` / `MESH_INTERRUPT_RECEIPTS=0`.

Limits: an interrupt never claims a killed process (not observable), and
it does not bypass a provider rate-limit hold (⛔ peers stay held — an
abort cannot heal a dead provider). CLI: `pimesh send <alias> <text…>
--interrupt --priority force --reason "…"`.

## Configuration

`<stateDir>/config.json` (default `<cwd>/.mesh/config.json`, all optional).
Precedence: defaults < config file < environment.

```jsonc
{ "alias": "alice", "rooms": ["default"], "transcript": false,
  "mailboxCap": 100, "mailboxTtlMs": 3600000, "ledgerMaxBytes": 5242880,
  "activityIdleMs": 120000, "activityStuckMs": 900000,
  "reservationTtlMs": 21600000,
  "watchdog": true, "watchdogSpikeBytes": 2097152, "watchdogMaxCalls": 64,
  "contextVerbosity": "compact",
  "inboundBroadcasts": "immediate",
  "interruptReceipts": true,
  "inboundBatchMs": 250, "inboundBatchMaxHoldMs": 30000 }
```

### Deferring unrelated broadcasts

Set `"inboundBroadcasts": "deferred"` or `MESH_INBOUND_BROADCASTS=deferred`
to avoid a model turn for every unrelated room update. The default,
`"immediate"`, preserves existing behavior. Invalid values are ignored
(invalid file values fall back to the default).

Only broadcasts and orphan `replyAll` replies without a case-insensitive
mention of your alias (`@alice` or `alice`, whole alias) are deferred.
Direct messages, mission answers (including LAUNCH wake-on-answer), urgent
and force priority, reminders, and reservation updates remain immediate.
Receipt, transcript, ledger, and mailbox behavior is unchanged.

Deferred frames form a separate, timer-free batch; the next user prompt
queues it with `deliverAs: "nextTurn"` and **no** `triggerTurn`. It begins
`[mesh deferred — N broadcast(s) not addressed to you]`. In the TUI,
`mesh:deferred N` remains in the footer until that prompt starts.
`/mesh inbox` lists sender, time, and a 120-character preview;
`/mesh inbox flush` delivers the pending batch now as one triggered follow-up.
An unrelated direct-message turn does not consume the deferred inbox.

The batch is held locally until input, because Pi has no public API to
cancel an already-enqueued `nextTurn` message (otherwise manual flush would
deliver it twice). If prompt preflight is cancelled after input, messages
already handed to Pi remain queued for the next successful prompt, not
re-sent by flush. Deferred state is session-local and cleared on reset,
reload, or shutdown; use the existing mesh history for older frames.

### Unreleased compatibility changes

- Added opt-in Tailscale mesh discovery and saved selections for Pi, omp, and the CLI.
- Added mesh/broker/boot identity to status and notifications when a broker restarts.
- Fixed doctor reporting success for reachable ports that reject mesh authentication.
- Fixed graph changes delivering old queued messages or suppressing new mission replies.
- Fixed interrupted broker identity initialization preventing a subsequent startup.
- Fixed batched and deferred messages missing read receipts and hash-only history after delivery.
- Fixed JSON diagnostics losing their error envelope when configuration is invalid.
- Fixed cancelled or superseded session selections overwriting a newer mesh configuration.

### v0.7.2-omp.2 portability changes

- Fixed broker stop leaving stale PID locks after Windows process termination.
- Broker stop now waits for termination and preserves locks owned by a replacement process.

### v0.7.2-omp.1 compatibility changes

- Added omp session and model integration while retaining modern Pi behavior.
- Fixed automatic broker startup in compiled hosts and exposed startup failures ([#8](https://github.com/cgarrot/pi-mesh/issues/8)).
- Fixed session identity handoff and release of queued messages after model recovery.
- Fixed file reservations being bypassed by omp multi-file hashline edits.

v0.7 highlights:

- **Deferred broadcasts (opt-in)** — `"inboundBroadcasts": "deferred"`
  (or `MESH_INBOUND_BROADCASTS=deferred`) stops unrelated room broadcasts
  from burning model turns: only broadcasts and orphan `replyAll` replies
  without a whole-alias mention are held. Direct messages, mission answers
  (LAUNCH wake-on-answer included), urgent/force, reminders and
  reservations stay immediate. Held frames surface as `mesh:deferred N`
  in the TUI footer and via `/mesh inbox`; `/mesh inbox flush` delivers
  them now as one triggered follow-up. Receipts, ledger and mailbox
  behavior are unchanged. Contributed by @avifenesh (#7).
- **`mesh:alias` event** — after connect/reconnect (and rename), mesh
  publishes its resolved identity on the shared event bus plus a
  `Symbol.for("pi-mesh:alias")` snapshot for other extensions. Contributed
  by @avifenesh (#7).

v0.6 highlights:

- **Wake-on-answer** — every LAUNCH mission answer (`awaitReply: true,
  block: false`) is delivered to the session the moment it lands (stored in
  the inbox, injected with `triggerTurn` through the hardened inbound path):
  an idle sender wakes, a busy one gets the batch, and the answer frame can
  itself be replied to. While a `mesh_wait_all` is in flight the verdict
  carries the batch instead — never a double delivery. Answers are now
  MORE visible than late (orphan) replies, never less.
- **Cancelable blocking sends** — ESC on a blocking `awaitReply` send
  settles it immediately as an honest `cancelled` (was: hanging until the
  30-min timeout); the mission is dropped and a late reply still arrives
  via the orphan-inject path.

v0.5 highlights:

- **Context watchdog** — notifies when ONE turn grows the session file by
  >2 MB or carries >64 tool calls (degenerate generation; measured incident:
  3450 duplicate calls, +7.9 MB, ×10 turn latency). A ≥1 MB file **drop** is
  detected as a compaction and triggers a mesh-context resync. Opt out:
  `"watchdog": false` or `MESH_WATCHDOG=0`.
- **Compact inbound context** — `[mesh] @from HH:MM:SS body (m_id)` by
  default; the full `↩ reply …` instruction shows on first sight per sender,
  every 20 messages and after 30-min silences (survives `/compact`). The
  legacy format: `"contextVerbosity": "full"` / `MESH_CONTEXT_VERBOSE=1`.
- **Reconnect diff** — the ~500-token identity block is sent once per
  session; reconnects inject a one-line peer diff instead.
- **Reservation TTL 6 h** (was unlimited) — stale claims stop blocking
  peers; long runs re-reserve to renew or use `autoReleaseMs`.
  Opt out: `"reservationTtlMs": 0`.
- **`/mesh stale`** — reservations held by peers, with age and TTL state.
- **`npm run report`** — session/ledger health report: bursts, rejected
  results, blocked sends, leaked reservations, **per-session generation
  latency (median/p90 + last-20-turns median — the "degraded NOW" signal
  a full-session median masks)**; exit 1 on findings. v0.5.3.

`.mesh/policy.json` (declarative governance, evaluated at send time):

```jsonc
{ "allow": [{ "from": "*", "to": "*", "room": "*" }],
  "deny":  [{ "from": "observer-*", "to": "*" }],
  "forceAllowedFrom": ["lead"],
  "rateLimits": { "msgPerMin": 30, "urgentPerMin": 15, "forcePerMin": 1 } }
```

Env overrides: `MESH_ALIAS`, `MESH_ROOMS`, `MESH_RUNTIME_DIR`,
`MESH_STATE_DIR`, `MESH_BROKER_URL`, `MESH_BROKER_TOKEN`, `MESH_LISTEN`,
`MESH_TLS_CERT/KEY/CA`, `MESH_TLS_INSECURE`, `MESH_MAX_FRAME_BYTES`,
`MESH_MAILBOX_CAP`, `MESH_MAILBOX_TTL_MS`, `MESH_TRANSCRIPT=1`,
`MESH_ACTIVITY_IDLE_MS`, `MESH_ACTIVITY_STUCK_MS`,
`MESH_RESERVATION_TTL_MS`, `MESH_INBOUND_BATCH_MS`,
`MESH_INTERRUPT_RECEIPTS=0`,
`MESH_INBOUND_BATCH_MAX_HOLD_MS`, `MESH_POLICY`, `MESH_WATCHDOG=0`,
`MESH_CONTEXT_VERBOSE=1`.

**Commands** — `/mesh status [room] · join <room> [as <alias>] [observer] ·
leave <room> · alias [<new-alias>] · new [--history] · reset · log [on|off] ·
ping <alias> · broker · help`.

- `/mesh join ops as agent-1` claims the alias `agent-1` and joins room
  `ops` in one step (live rename, rooms + reservations re-declared).
- `/mesh new [--history]` opens a fresh pi session like `/new` but hands
  over the mesh identity (alias, rooms, reservations; `--history` also
  transfers the last 30 mesh frames as context). Stale handoffs expire
  after 15 min.
- `/mesh reset` factory-resets the identity of the CURRENT session (fresh
  alias, default rooms, no reservations) without leaving it; `/reload`
  preserves the identity.
- **Identity survives `/reload`**: alias, rooms and reservations are
  persisted in `<stateDir>/identity-<sessionId>.json` — one file per pi
  session, stable across reloads, sessions sharing a stateDir never
  overwrite each other. Stale persisted reservations older than 24 h are
  dropped at load; if a crashed session still holds the alias, the client
  falls back to a random one (notified + persisted) instead of looping.
- **HUD**: a live widget above the editor shows the connection dot, rooms,
  peers with per-agent colors and turn-state markers (`●`/`○`/`✕`),
  pending awaits, transcript state and the last inbound preview.
- `/mesh broker` reports the version, session file size and compaction
  count, with a `/mesh new` hint past 15 MB.
- `/mesh stale` lists every reservation held by peers with its age —
  the operator sees in one glance who to ping or wait for.

The **`mesh-coordination` skill** (skills/mesh-coordination) is bundled in
the package: a protocol guide for agents (reply once per msgId, expired ≠
lost, reservation etiquette, the launch → wait_all rhythm) — loaded on
demand like any pi skill.

## Multi-machine

The mesh is loopback-only by default; to connect several machines (a VPS, a
LAN PC, a MacBook over Wi-Fi …) start the broker on ONE machine with
`MESH_LISTEN=tcp://…` and a shared token, and point the other machines'
clients at it. Since **v0.4.18** the broker listens on **both endpoints at
once** (dual listen): the local unix socket keeps serving local sessions
tokenless (file-perm protected, zero disruption) while the tcp/tls endpoint
admits remote machines with the token.

```bash
# Machine A (broker + agents) — open the port in the firewall
MESH_LISTEN=tcp://0.0.0.0:8712 MESH_BROKER_TOKEN=change-me pi
# → broker up endpoints=tcp://0.0.0.0:8712 + unix:///tmp/mesh-<uid>/broker.sock

# Machine B (clients only — no local broker is spawned)
MESH_BROKER_URL=tcp://<machine-A>:8712 MESH_BROKER_TOKEN=change-me pi
```

- The broker standalone honors `MESH_LISTEN`/`listen` in `config.json`
  (tcp:// and tls://); the local unix socket stays up in tcp/tls mode so
  already-running local sessions reconnect untouched.
- The token is **required for tcp/tls connections** (per connection: a hello
  without it is refused with `invalid_token`, token travels hashed); local
  unix-socket connections never need it.
- The CLI (`mesh doctor|peers|send|reserve|join`) honors `MESH_BROKER_URL`
  / `MESH_BROKER_TOKEN` / `.mesh/config.json` exactly like extension
  clients — remote machines can debug with `mesh doctor`.
- `tcp://` for LAN/VPN (Tailscale/ZeroTier/WireGuard recommended),
  `tls://` for a VPS (set `MESH_TLS_CERT`/`MESH_TLS_KEY` on the broker;
  clients may set `MESH_TLS_CA`, or `MESH_TLS_INSECURE=1` for self-signed —
  dev only).
- **Remote peers are visible**: every peer carries its connection origin
  (`via=tcp:<ip>` / `tls:<ip>`; broker-local unix peers have none) — shown in
  `mesh_status` (`via=… (network origin)`), `/mesh status`, the session
  context block, and the HUD peers line (`alias⌁<last-ip-octet>`). A proxy
  address is not a verified remote device or user identity.
- **Privacy note**: `via` exposes the peer IP as seen by the broker — fine
  on a trusted LAN/home mesh; for a multi-org mesh, gate or truncate it
  before it rides any presence broadcast (future policy hook).
- `MESH_DEBUG=1` (env or `"debug": true` in config.json) logs wire-level
  client lifecycle events to `<stateDir>/client-debug.log` — no bodies.
- Everything works unchanged across machines: rooms, broadcast, read
  receipts, mailbox, reservations, turn state (state lives in the broker).
  `mesh doctor` checks the endpoint/auth on any machine.
- Aliases must stay **unique mesh-wide**: prefix per machine
  (`MESH_ALIAS=pcB-agent-2`). Identities/ledgers stay local to each
  machine; reservations protect the same repo paths when both machines
  share the same git checkout (always reserve repo-relative paths).

### Tailnet discovery (unreleased)

This section describes the working-tree build; the published
`v0.7.2-omp.2` does not include these commands. Load this checkout's
`dist/src/extension/index.js` instead of a second installed mesh extension.

Discovery is **off by default**. It reuses the installed Tailscale client;
no management API token, Go sidecar, IRC transport, or tailnet-wide port scan
is involved. Automatic Service enumeration requires Tailscale **1.102.1+**
and a working `service list --json` command. Linux, macOS, and Windows use
the official CLI; macOS also supports the application-bundled CLI.

#### Publish one broker

Set a strong `MESH_BROKER_TOKEN` in the environment or private `config.json`.
For Tailscale Services, start the broker on a token-protected loopback TCP
endpoint, then publish that endpoint explicitly:

```bash
MESH_ID=main MESH_LISTEN=tcp://127.0.0.1:8712 pimesh broker start
tailscale serve --service=svc:pi-mesh-main --tcp=8712 tcp://127.0.0.1:8712
```

An administrator must first define the Service, permit client access to its
TCP endpoint, and approve the host (or configure Service auto-approval).
Service hosts require **tag-based identity**; do not retag a personal
computer merely to try this feature. Never forward the existing tokenless
Unix socket/named pipe to remote clients.

Without a dedicated tagged host, use direct node discovery instead:

```bash
MESH_ID=main MESH_LISTEN="tcp://$(tailscale ip -4):8712" pimesh broker start
```

The host must have native Tailscale networking. Pure userspace/SOCKS mode
does not transparently give Node TCP sockets tailnet connectivity. Both
deployments retain the existing tokenless local socket for local sessions.

#### Discover and select

```bash
pimesh tailscale status
pimesh tailscale discover --json
pimesh tailscale join main --service svc:pi-mesh-main
# Or choose an exact device name, full MagicDNS name, IP, or unique tag:
pimesh tailscale join main --host my-broker --port 8712
pimesh tailscale join main --tag tag:mesh-broker --port 8712
pimesh tailscale doctor --json
```

The extension has the same actions under `/mesh tailscale`. `join` verifies
the token and returned mesh identity **before** changing `config.json`.
It preserves unrelated settings and secrets. A CLI selection applies to
subsequent sessions; `/mesh tailscale join` also switches the current client.
Explicit graph selection preserves alias, current rooms, and own reservations,
but cancels old batch waits and discards locally buffered old-graph traffic.
Messages already handed to the host SDK remain part of the session history.
Cancelled or replaced sessions cannot publish a stale pending selection.
Read receipts and hash-only history are recorded only after the host accepts
the message, including batched, deferred, retried, and delayed force delivery.


The equivalent opt-in configuration is:

```json
{
  "meshId": "main",
  "tailscale": {
    "mode": "service",
    "service": "svc:pi-mesh-main",
    "port": 8712
  }
}
```

Node mode uses `"mode": "node"` with `"host"` and/or `"tag"`. Omitting the
Service name selects exactly `svc:pi-mesh-<meshId>`, never a random matching
machine. An ambiguous selector fails even when one of the devices is offline.
Device online/active status does not prove that a broker or agent is ready.

Environment overrides: `MESH_ID`, `MESH_BROKER_ID`, `MESH_TAILSCALE_MODE`
(`off|service|node`), `MESH_TAILSCALE_SERVICE`, `MESH_TAILSCALE_HOST`,
`MESH_TAILSCALE_TAG`, `MESH_TAILSCALE_PORT`, and `MESH_TAILSCALE_EXEC_PATH`.
An explicit `MESH_BROKER_URL`/`brokerUrl` takes precedence. Invalid explicit
URLs, missing Services, daemon failures, and failed authentication never
silently join a separate local graph. Conflicting environment overrides
must be removed before a saved selection can take effect.

#### Identity, recovery, and limits

- Tailnet mode requires a broker supporting `meshId`, `brokerId`, and `bootId`
  metadata. Upgrade the broker too; generic legacy TCP/TLS without an expected
  mesh identity remains compatible.
- The standalone broker persists its stable identity in `<stateDir>/broker-id`
  with crash-safe, exclusive publication. A fresh process gets a new boot ID.
  An explicit `brokerId`/`MESH_BROKER_ID` overrides it without rewriting the file.
- Clients pin the first accepted broker ID for that client lifetime; an explicit
  configured broker ID can pin it across sessions. A different broker is refused.
  Restart detection clears stale peer state and re-declares own claims; the old
  in-memory mailbox is **not recovered**.
- Each Service must route to **one actual broker**. Adding independent backends
  does not replicate rooms, aliases, reservations or mailboxes, and is not HA.
- Successful discovery is cached per process for 30 seconds with concurrent
  requests shared. Connection failures refresh discovery. CLI calls are bounded
  to 5 seconds and 1 MiB output; normalized Service port expansions are bounded
  to 65,535 total entries per response.
- Doctor separates discovery, socket reachability, and authenticated mesh
  readiness. Its success exit code requires a completed mesh handshake; status
  and discover intentionally do not claim that advertisements are healthy agents.
- Shared tokens remain required. Mesh IDs and rooms are routing/coordination
  boundaries, not tenant authorization. WhoIs/app-capability authorization and
  trusted PROXY-protocol identity forwarding are not implemented in this stage.

## Reliability notes

- **Broker is stateless**: kill it any time, clients re-hello and re-declare
  rooms + reservations; it re-spawns automatically.
- **Mailbox is volatile**: a broker restart loses queued offline messages;
  senders always get the honest `queued_offline` status.
- **Zero loops**: rate caps (client and broker), anti-duplicate send window,
  self-send block, reply dedup (first answer wins, exact re-sends dropped),
  reply-à-reply protection, `force` requires a reason.
- **No body is ever persisted** outside the opt-in transcript: the ledger is
  hash-only with a fail-closed forbidden-key scan; `identity-pending.json`
  (the `/mesh new --history` handover) is the only opt-in body staging,
  deleted after consumption.
- **Bounds**: frame 64 KiB, body 32 KiB, mailbox 100/1 h, reminds ≤ 2,
  16 rooms/peer, 64 peers/room — every bound is a named constant.
- **Broker down** → tools answer `blocked{broker_unavailable}`, never crash.
- **Turn state** is announced by each session (busy on the first tool call,
  idle when the run settles) and shared with the room; peers without
  announcements fall back to the idle/stuck heuristic. Provider errors are
  detected from the HTTP status: TRANSIENT ones (429 rate limit, 5xx) flag
  the agent `⛔ rate-limited` (peers pause reminders — no ping-pong of turns
  that burn rate-limited requests; `mesh_wait_all` says "retry later");
  PERMANENT ones (401/403/404…) flag `✖ blocked` (retrying won't heal —
  needs a human). The flag sticks for a 30 s cooldown after the last error.
  While flagged, the session also HOLDS inbound injections (messages and
  reminders are queued, nothing burns a failed turn). Detection reads the
  FAILED ASSISTANT TURN (the SDK throws on HTTP errors, so the response
  status is not observable): the same classification pi uses — quota /
  budget limits (FreeUsageLimitError, insufficient_quota…) and auth
  errors → `blocked` with a LONG hold (30 min, the limit must reset or
  the model must change); plain 429/5xx → `rate_limited` with a 60 s
  hold. Switching the model (`model_select`) lifts the hold and delivers
  the backlog immediately.

## Platform notes

- **Windows**: AF_UNIX sockets are unavailable on win32 (`listen` throws
  `EACCES`), so the broker endpoint falls back to a **named pipe**
  (`\\.\pipe\mesh-<hash>-broker`). Everything else is unchanged; the full
  test suite + smoke pass on Windows.

## Known limitations

- Broker restarts lose rooms/mailbox (clients re-declare on hello).
- One shared token for the whole mesh on TCP/TLS — no per-alias
  authorization yet (policy covers `force` and deny lists).
- Aliases are unique mesh-wide by convention (prefix per machine) — no
  cross-machine collision detection beyond the broker's live check.
- The broker is a single process — no clustering or failover.
- File reservations are coordination guards on `edit`/`write`, not a sandbox;
  shell commands and other mutation tools can bypass them.

## Development

```bash
bun check       # strict type check with native TypeScript
npm run build   # ESM/NodeNext output with native TypeScript
npm test        # build + the package-local Node test suite
npm run smoke   # E2E broker/client behavior
```

CI runs the full suite on Node 24 (GitHub Actions); the suite is verified on
Node 20 as well. Tags matching `v*-omp.*` publish a GitHub Release tarball using
the repository's `GITHUB_TOKEN`; this fork never publishes the upstream npm name.
The GitHub install command uses the checked-in distribution without waiting
for release workflow execution.

Rebuild and include `dist/src` whenever runtime source changes. Do not include
`dist/test` or local dependencies in a published commit.

Layout: `src/protocol` (frames, envelope) · `src/broker` (server, rooms,
mailbox, ratelimit, policy) · `src/client` (MeshClient, pending, reconnect) ·
`src/extension` (Pi/omp host boundary, tools, commands, inbound, guards,
ledger, transcript) · `src/cli` · `test/` · `scripts/mesh-smoke.mjs`.

## License

MIT — see [LICENSE](LICENSE).

## For other extensions

After connect/reconnect (and rename), mesh publishes the resolved identity:

```typescript
pi.events.on("mesh:alias", (data) => {
  const { alias, rooms } = data as { alias: string; rooms: string[] };
  // Refresh your own status or integration. No model turn is triggered.
});
const alias = (globalThis as Record<symbol, unknown>)[Symbol.for("pi-mesh:alias")];
```

`alias` has no leading `@`. The symbol is a convenience snapshot of the
latest connected alias in this process, not a cross-process registry;
subscribe to the event for updates. It is undefined before the first
connection and is not a connection-health signal. In-process child sessions
share `globalThis`; consumers needing per-session identity should use their
session's event lifecycle rather than treating the symbol as session-local.

## Contributors

Thanks to everyone who has made pi-mesh better:

- [cgarrot](https://github.com/cgarrot) — maintainer
- [wait4xx](https://github.com/wait4xx) — wake-on-answer & cancellable blocking
  sends (#6), CJK verdict-width crash fix (#4), mailbox drop-notice fix (#3),
  verdict renderer regression tests (#1)
- [avifenesh](https://github.com/avifenesh) — deferred broadcast policy (#7),
  `mesh:alias` extension bridge (#7)
