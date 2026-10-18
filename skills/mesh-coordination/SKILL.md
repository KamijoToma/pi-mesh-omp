---
name: mesh-coordination
description: Guide for using the multi-agent mesh (pi-mesh) — messaging, replies, reservations, anti-loop rules. Read it when you coordinate with other agents or answer a mesh message.
---

# Mesh coordination (pi-mesh)

You are connected to a local mesh of pi agents. Rules and reflexes to avoid
duplicates, loops and confusion.

## Know your identity

- Your alias is injected at session start: `[mesh] you are @<alias> (rooms: …)`.
  It is **stable across /reload**. Use `mesh_alias { alias: "reviewer" }` or
  `/mesh alias <name>` to choose a recognizable name for yourself;
  `mesh_alias {}` shows it. `/mesh reset` gives a fresh random alias and
  `/mesh new` hands the identity to the next session.
- Your mesh alias is a routing identity, independent of the session's task
  title. Mesh leaves session titles to the host and user; check mesh status
  or the mesh widget for your alias and rooms.
- **If in doubt, `mesh_status` shows your alias** (first line) and who is
  online. Never guess who you are from the conversation alone.
- Custom names are routing aliases: peers use the name in `to`, `replyTo` and
  whole-name `@mentions`, including immediate delivery of mentioned deferred
  broadcasts. Names are 2–32 ASCII characters: letter first, then letters,
  digits or hyphens; `@`, whitespace and uppercase are normalized.
- Name yourself before dispatching missions: renaming briefly reconnects and
  cancels in-flight requests. Rooms and reservations survive. Tell collaborators
  the new name; the old alias does NOT redirect to it.
- A session that inherits an alias (e.g. `/mesh new` handoff) IS that agent:
  same alias, same rooms, same reservations.
- You can NEVER take an alias held by a live peer (`alias_taken`). Check
  `mesh_status` for the holder; close that session first or pick another alias.

## Get oriented

- `mesh_status` — who is online, in which room, **status** (`○idle`,
  `✕stuck` = idle long with reservations), and read receipts (`reads:`).
- `mesh_history` — recent exchanges (memory).
- `mesh_ledger` — durable history (hash-only): filter by
  `from/to/room/event`. **Use it to verify before re-sending.**
- `/mesh join <room>` is additive: keep existing room memberships.
- `/mesh switch <room> [as <alias>] [observer]` keeps ONLY the destination,
  leaving every other room, including `default`. It joins first so a refused
  destination does not drop old memberships. A failed leave can leave multiple
  rooms; the command reports the remaining rooms. Check `mesh_status` rather
  than assuming an atomic switch. Use `join` when you need multiple rooms.

## Sending

- `mesh_send { to: "agent-X", message }` — direct message (shared room
  required).
- **Room resolution**: without an explicit `room`, the message goes to
  `default` when you are still in it, otherwise to your FIRST joined room.
  A session that left `default` (e.g. only in `cs-room`) sends into its
  remaining room — never assume `default`. If in doubt, `mesh_status` shows
  your rooms.
- `mesh_send { broadcast: true, room: "cs-room", message }` — announce to the
  whole room (the result reports `delivered N/M`).
- `awaitReply: true` waits for an answer (60 min default and maximum timeout).
- A blocking send yields on actionable input with `status: interrupted`.
  Check `delivery` (`not_sent` / `unknown` / `delivered` / `queued_offline`)
  and `tracked`: written reply missions continue in the background and their
  later answers still wake the session. Do not resend tracked/uncertain work;
  process the new message, then wait again if needed.
- Control tools (`mesh_reply`, `mesh_status`, `mesh_alias`, `mesh_reserve`,
  `mesh_release`) can yield with `operationId` and `pendingOperation: true`.
  They are not cancelled: their real completion arrives as
  `mesh-operation-result`. Do not repeat the operation or reply to its notice.
- **`replyTo: ["agent-Y", "agent-Z"]`** — designate WHO receives the reply
  instead of you (single alias or list). The recipient's plain `mesh_reply`
  then goes to ALL of them. Include yourself in the list if you also want the
  answer (e.g. with `awaitReply`). Default: the reply comes back to the
  sender.
- A message whose sender set `replyTo` shows `(reply goes to @Y, @Z)` —
  answer it with `mesh_reply` as usual; it will be routed to those targets.
  You can still override with `to:` (single) or `replyAll: true` (room).

## Replying (GOLDEN RULES)

1. **Always `mesh_reply { msgId, message }` with the EXACT msgId received** —
   never answer with a new `mesh_send`.
2. **One answer per msgId.** If the result says `⚠️ already replied to this
   msgId recently` — you ALREADY replied: do not insist, the answer is gone
   (`delivered`).
3. **`replyAll: true`** to answer the WHOLE room of the original message
   (e.g. "mission done" visible to everyone).
4. **`to: "agent-Y"`** to forward the answer to another agent than the
   sender.
5. A **reminder** says *"IGNORE this reminder if you ALREADY replied to this
   msgId"*: if you already replied, **ignore it**.
6. **NEVER reply to a reply** (reply-à-reply). Replies to replies arrive with
   the **INFO ONLY** label: read them (a proof or a correction can be
   important), but **never answer with an acknowledgment** — to react
   (question, correction), send a **new message** (`mesh_send`), not a reply.

## Launch a burst, then wait_all (delay-free, general pattern)

1. **Launch** every mission with `awaitReply: true, block: false` — the
   tool normally returns the broker's `delivered`/`queued_offline` ACK and
   keeps the mission in the background. If it yields before the ACK, inspect
   `delivery` and `tracked` rather than assuming success or resending.
   A burst costs one turn, not N blocking calls. **Each answer
   lands as a `[mesh]` reply event** (an idle session wakes; keep working
   in the meantime) and lands in the inbox, so it can be replied to.
2. **`mesh_wait_all { timeoutMs }`** — the turn is suspended INSIDE the
   tool call (no sleep, no polling; default and maximum wait: 60 min).
   It returns the honest group verdict: who answered (with complete,
   multiline answers), who is missing. Answers arriving before the call
   are included; already-reported answers are not re-listed. Expired/failed
   missions remain missing, including those that settled before the call;
   a batch containing only terminal missing missions returns immediately.
   While it runs, answers are carried by the verdict, not injected again.
   Reach for it only when you must collect the whole batch before continuing.
3. **Single sends and group waits can yield without cancelling their tasks.**
   Actionable normal/urgent input returns `interrupted` (`reason: inbound_message`);
   it reaches the next model call as steer, without force or a host-turn abort.
   Deferred broadcasts, machine receipts and reply-to-reply metadata do not
   wake it. A previously-online target leaving the mission room returns
   `error: target_offline`; initial `queued_offline` tasks still wait normally.
   Own connection loss reports `connection_lost`, not a remote task failure.
4. **ESC cancels a pending wait**, with `status: cancelled` and `reason: aborted`.
   Interrupted/error/cancelled verdicts do not consume partial answers:
   tasks and answers remain reportable by the next `mesh_wait_all`.
   Explicit batch clearing cancels the tasks instead (`missions_cancelled`).
5. **Do not resend solely because a task is missing.** Handle new input or
   reconnect, then wait again. Broker mailboxes are memory-only:
   `broker_state_lost_delivery_unknown` does not prove non-execution.
   Confirmed-delivered tasks can still finish after the broker restarts.
- Provider-failure holds still yield foreground waits, but retain ordinary
  message bodies until recovery to avoid failed model turns. Authorized force
  bypasses that hold; it does not clear the protection for ordinary input.
- Never poll with `mesh_history`/`mesh_status` to check who answered.
- `block: false` without `awaitReply` is refused. ESC on a blocking send
  returns `aborted`; a pre-write abort sends nothing, but after a write
  delivery can be unknown. Late replies still arrive; do not duplicate work.

## Orchestrator rhythm (delay-free)

- **NEVER `sleep` while awaiting replies.** A `sleep` is a long tool call:
  incoming messages stay queued until it finishes, then arrive in one big
  burst. Instead: END YOUR TURN — inbound replies trigger a new turn
  automatically (triggerTurn), delivered between tool calls.
- **Process replies as they arrive**, one at a time. Do not batch-answer.
- **`awaitReply: true`** resolves each mission as its answer lands — keep
  the loop tight: send, treat the reply, move on.
- **NEVER send acknowledgments** ("received", "thanks", "ok"). An ack
  generates an ack-of-ack and burns tokens (INFO ONLY replies are already
  labelled; read them, do not answer them).

## Anti-loops (orchestrator)

- **`expired` ≠ lost.** A late answer is delivered and injected
  automatically. Before re-sending a mission: check `mesh_ledger` or
  `mesh_history` to see whether the answer already arrived.
- Do not re-assign an already delivered mission: check the register
  (MISSIONS.md / work folder) AND `mesh_ledger` first.
- Agents can be `✕stuck` (idle with reservations): contact them via
  `mesh_send` before concluding they are lost.

## File reservations

- **`mesh_reserve { paths: [...] }` BEFORE editing** a shared file;
  `mesh_release` as soon as you are done.
- If an `edit`/`write` is **blocked**: another agent reserved the path —
  `mesh_send` the owner to coordinate, do not force it.
- Reservations disappear on disconnect (and after the configured TTL).

## Honest statuses

`delivered` = written on the recipient's socket (≠ read ≠ answered).
`reads:` in `mesh_status` shows who has taken knowledge of your messages.

## Unblocking a stuck agent (escalation ladder)

A peer stuck inside a long/hung command (`✕stuck`, or not answering while a
tool runs) is unblocked through the ESCALATION LADDER — never jump to the
top:

1. `mesh_send` (normal) — maybe they are just busy.
2. `urgent` — steer: interrupts the current reflection, not a running tool.
3. `force` (+ `reason`) — aborts the recipient's turn.
4. `force` + `reason` + `interrupt: true` — **last resort**: aborts the
   blocked turn (kills its running tool's process tree) so the message
   lands with priority. Requires the recipient's policy
   (`forceAllowedFrom`); the recipient answers with an honest machine
   receipt (`turn aborted` / `still busy after N aborts`) that is INFO
   ONLY — it never settles your awaited mission; the real answer still
   comes.

`interrupt` is a destructive lever: always with a reason, never as
spam (rate-limited 1/min), never toward a `⛔ rate-limited` peer (an abort
cannot heal a dead provider).
