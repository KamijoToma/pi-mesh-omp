import { isOmpHost, scheduleHostTimeout } from "./host.js";
export const DEFAULT_MESH_LOOP_MAX_RUNS = 8;
export const MAX_MESH_LOOP_RUNS = 50;
const CONTINUE_DELAY_MS = 800;
const MAX_HOST_IDLE_RETRIES = 4;
const MAX_CANCELLED_PROMPTS = 16;
export class MeshLoopController {
    pi;
    getRuntime;
    state;
    cancelledPromptContents = new Map();
    promptSequence = 0;
    constructor(pi, getRuntime) {
        this.pi = pi;
        this.getRuntime = getRuntime;
    }
    isActive() {
        return this.state !== undefined;
    }
    start(rt, ctx, maxRuns = DEFAULT_MESH_LOOP_MAX_RUNS, taskText = "") {
        if (!isOmpHost(this.pi)) {
            return { ok: false, message: "mesh-loop requires OMP; Pi cannot observe its background-job state safely." };
        }
        if (ctx.mode !== "tui") {
            return { ok: false, message: "mesh-loop requires the interactive OMP TUI; headless, RPC, and ACP sessions defer agent-initiated turns." };
        }
        if (typeof ctx.getAsyncJobSnapshot !== "function") {
            return { ok: false, message: "mesh-loop unavailable: this OMP context does not expose getAsyncJobSnapshot()." };
        }
        if (typeof ctx.hasPendingMessages !== "function" || typeof ctx.isIdle !== "function") {
            return { ok: false, message: "mesh-loop unavailable: this OMP context cannot report queued messages and host-idle state." };
        }
        if (typeof ctx.abort !== "function") {
            return { ok: false, message: "mesh-loop unavailable: this OMP context cannot cancel a stopped hidden prompt safely." };
        }
        try {
            if (ctx.getAsyncJobSnapshot() === null) {
                return { ok: false, message: "mesh-loop unavailable: OMP async-job state is not available in this session." };
            }
        }
        catch (error) {
            return { ok: false, message: `mesh-loop unavailable: could not read OMP async-job state (${errorMessage(error)}).` };
        }
        if (!Number.isInteger(maxRuns) || maxRuns < 1 || maxRuns > MAX_MESH_LOOP_RUNS) {
            return { ok: false, message: `mesh-loop max-runs must be an integer from 1 to ${MAX_MESH_LOOP_RUNS}.` };
        }
        if (this.state !== undefined) {
            return { ok: false, message: "a mesh loop is already active; use /mesh-loop stop or mesh_loop_stop first." };
        }
        if (this.getRuntime() !== rt || (ctx.sessionManager?.getSessionId() ?? rt.sessionId) !== rt.sessionId) {
            return { ok: false, message: "mesh-loop could not start because the session changed." };
        }
        let hostIdle;
        try {
            hostIdle = ctx.isIdle();
        }
        catch (error) {
            return { ok: false, message: `mesh-loop could not read host-idle state (${errorMessage(error)}).` };
        }
        const state = {
            runtime: rt,
            sessionId: rt.sessionId,
            maxRuns,
            completedRuns: 0,
            taskText: taskText.trim(),
            awaitingSettle: !hostIdle,
            currentTurnIsLoopPrompt: false,
            promptAwaitingStart: false,
            parkedForMesh: false,
            hostIdleRetries: 0,
        };
        this.state = state;
        if (!hostIdle) {
            return { ok: true, message: `mesh-loop armed (maximum ${maxRuns} runs); waiting for the current turn to settle.` };
        }
        const blocked = this.blocked(rt, ctx);
        state.parkedForMesh = blocked.mesh || blocked.inbound;
        if (blocked.mesh || blocked.inbound || blocked.other) {
            return {
                ok: true,
                message: `mesh-loop armed (maximum ${maxRuns} runs); waiting for pending work before the first prompt.`,
            };
        }
        if (blocked.hostBusy) {
            state.awaitingSettle = true;
            return { ok: true, message: `mesh-loop armed (maximum ${maxRuns} runs); waiting for the current turn to settle.` };
        }
        try {
            this.sendPrompt(state);
        }
        catch (error) {
            this.clearState(state);
            return { ok: false, message: `mesh-loop could not queue its first continuation: ${errorMessage(error)}.` };
        }
        return { ok: true, message: `mesh-loop started (maximum ${maxRuns} runs).` };
    }
    stop(reason) {
        const state = this.state;
        if (state === undefined)
            return { active: false, message: "not_active: no mesh loop is active." };
        this.clearState(state);
        return { active: true, message: `mesh-loop stopped; future automatic prompts are stopped (${reason}).` };
    }
    /** Drop per-session loop state without persisting or transferring it. */
    clear() {
        const state = this.state;
        if (state === undefined)
            return false;
        this.clearState(state);
        return true;
    }
    discard() {
        if (this.state !== undefined) {
            const state = this.state;
            state.cancelTimer?.();
            state.cancelTimer = undefined;
            state.promptAwaitingStart = false;
            state.pendingPromptMarker = undefined;
            state.pendingPromptContent = undefined;
            this.state = undefined;
        }
        this.cancelledPromptContents.clear();
    }
    hasPendingPromptCancellation() {
        return this.cancelledPromptContents.size > 0;
    }
    onAgentStart(rt, ctx) {
        const state = this.currentState(rt, ctx);
        if (state === undefined)
            return;
        state.cancelTimer?.();
        state.cancelTimer = undefined;
        state.hostIdleRetries = 0;
        if (state.promptAwaitingStart) {
            state.promptAwaitingStart = false;
            state.pendingPromptMarker = undefined;
            state.pendingPromptContent = undefined;
            state.currentTurnIsLoopPrompt = true;
        }
        state.awaitingSettle = true;
    }
    /** Abort a queued loop-only prompt if its loop was stopped before dispatch. */
    onBeforeAgentStart(rt, event, ctx) {
        if (this.getRuntime() !== rt || (ctx.sessionManager?.getSessionId() ?? rt.sessionId) !== rt.sessionId)
            return;
        const prompt = event !== null && typeof event === "object" && "prompt" in event &&
            typeof event.prompt === "string" ? event.prompt : undefined;
        if (prompt === undefined)
            return;
        for (const [marker, content] of this.cancelledPromptContents) {
            if (!prompt.includes(marker))
                continue;
            this.cancelledPromptContents.delete(marker);
            if (prompt.trim() !== content.trim())
                return;
            try {
                ctx.abort?.();
            }
            catch {
                // A stale session must not turn a cancelled loop into an extension failure.
            }
            return;
        }
    }
    onSettled(rt, ctx) {
        const state = this.currentState(rt, ctx);
        if (state === undefined || !state.awaitingSettle)
            return;
        state.awaitingSettle = false;
        if (state.currentTurnIsLoopPrompt) {
            state.currentTurnIsLoopPrompt = false;
            state.completedRuns += 1;
            if (state.completedRuns >= state.maxRuns) {
                this.clearState(state);
                return;
            }
        }
        this.continueWhenReady(state, ctx);
    }
    /** Called only after a mesh inbound message has been accepted by sendMessage. */
    onInboundInjected(rt) {
        const state = this.state;
        if (state === undefined || state.runtime !== rt || !state.parkedForMesh)
            return;
        const ctx = rt.ctx;
        if (ctx !== null)
            this.continueWhenReady(state, ctx);
    }
    /** Any terminal mission settlement can unblock a parked loop. */
    onMissionsChanged(rt) {
        const state = this.state;
        if (state === undefined || state.runtime !== rt || !state.parkedForMesh || rt.client.pendingCount !== 0)
            return;
        // Matched replies settle before MeshClient emits the inbound frame. Let
        // attach.ts enqueue or batch that frame before the readiness check.
        queueMicrotask(() => {
            if (this.state !== state || state.runtime !== rt || rt.client.pendingCount !== 0)
                return;
            const ctx = rt.ctx;
            if (ctx !== null)
                this.continueWhenReady(state, ctx);
        });
    }
    currentState(rt, ctx) {
        const state = this.state;
        if (state === undefined)
            return undefined;
        if (state.runtime !== rt || this.getRuntime() !== rt || state.sessionId !== rt.sessionId ||
            (ctx.sessionManager?.getSessionId() ?? state.sessionId) !== state.sessionId) {
            this.clearState(state);
            return undefined;
        }
        return state;
    }
    continueWhenReady(state, ctx) {
        if (this.state !== state || state.awaitingSettle)
            return;
        state.cancelTimer?.();
        state.cancelTimer = undefined;
        const blocked = this.blocked(state.runtime, ctx);
        state.parkedForMesh = blocked.mesh || blocked.inbound;
        if (blocked.mesh || blocked.inbound || blocked.other)
            return;
        state.cancelTimer = scheduleHostTimeout(ctx, () => {
            state.cancelTimer = undefined;
            if (this.state !== state || state.awaitingSettle)
                return;
            const activeCtx = state.runtime.ctx ?? ctx;
            if (this.currentState(state.runtime, activeCtx) !== state)
                return;
            const rechecked = this.blocked(state.runtime, activeCtx);
            state.parkedForMesh = rechecked.mesh || rechecked.inbound;
            if (rechecked.mesh || rechecked.inbound || rechecked.other)
                return;
            if (rechecked.hostBusy) {
                if (state.hostIdleRetries >= MAX_HOST_IDLE_RETRIES) {
                    this.clearState(state);
                    try {
                        activeCtx.ui.notify("mesh-loop stopped because OMP did not return to idle after settlement.", "warning");
                    }
                    catch {
                        // Session teardown may invalidate the UI while the timer is firing.
                    }
                    return;
                }
                state.hostIdleRetries += 1;
                this.continueWhenReady(state, activeCtx);
                return;
            }
            state.hostIdleRetries = 0;
            try {
                this.sendPrompt(state);
            }
            catch (error) {
                this.clearState(state);
                try {
                    activeCtx.ui.notify(`mesh-loop stopped: could not queue the next continuation (${errorMessage(error)}).`, "warning");
                }
                catch {
                    // Session teardown may invalidate the UI while the timer is firing.
                }
            }
        }, CONTINUE_DELAY_MS);
    }
    blocked(rt, ctx) {
        const mesh = rt.client.pendingCount !== 0;
        let inbound = false;
        let other = (rt.pendingToolOperations?.size ?? 0) !== 0;
        try {
            inbound = rt.hasPendingInboundDelivery?.() === true;
        }
        catch {
            inbound = true;
        }
        try {
            const snapshot = ctx.getAsyncJobSnapshot?.();
            const delivery = snapshot?.delivery;
            if (snapshot === null || snapshot === undefined || !Array.isArray(snapshot.running) || snapshot.running.length !== 0 ||
                delivery === undefined || delivery.queued !== 0 || delivery.delivering !== false ||
                !Array.isArray(delivery.pendingJobIds) || delivery.pendingJobIds.length !== 0) {
                other = true;
            }
        }
        catch {
            other = true;
        }
        try {
            if (ctx.hasPendingMessages?.() !== false)
                other = true;
        }
        catch {
            other = true;
        }
        let hostBusy = true;
        try {
            hostBusy = ctx.isIdle?.() !== true;
        }
        catch {
            hostBusy = true;
        }
        return { mesh, inbound, other, hostBusy };
    }
    sendPrompt(state) {
        const marker = `mesh-loop-${state.sessionId}-${++this.promptSequence}`;
        const task = state.taskText === "" ? "" : `\nAssigned task: ${state.taskText}`;
        const content = `[internal ${marker}]\n` +
            "Continue the current assigned task within its existing scope. Do not expand the task.\n" +
            "Never ask the end user for decisions or clarification. If a decision is needed, use mesh_send to ask the designated coordinator and wait for that answer.\n" +
            "For launch-mode mesh missions (mesh_send with awaitReply:true and block:false), explicitly call mesh_wait_all to collect the replies. For your own OMP background jobs, explicitly use OMP wait. Do not sleep or poll.\n" +
            "Do not call mesh_loop_stop while waiting. Call mesh_loop_stop only when the task is genuinely complete or irrecoverably blocked." + task;
        state.pendingPromptMarker = marker;
        state.pendingPromptContent = content;
        state.promptAwaitingStart = true;
        try {
            this.pi.sendMessage({
                customType: "mesh-loop",
                content,
                display: false,
            }, { triggerTurn: true, deliverAs: "followUp" });
        }
        catch (error) {
            state.promptAwaitingStart = false;
            state.pendingPromptMarker = undefined;
            state.pendingPromptContent = undefined;
            throw error;
        }
    }
    clearState(state) {
        state.cancelTimer?.();
        state.cancelTimer = undefined;
        if (state.promptAwaitingStart && state.pendingPromptMarker !== undefined && state.pendingPromptContent !== undefined) {
            this.cancelledPromptContents.set(state.pendingPromptMarker, state.pendingPromptContent);
            while (this.cancelledPromptContents.size > MAX_CANCELLED_PROMPTS) {
                const oldest = this.cancelledPromptContents.keys().next().value;
                if (oldest === undefined)
                    break;
                this.cancelledPromptContents.delete(oldest);
            }
        }
        state.promptAwaitingStart = false;
        state.pendingPromptMarker = undefined;
        state.pendingPromptContent = undefined;
        state.currentTurnIsLoopPrompt = false;
        if (this.state === state)
            this.state = undefined;
    }
}
function errorMessage(error) {
    return error instanceof Error ? error.message : String(error);
}
