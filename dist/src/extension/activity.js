import { PROVIDER_BLOCKED_HOLD_MS, PROVIDER_MODEL_WATCH_MS, PROVIDER_RATE_LIMIT_HOLD_MS } from "../shared/config.js";
import { currentModelKey, scheduleHostTimeout } from "./host.js";
/** One controller per session. Only an active error hold owns background timers. */
export class MeshActivity {
    #deps;
    #announced;
    #cancelExpiry;
    #cancelModelWatch;
    #heldModel;
    #holding = false;
    #runFailed = false;
    #heldUntil;
    #disposed = false;
    constructor(deps, state) {
        this.#deps = deps;
        if (state === undefined) {
            this.#announce("idle");
            return;
        }
        this.#heldModel = state.heldModel;
        this.#holding = state.holding;
        this.#runFailed = state.runFailed;
        this.#heldUntil = state.heldUntil;
        this.#announce(state.announced ?? "idle");
        if (this.#holding && this.#heldUntil !== undefined) {
            const remaining = this.#heldUntil - Date.now();
            if (remaining <= 0)
                this.recovered();
            else {
                this.#deps.holdUntil(this.#heldUntil);
                this.#cancelExpiry = scheduleHostTimeout(this.#deps.context(), () => this.recovered(), remaining);
                if (this.#deps.observeModel)
                    this.#watchModel();
            }
        }
    }
    snapshotForReload() {
        return {
            announced: this.#announced,
            heldModel: this.#heldModel,
            holding: this.#holding,
            runFailed: this.#runFailed,
            heldUntil: this.#heldUntil,
        };
    }
    /** A new socket has no activity declaration from the previous connection. */
    announceCurrent() {
        if (!this.#disposed && this.#deps.isCurrent())
            this.#deps.announce(this.#announced ?? "idle");
    }
    #announce(state) {
        if (this.#disposed || !this.#deps.isCurrent() || this.#announced === state)
            return;
        this.#announced = state;
        this.#deps.announce(state);
    }
    beginRun() {
        this.#runFailed = false;
        this.checkModel();
    }
    toolCalled() {
        this.checkModel();
        if (!this.#holding)
            this.#announce("busy");
    }
    failed(state) {
        if (this.#disposed || !this.#deps.isCurrent())
            return;
        this.#runFailed = true;
        this.#stopTimers();
        this.#holding = true;
        this.#heldModel = currentModelKey(this.#deps.context());
        const ms = state === "rate_limited" ? PROVIDER_RATE_LIMIT_HOLD_MS : PROVIDER_BLOCKED_HOLD_MS;
        this.#heldUntil = Date.now() + ms;
        this.#deps.holdUntil(this.#heldUntil);
        this.#announce(state);
        this.#cancelExpiry = scheduleHostTimeout(this.#deps.context(), () => this.recovered(), ms);
        if (this.#deps.observeModel)
            this.#watchModel();
    }
    #watchModel() {
        this.#cancelModelWatch = scheduleHostTimeout(this.#deps.context(), () => {
            this.#cancelModelWatch = undefined;
            if (this.#disposed || !this.#deps.isCurrent() || !this.#holding)
                return;
            this.checkModel();
            if (this.#holding)
                this.#watchModel();
        }, PROVIDER_MODEL_WATCH_MS);
    }
    checkModel() {
        if (!this.#holding || this.#disposed || !this.#deps.isCurrent())
            return;
        if (currentModelKey(this.#deps.context()) !== this.#heldModel)
            this.recovered();
    }
    successfulTurn() {
        this.#runFailed = false;
        if (this.#holding)
            this.recovered();
    }
    settled() {
        this.checkModel();
        if (!this.#runFailed && !this.#holding)
            this.#announce("idle");
    }
    recovered() {
        if (this.#disposed || !this.#deps.isCurrent())
            return;
        const wasHolding = this.#holding;
        this.#holding = false;
        this.#runFailed = false;
        this.#heldUntil = undefined;
        this.#stopTimers();
        this.#deps.holdUntil(undefined);
        this.#announce(this.#deps.context()?.isIdle?.() === false ? "busy" : "idle");
        if (wasHolding)
            this.#deps.flushHeld();
    }
    #stopTimers() {
        this.#cancelExpiry?.();
        this.#cancelModelWatch?.();
        this.#cancelExpiry = undefined;
        this.#cancelModelWatch = undefined;
    }
    dispose() {
        this.#disposed = true;
        this.#holding = false;
        this.#stopTimers();
        this.#deps.holdUntil(undefined);
    }
}
