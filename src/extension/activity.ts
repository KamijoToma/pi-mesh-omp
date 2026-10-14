import { PROVIDER_BLOCKED_HOLD_MS, PROVIDER_MODEL_WATCH_MS, PROVIDER_RATE_LIMIT_HOLD_MS } from "../shared/config.js";
import { currentModelKey, scheduleHostTimeout } from "./host.js";
import type { SessionContext } from "./pi-types.js";

export type MeshActivityState = "busy" | "idle" | "rate_limited" | "blocked";

export interface ActivityDependencies {
  context(): SessionContext | null;
  isCurrent(): boolean;
  announce(state: MeshActivityState): void;
  holdUntil(until: number | undefined): void;
  flushHeld(): void;
  observeModel: boolean;
}

export interface MeshActivityReloadState {
  announced?: MeshActivityState;
  heldModel?: string;
  holding: boolean;
  runFailed: boolean;
  heldUntil?: number;
}

/** One controller per session. Only an active error hold owns background timers. */
export class MeshActivity {
  readonly #deps: ActivityDependencies;
  #announced: MeshActivityState | undefined;
  #cancelExpiry: (() => void) | undefined;
  #cancelModelWatch: (() => void) | undefined;
  #heldModel: string | undefined;
  #holding = false;
  #runFailed = false;
  #heldUntil: number | undefined;
  #disposed = false;

  constructor(deps: ActivityDependencies, state?: MeshActivityReloadState) {
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
      if (remaining <= 0) this.recovered();
      else {
        this.#deps.holdUntil(this.#heldUntil);
        this.#cancelExpiry = scheduleHostTimeout(this.#deps.context(), () => this.recovered(), remaining);
        if (this.#deps.observeModel) this.#watchModel();
      }
    }
  }

  snapshotForReload(): MeshActivityReloadState {
    return {
      announced: this.#announced,
      heldModel: this.#heldModel,
      holding: this.#holding,
      runFailed: this.#runFailed,
      heldUntil: this.#heldUntil,
    };
  }

  /** A new socket has no activity declaration from the previous connection. */
  announceCurrent(): void {
    if (!this.#disposed && this.#deps.isCurrent()) this.#deps.announce(this.#announced ?? "idle");
  }

  #announce(state: MeshActivityState): void {
    if (this.#disposed || !this.#deps.isCurrent() || this.#announced === state) return;
    this.#announced = state;
    this.#deps.announce(state);
  }

  beginRun(): void {
    this.#runFailed = false;
    this.checkModel();
  }

  toolCalled(): void {
    this.checkModel();
    if (!this.#holding) this.#announce("busy");
  }

  failed(state: "rate_limited" | "blocked"): void {
    if (this.#disposed || !this.#deps.isCurrent()) return;
    this.#runFailed = true;
    this.#stopTimers();
    this.#holding = true;
    this.#heldModel = currentModelKey(this.#deps.context());
    const ms = state === "rate_limited" ? PROVIDER_RATE_LIMIT_HOLD_MS : PROVIDER_BLOCKED_HOLD_MS;
    this.#heldUntil = Date.now() + ms;
    this.#deps.holdUntil(this.#heldUntil);
    this.#announce(state);
    this.#cancelExpiry = scheduleHostTimeout(this.#deps.context(), () => this.recovered(), ms);
    if (this.#deps.observeModel) this.#watchModel();
  }

  #watchModel(): void {
    this.#cancelModelWatch = scheduleHostTimeout(this.#deps.context(), () => {
      this.#cancelModelWatch = undefined;
      if (this.#disposed || !this.#deps.isCurrent() || !this.#holding) return;
      this.checkModel();
      if (this.#holding) this.#watchModel();
    }, PROVIDER_MODEL_WATCH_MS);
  }

  checkModel(): void {
    if (!this.#holding || this.#disposed || !this.#deps.isCurrent()) return;
    if (currentModelKey(this.#deps.context()) !== this.#heldModel) this.recovered();
  }

  successfulTurn(): void {
    this.#runFailed = false;
    if (this.#holding) this.recovered();
  }

  settled(): void {
    this.checkModel();
    if (!this.#runFailed && !this.#holding) this.#announce("idle");
  }

  recovered(): void {
    if (this.#disposed || !this.#deps.isCurrent()) return;
    const wasHolding = this.#holding;
    this.#holding = false;
    this.#runFailed = false;
    this.#heldUntil = undefined;
    this.#stopTimers();
    this.#deps.holdUntil(undefined);
    this.#announce(this.#deps.context()?.isIdle?.() === false ? "busy" : "idle");
    if (wasHolding) this.#deps.flushHeld();
  }

  #stopTimers(): void {
    this.#cancelExpiry?.();
    this.#cancelModelWatch?.();
    this.#cancelExpiry = undefined;
    this.#cancelModelWatch = undefined;
  }

  dispose(): void {
    this.#disposed = true;
    this.#holding = false;
    this.#stopTimers();
    this.#deps.holdUntil(undefined);
  }
}
