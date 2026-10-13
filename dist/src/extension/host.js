/** Omp injects its public SDK facade; Pi does not. No process-name guessing. */
export function isOmpHost(pi) {
    return "pi" in pi && typeof pi.pi === "object" && pi.pi !== null;
}
export function registerHostLifecycle(pi, handlers) {
    if (isOmpHost(pi)) {
        // The capability check selects the host; the SDK checks its event/context API.
        const omp = pi;
        omp.on("session_switch", (event, ctx) => handlers.switchSession(ctx, event.reason === "fork"));
        omp.on("session_branch", (_event, ctx) => handlers.switchSession(ctx, true));
        omp.on("agent_end", (event, ctx) => {
            if (event.willContinue !== true)
                handlers.settled(ctx);
        });
    }
    else {
        // Modern Pi's settled event excludes retries, compaction and queued continuations.
        const nativePi = pi;
        nativePi.on("agent_settled", (_event, ctx) => handlers.settled(ctx));
        nativePi.on("model_select", (_event, ctx) => handlers.modelSelected(ctx));
        nativePi.on("session_before_fork", () => handlers.beforeFork());
    }
}
/** Return a cancellation closure, keeping host timer handle types inside the boundary. */
export function scheduleHostTimeout(ctx, callback, ms) {
    if (ctx?.setTimeout !== undefined && ctx.clearTimer !== undefined) {
        const timer = ctx.setTimeout(callback, ms);
        const clear = ctx.clearTimer;
        return () => clear(timer);
    }
    const timer = setTimeout(callback, ms);
    timer.unref();
    return () => clearTimeout(timer);
}
export function currentModelKey(ctx) {
    const model = ctx?.models?.current() ?? ctx?.model;
    return model === undefined ? undefined : `${model.provider}/${model.id}`;
}
/** Startup remains recoverable, but never disappears behind a silent catch. */
export function reportConnectionError(pi, ctx, error) {
    const reason = error instanceof Error ? error.message : String(error);
    const message = `mesh: broker connection failed: ${reason}. Retry with /mesh status after fixing the broker runtime/configuration.`;
    try {
        ctx?.ui.notify(message, "error");
    }
    catch {
        // Headless or stale UI: the structured diagnostic remains available below.
    }
    try {
        pi.appendEntry("mesh-error", { operation: "connect", reason, at: new Date().toISOString() });
    }
    catch {
        // The session may already be shutting down.
    }
    try {
        pi.events?.emit("mesh:error", { operation: "connect", reason });
    }
    catch {
        // Diagnostics must not turn a recoverable connection error into a host failure.
    }
}
