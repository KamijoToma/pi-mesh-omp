import type { CommandDefinition, ExtensionAPI, SessionContext, ToolDefinition, ToolResult } from "./pi-types.js";
import { isOmpHost } from "./host.js";
import { loadReloadSource, readReloadRevision, type ReloadSource } from "./reload-source.js";
import type { MeshHostTui } from "./tui.js";

export interface MeshExtensionInstance {
  readonly reloadApiVersion: 2;
  readonly version: string;
  isReloadSafe(): boolean;
  suspend(): Promise<unknown>;
  restore(ctx: SessionContext, state: unknown): Promise<void>;
  /** Resume inbound delivery only after this instance is the published generation. */
  activate(): void;
  dispose(): Promise<void>;
}

// Pi can reuse the same extension after shutdown/new-session. Its parked code
// must remain available for broker spawn until restart or actual process exit.
const liveSources = new Set<ReloadSource>();
process.once("exit", () => {
  for (const source of liveSources) {
    try {
      source.dispose();
    } catch (error) {
      process.stderr.write(`[mesh] reload snapshot cleanup failed: ${String(error)}\n`);
    }
  }
});

export type MeshExtensionFactory = (api: ExtensionAPI, host: MeshHostTui) => MeshExtensionInstance;

type Callback = (...args: unknown[]) => unknown;
type Definition = Record<string, unknown>;

interface Registrations {
  hooks: Map<string, Callback[]>;
  tools: Map<string, ToolDefinition>;
  commands: Map<string, CommandDefinition>;
  messageRenderers: Map<string, Callback>;
  entryRenderers: Map<string, Callback>;
}

interface Generation {
  factory: MeshExtensionFactory;
  instance: MeshExtensionInstance;
  registrations: Registrations;
  revision: string | undefined;
  source: ReloadSource | undefined;
  disposal?: Promise<void>;
}

interface ReloadRequest {
  ctx: SessionContext;
  sessionId: string | undefined;
  epoch: number;
  phase: "preparing" | "queued" | "switching" | "committed";
  cancelled: boolean;
  source?: ReloadSource;
  candidate?: Generation;
  restoring?: Generation;
  published?: Generation;
}

interface DeferredLifecycle {
  name: string;
  event: unknown;
  ctx: SessionContext;
  resolve(value: unknown): void;
  reject(error: unknown): void;
}

interface RenderComponent {
  render(width: number): string[];
  invalidate(): void;
}

const POLL_MS = 100;
const LIFECYCLE_EVENTS: Record<string, true> = {
  session_start: true, session_switch: true, session_branch: true, session_shutdown: true,
};

function reason(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function notify(ctx: SessionContext, message: string, severity: "info" | "warning" | "error" = "info"): void {
  try {
    ctx.ui.notify(`mesh: ${message}`, severity);
  } catch {
    // A departing/headless UI must not compromise cleanup or the cutover.
  }
}

function sessionId(ctx: SessionContext): string | undefined {
  return ctx.sessionManager?.getSessionId();
}

function promiseLike(value: unknown): value is PromiseLike<unknown> {
  return value !== null && (typeof value === "object" || typeof value === "function") &&
    "then" in value && typeof value.then === "function";
}

function asDefinition(value: object): Definition {
  // Registered definitions are validated in-process objects; their SDK types omit host-specific keys.
  return value as unknown as Definition;
}

/** Compare the actual JSON schema, independently of object-key insertion order. */
function schemaKey(value: unknown): string {
  const visiting = new Set<object>();
  const normalize = (item: unknown): unknown => {
    if (item === null || typeof item === "string" || typeof item === "boolean") return item;
    if (typeof item === "number" && Number.isFinite(item)) return item;
    if (typeof item !== "object" || item === null) throw new Error("Tool schemas must be plain JSON values");
    if (visiting.has(item)) throw new Error("Tool schemas must not contain cycles");
    visiting.add(item);
    let result: unknown;
    if (Array.isArray(item)) {
      result = item.map(normalize);
    } else {
      const prototype = Object.getPrototypeOf(item);
      if (prototype !== Object.prototype && prototype !== null) throw new Error("Tool schemas must be plain JSON objects");
      const object: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      const fields = asDefinition(item);
      for (const key of Object.keys(item).sort()) object[key] = normalize(fields[key]);
      result = object;
    }
    visiting.delete(item);
    return result;
  };
  return value === undefined ? "absent" : JSON.stringify(normalize(value));
}

function sameNames(previous: Map<string, unknown>, next: Map<string, unknown>, kind: string): void {
  const removed = [...previous.keys()].filter((name) => !next.has(name));
  const added = [...next.keys()].filter((name) => !previous.has(name));
  if (removed.length === 0 && added.length === 0) return;
  const changes = [removed.length === 0 ? "" : `removed ${removed.join(", ")}`, added.length === 0 ? "" : `added ${added.join(", ")}`]
    .filter(Boolean).join("; ");
  throw new Error(`${kind} registration names changed (${changes}). Restart the host to apply registration changes; the public API cannot safely unregister them`);
}

function sameCallbacks(previous: object, next: object, label: string): void {
  const keys = (definition: object): string[] => Object.keys(definition)
    .filter((key) => typeof asDefinition(definition)[key] === "function").sort();
  if (JSON.stringify(keys(previous)) !== JSON.stringify(keys(next))) {
    throw new Error(`${label} callback contract changed. Restart the host to apply this incompatible registration`);
  }
}

function validateInstance(instance: MeshExtensionInstance): void {
  if (instance === null || typeof instance !== "object" || instance.reloadApiVersion !== 2 ||
    typeof instance.version !== "string" || instance.version.length === 0 ||
    typeof instance.isReloadSafe !== "function" || typeof instance.suspend !== "function" ||
    typeof instance.restore !== "function" || typeof instance.activate !== "function" || typeof instance.dispose !== "function") {
    throw new Error("Built mesh implementation has an incompatible reload API (expected reloadApiVersion 2 with suspend/restore/activate/dispose). Rebuild a compatible version or restart the host");
  }
}

/** Install stable host entrypoints once; all implementation callbacks stay generation-local. */
export function installReloadableMesh(api: ExtensionAPI, factory: MeshExtensionFactory, entryUrl: string, host: MeshHostTui): void {
  const nativeOmp = isOmpHost(api);
  const hostOn = api.on.bind(api) as unknown as (event: string, handler: Callback) => unknown;
  const installedEvents = new Set<string>();
  const disposedSources = new WeakSet<ReloadSource>();
  const deferredLifecycle: DeferredLifecycle[] = [];
  let active: Generation | undefined;
  let dormant: Generation | undefined;
  let pending: ReloadRequest | undefined;
  let poll: NodeJS.Timeout | undefined;
  let epoch = 0;
  let currentSessionId: string | undefined;
  let latestCtx: SessionContext | undefined;
  let entrypoints = 0;
  let hostBusy = false;
  let switching = false;
  let stopped = false;

  function disposeSource(source: ReloadSource | undefined): void {
    if (source === undefined || disposedSources.has(source)) return;
    source.dispose();
    disposedSources.add(source);
    liveSources.delete(source);
  }

  function disposeInstance(generation: Generation): Promise<void> {
    generation.disposal ??= Promise.resolve().then(() => generation.instance.dispose());
    return generation.disposal;
  }

  async function cleanup(generation: Generation | undefined, removeSource: boolean, ctx: SessionContext): Promise<void> {
    if (generation === undefined) return;
    try {
      await disposeInstance(generation);
    } catch (error) {
      notify(ctx, `generation cleanup failed: ${reason(error)}`, "warning");
    }
    if (removeSource) {
      try {
        disposeSource(generation.source);
      } catch (error) {
        notify(ctx, `snapshot cleanup failed: ${reason(error)}`, "warning");
      }
    }
  }

  function clearPoll(): void {
    clearTimeout(poll);
    poll = undefined;
  }

  function finish(request: ReloadRequest): void {
    if (pending !== request) return;
    pending = undefined;
    clearPoll();
  }

  async function discard(request: ReloadRequest): Promise<void> {
    await cleanup(request.candidate, true, request.ctx);
    if (request.candidate === undefined) {
      try {
        disposeSource(request.source);
      } catch (error) {
        notify(request.ctx, `snapshot cleanup failed: ${reason(error)}`, "warning");
      }
    }
  }

  function cancelPending(message: string): void {
    const request = pending;
    if (request === undefined || request.cancelled) return;
    request.cancelled = true;
    finish(request);
    notify(request.ctx, `reload cancelled: ${message}`, "warning");
    if (request.phase === "preparing" || request.phase === "queued") {
      void discard(request);
    } else if (request.phase === "switching") {
      // dispose() invalidates outstanding restores; the cutover owns final cleanup.
      if (request.candidate !== undefined) void disposeInstance(request.candidate).catch(() => {});
      if (request.restoring !== undefined) void disposeInstance(request.restoring).catch(() => {});
    }
  }

  function observeContext(ctx: SessionContext): void {
    const id = sessionId(ctx);
    if (latestCtx !== undefined && currentSessionId !== id) {
      epoch++;
      cancelPending("the session changed");
    }
    currentSessionId = id;
    latestCtx = ctx;
  }

  function bound(request: ReloadRequest): boolean {
    return !request.cancelled && !stopped && request.epoch === epoch &&
      request.sessionId === currentSessionId && request.sessionId === sessionId(request.ctx);
  }

  function scheduleAttempt(delay = POLL_MS): void {
    if (pending?.phase !== "queued" || stopped || switching) return;
    clearPoll();
    poll = setTimeout(() => {
      poll = undefined;
      const request = pending;
      if (request !== undefined) void attempt(request);
    }, delay);
    poll.unref();
  }

  function tracked(callback: () => unknown): unknown {
    entrypoints++;
    let result: unknown;
    try {
      result = callback();
    } catch (error) {
      entrypoints--;
      scheduleAttempt(0);
      throw error;
    }
    if (promiseLike(result)) {
      return Promise.resolve(result).finally(() => {
        entrypoints--;
        scheduleAttempt(0);
      });
    }
    entrypoints--;
    scheduleAttempt(0);
    return result;
  }

  function runHooks(generation: Generation, name: string, event: unknown, ctx: SessionContext): unknown {
    const hooks = generation.registrations.hooks.get(name) ?? [];
    let index = 0;
    let returned: unknown;
    const next = (): unknown => {
      while (index < hooks.length) {
        const hook = hooks[index++];
        if (hook === undefined) continue;
        const result = hook(event, ctx);
        if (promiseLike(result)) return Promise.resolve(result).then((value) => {
          accept(value);
          return terminal(returned) ? returned : next();
        });
        accept(result);
        if (terminal(returned)) break;
      }
      return returned;
    };
    const accept = (value: unknown): void => { if (value !== undefined) returned = value; };
    const terminal = (value: unknown): boolean => value !== null && typeof value === "object" &&
      ((name === "tool_call" && "block" in value && value.block === true) ||
        (name === "input" && "action" in value && value.action === "handled"));
    return tracked(next);
  }

  async function shutdown(ctx: SessionContext): Promise<void> {
    const generation = active;
    active = undefined;
    if (generation === undefined) return;
    if (!nativeOmp) dormant = generation;
    await cleanup(generation, nativeOmp, ctx);
  }

  function eventHandler(name: string, event: unknown, ctx: SessionContext): unknown {
    if (stopped) {
      if (name !== "session_start" || dormant === undefined) return undefined;
      const previous = dormant;
      const replacement = capture(previous.factory, previous.revision, previous.source);
      preflight(previous, replacement);
      publishRegistrations(replacement);
      active = replacement;
      dormant = undefined;
      stopped = false;
      hostBusy = false;
    }
    observeContext(ctx);
    if (name === "session_shutdown") {
      stopped = true;
      epoch++;
      cancelPending("the session shut down");
      clearPoll();
      if (switching && active !== undefined) void disposeInstance(active).catch(() => {});
    } else if (LIFECYCLE_EVENTS[name]) {
      epoch++;
      cancelPending("the session changed");
      hostBusy = false;
    } else if (name === "agent_start") {
      hostBusy = true;
    } else if (name === "agent_settled" || (name === "agent_end" &&
      !(event !== null && typeof event === "object" && "willContinue" in event && event.willContinue === true))) {
      hostBusy = false;
    }
    if (switching && LIFECYCLE_EVENTS[name]) {
      return new Promise<unknown>((resolve, reject) => deferredLifecycle.push({ name, event, ctx, resolve, reject }));
    }
    const generation = active;
    if (name === "session_shutdown") {
      let result: unknown;
      try {
        result = generation === undefined ? undefined : runHooks(generation, name, event, ctx);
      } catch (error) {
        return shutdown(ctx).then(() => { throw error; });
      }
      return Promise.resolve(result).finally(() => shutdown(ctx));
    }
    if (generation === undefined || (switching && pending?.published !== generation)) return undefined;
    return runHooks(generation, name, event, ctx);
  }

  function installEvent(name: string): void {
    if (installedEvents.has(name)) return;
    hostOn(name, (event, ctx) => eventHandler(name, event, ctx as SessionContext));
    installedEvents.add(name);
  }

  function capture(factoryForGeneration: MeshExtensionFactory, revision: string | undefined, source?: ReloadSource): Generation {
    const registrations: Registrations = {
      hooks: new Map(), tools: new Map(), commands: new Map(), messageRenderers: new Map(), entryRenderers: new Map(),
    };
    let sealed = false;
    const registering = (): void => {
      if (sealed) throw new Error("Mesh reload registrations must be made synchronously by the extension factory");
    };
    const overrides: Record<string, Callback> = {
      on: (event, handler) => {
        registering();
        if (typeof event !== "string" || typeof handler !== "function") throw new Error("Invalid mesh event registration");
        const hooks = registrations.hooks.get(event) ?? [];
        hooks.push(handler as Callback);
        registrations.hooks.set(event, hooks);
        return () => {
          const index = hooks.indexOf(handler as Callback);
          if (index !== -1) hooks.splice(index, 1);
        };
      },
      registerTool: (value) => {
        registering();
        const tool = value as ToolDefinition;
        if (tool === null || typeof tool !== "object" || typeof tool.name !== "string" || tool.name.length === 0 ||
          typeof tool.execute !== "function" || typeof tool.label !== "string" || typeof tool.description !== "string" ||
          tool.parameters === null || typeof tool.parameters !== "object" || Array.isArray(tool.parameters)) throw new Error("Invalid mesh tool registration");
        schemaKey(tool.parameters);
        schemaKey(asDefinition(tool).outputSchema);
        if (registrations.tools.has(tool.name)) throw new Error(`Duplicate mesh tool registration: ${tool.name}`);
        registrations.tools.set(tool.name, { ...tool });
      },
      registerCommand: (name, value) => {
        registering();
        const definition = value as CommandDefinition;
        if (typeof name !== "string" || name.length === 0 || definition === null || typeof definition !== "object" ||
          typeof definition.handler !== "function") throw new Error("Invalid mesh command registration");
        if (registrations.commands.has(name)) throw new Error(`Duplicate mesh command registration: ${name}`);
        registrations.commands.set(name, { ...definition });
      },
      registerMessageRenderer: (name, renderer) => {
        registering();
        if (typeof name !== "string" || typeof renderer !== "function") throw new Error("Invalid mesh message renderer registration");
        registrations.messageRenderers.set(name, renderer as Callback);
      },
      registerEntryRenderer: (name, renderer) => {
        registering();
        if (typeof name !== "string" || typeof renderer !== "function") throw new Error("Invalid mesh entry renderer registration");
        registrations.entryRenderers.set(name, renderer as Callback);
      },
    };
    const boundMethods = new Map<PropertyKey, unknown>();
    const proxy = new Proxy(api, {
      get(target, property) {
        // Keep absent optional capabilities absent (and preserve omp's public api.pi).
        const value = Reflect.get(target, property, target);
        if (typeof value !== "function") return value;
        if (typeof property === "string" && Object.hasOwn(overrides, property)) return overrides[property];
        if (!boundMethods.has(property)) boundMethods.set(property, value.bind(target));
        return boundMethods.get(property);
      },
    });
    const instance = factoryForGeneration(proxy, host);
    sealed = true;
    return { factory: factoryForGeneration, instance, registrations, revision, source };
  }

  function preflight(previous: Generation, candidate: Generation): void {
    validateInstance(candidate.instance);
    const before = previous.registrations;
    const after = candidate.registrations;
    sameNames(before.tools, after.tools, "Tool");
    sameNames(before.commands, after.commands, "Command");
    sameNames(before.messageRenderers, after.messageRenderers, "Message renderer");
    sameNames(before.entryRenderers, after.entryRenderers, "Entry renderer");
    sameNames(before.hooks, after.hooks, "Event");
    for (const [name, tool] of before.tools) {
      const replacement = after.tools.get(name)!;
      sameCallbacks(tool, replacement, `Tool ${name}`);
      if (!nativeOmp && (schemaKey(tool.parameters) !== schemaKey(replacement.parameters) ||
        schemaKey(asDefinition(tool).outputSchema) !== schemaKey(asDefinition(replacement).outputSchema))) {
        throw new Error(`Tool ${name} changed its schema. Pi cannot reliably refresh a live tool schema; restart Pi to apply this build`);
      }
    }
    for (const [name, command] of before.commands) sameCallbacks(command, after.commands.get(name)!, `Command /${name}`);
  }

  function unavailableTool(): ToolResult {
    const message = stopped ? "blocked: mesh is shutting down" : switching ? "blocked: mesh reload is loading; retry after cutover" :
      "blocked: mesh is unavailable after failed rollback; restart the host";
    return { content: [{ type: "text", text: message }], details: { status: "blocked", reason: stopped ? "shutdown" : switching ? "reload_loading" : "reload_failed" } };
  }

  /** Also redirect components retained by the host, not just renderer factory calls. */
  function renderForward(resolve: () => { generation: Generation; callback: Callback; receiver?: object } | undefined, args: unknown[]): unknown {
    let selected = resolve();
    if (selected === undefined) return undefined;
    let component = selected.callback.apply(selected.receiver, args);
    if (component === null || typeof component !== "object" || !("render" in component) || typeof component.render !== "function") return component;
    const refresh = (): RenderComponent | undefined => {
      if (active === undefined) return undefined;
      if (active !== selected?.generation) {
        const next = resolve();
        if (next === undefined) return undefined;
        selected = next;
        component = next.callback.apply(next.receiver, args);
      }
      if (component === null || typeof component !== "object" || !("render" in component) || typeof component.render !== "function") return undefined;
      // Renderer output has the validated Component render surface used by both hosts.
      return component as RenderComponent;
    };
    const facade: RenderComponent = {
      render: (width) => refresh()?.render(width) ?? [],
      invalidate: () => refresh()?.invalidate(),
    };
    const methods = new Map<PropertyKey, Callback>();
    return new Proxy(facade, {
      get(target, property) {
        if (property === "render" || property === "invalidate") return Reflect.get(target, property);
        const current = refresh();
        if (current === undefined) return undefined;
        const value = Reflect.get(current, property, current);
        if (typeof value !== "function") return value;
        if (!methods.has(property)) methods.set(property, (...callArgs) => {
          const updated = refresh();
          if (updated === undefined) return undefined;
          const method = Reflect.get(updated, property, updated);
          return typeof method === "function" ? Reflect.apply(method, updated, callArgs) : undefined;
        });
        return methods.get(property);
      },
      has(target, property) {
        const current = refresh();
        return property in target || (current !== undefined && property in current);
      },
      ownKeys(target) {
        const current = refresh();
        return [...new Set([...Reflect.ownKeys(target), ...(current === undefined ? [] : Reflect.ownKeys(current))])];
      },
      getOwnPropertyDescriptor(target, property) {
        const current = refresh();
        const descriptor = Reflect.getOwnPropertyDescriptor(target, property) ??
          (current === undefined ? undefined : Reflect.getOwnPropertyDescriptor(current, property));
        return descriptor === undefined ? undefined : { ...descriptor, configurable: true };
      },
      getPrototypeOf(target) {
        return Object.getPrototypeOf(refresh() ?? target);
      },
    });
  }

  function toolRegistration(name: string, tool: ToolDefinition): ToolDefinition {
    const definition = { ...tool };
    const callbacks = asDefinition(definition);
    for (const [key, value] of Object.entries(asDefinition(tool))) {
      if (typeof value !== "function") continue;
      callbacks[key] = (...args: unknown[]) => {
        if (key === "execute") {
          const ctx = args[4] as SessionContext;
          observeContext(ctx);
          if (stopped || switching || active === undefined) return Promise.resolve(unavailableTool());
          const current = active.registrations.tools.get(name)!;
          return tracked(() => (asDefinition(current)[key] as Callback).apply(current, args));
        }
        if (key.startsWith("render")) {
          return renderForward(() => {
            if (active === undefined) return undefined;
            const current = active.registrations.tools.get(name);
            const callback = current === undefined ? undefined : asDefinition(current)[key];
            return typeof callback === "function" ? { generation: active, callback: callback as Callback, receiver: current } : undefined;
          }, args);
        }
        if (stopped || switching || active === undefined) return undefined;
        const current = active.registrations.tools.get(name)!;
        return tracked(() => (asDefinition(current)[key] as Callback).apply(current, args));
      };
    }
    return definition;
  }

  function commandRegistration(name: string, command: CommandDefinition): CommandDefinition {
    const definition = { ...command };
    for (const [key, value] of Object.entries(asDefinition(command))) {
      if (typeof value !== "function") continue;
      asDefinition(definition)[key] = (...args: unknown[]) => {
        if (key === "handler") {
          const commandArgs = args[0] as string;
          const ctx = args[1] as SessionContext;
          observeContext(ctx);
          if (name === "mesh" && commandArgs.trim().split(/\s+/)[0] === "reload") {
            if (commandArgs.trim() !== "reload") {
              notify(ctx, "usage: /mesh reload (no additional arguments)", "error");
              return;
            }
            return requestReload(ctx);
          }
          if (stopped || switching || active === undefined) {
            notify(ctx, unavailableTool().content[0]!.text, "warning");
            return;
          }
        } else if (stopped || switching || active === undefined) {
          return undefined;
        }
        const current = active!.registrations.commands.get(name)!;
        return tracked(() => (asDefinition(current)[key] as Callback).apply(current, args));
      };
    }
    return definition;
  }

  function publishRegistrations(generation: Generation, request?: ReloadRequest): void {
    for (const [name, tool] of generation.registrations.tools) {
      api.registerTool(toolRegistration(name, tool));
      if (request !== undefined && !bound(request)) throw new Error("Reload cancelled during tool publication");
    }
    for (const [name, command] of generation.registrations.commands) {
      api.registerCommand(name, commandRegistration(name, command));
      if (request !== undefined && !bound(request)) throw new Error("Reload cancelled during command publication");
    }
  }

  async function replayLifecycle(): Promise<void> {
    if (deferredLifecycle.length === 0) return;
    switching = true;
    try {
      while (deferredLifecycle.length > 0) {
        const deferred = deferredLifecycle.shift()!;
        try {
          const result = stopped || active === undefined ? undefined : await runHooks(active, deferred.name, deferred.event, deferred.ctx);
          deferred.resolve(result);
        } catch (error) {
          deferred.reject(error);
        }
      }
      if (stopped && latestCtx !== undefined) await shutdown(latestCtx);
    } finally {
      switching = false;
    }
  }

  async function recoverCancelled(previous: Generation, candidate: Generation, request: ReloadRequest, restored?: Generation): Promise<void> {
    await cleanup(candidate, true, request.ctx);
    await cleanup(restored, false, request.ctx);
    await cleanup(previous, false, request.ctx);
    if (stopped) {
      active = undefined;
      if (!nativeOmp) dormant = previous;
      else try { disposeSource(previous.source); } catch (error) { notify(request.ctx, `snapshot cleanup failed: ${reason(error)}`, "warning"); }
    } else {
      // The state belongs to the outgoing session. Never restore it into the new one.
      let replacement: Generation | undefined;
      try {
        replacement = capture(previous.factory, previous.revision, previous.source);
        preflight(previous, replacement);
        publishRegistrations(replacement);
        if (stopped) {
          if (!nativeOmp) dormant = replacement;
          await cleanup(replacement, nativeOmp, request.ctx);
          active = undefined;
        } else {
          active = replacement;
        }
      } catch (error) {
        active = undefined;
        await cleanup(replacement, true, request.ctx);
        notify(latestCtx ?? request.ctx, `reload cancellation recovery failed: ${reason(error)}. Restart the host`, "error");
      }
    }
    switching = false;
    await replayLifecycle();
  }

  async function cutover(request: ReloadRequest, previous: Generation, candidate: Generation): Promise<void> {
    request.phase = "switching";
    switching = true;
    let state: unknown;
    let captured = false;
    let rollback: Generation | undefined;
    try {
      state = await previous.instance.suspend();
      captured = true;
      if (!bound(request)) {
        await recoverCancelled(previous, candidate, request);
        return;
      }
      request.restoring = candidate;
      await candidate.instance.restore(latestCtx ?? request.ctx, state);
      if (!bound(request)) {
        await recoverCancelled(previous, candidate, request);
        return;
      }
      publishRegistrations(candidate, request);
      active = candidate;
      request.published = candidate;
      candidate.instance.activate();
      if (!bound(request)) {
        await recoverCancelled(previous, candidate, request);
        return;
      }
      request.phase = "committed";
      request.restoring = undefined;
      switching = false;
      await cleanup(previous, true, request.ctx);
      if (bound(request)) notify(request.ctx, `reloaded v${candidate.instance.version} (${candidate.revision?.slice(0, 12) ?? "unknown revision"})`);
    } catch (error) {
      request.published = undefined;
      if (!bound(request)) {
        await recoverCancelled(previous, candidate, request, rollback);
        return;
      }
      const failure = reason(error);
      try {
        if (!captured) throw new Error("the previous instance could not capture its memory state");
        // A partially restored candidate may already have received new mailbox frames.
        try {
          const newerState = await candidate.instance.suspend();
          if (newerState !== undefined) state = newerState;
        } catch {
          // Restore may have failed before constructing a runtime; original memory remains intact.
        }
        await cleanup(candidate, true, request.ctx);
        if (!bound(request)) {
          await recoverCancelled(previous, candidate, request);
          return;
        }
        rollback = capture(previous.factory, previous.revision, previous.source);
        preflight(previous, rollback);
        request.restoring = rollback;
        await rollback.instance.restore(latestCtx ?? request.ctx, state);
        if (!bound(request)) {
          await recoverCancelled(previous, candidate, request, rollback);
          return;
        }
        publishRegistrations(rollback, request);
        active = rollback;
        request.published = rollback;
        rollback.instance.activate();
        if (!bound(request)) {
          await recoverCancelled(previous, candidate, request, rollback);
          return;
        }
        request.phase = "committed";
        request.restoring = undefined;
        switching = false;
        await cleanup(previous, false, request.ctx);
        if (bound(request)) notify(request.ctx, `reload failed: ${failure}. Rollback succeeded; v${rollback.instance.version} remains active`, "error");
      } catch (rollbackError) {
        request.published = undefined;
        if (!bound(request)) {
          await recoverCancelled(previous, candidate, request, rollback);
          return;
        }
        active = undefined;
        await cleanup(rollback, false, request.ctx);
        await cleanup(candidate, true, request.ctx);
        await cleanup(previous, true, request.ctx);
        switching = false;
        notify(request.ctx, `reload failed: ${failure}. Rollback failed: ${reason(rollbackError)}. Mesh is unavailable; restart the host`, "error");
      }
    } finally {
      finish(request);
      if (!switching && deferredLifecycle.length > 0) await replayLifecycle();
    }
  }

  async function attempt(request: ReloadRequest): Promise<void> {
    if (request.phase !== "queued" || pending !== request) return;
    if (!bound(request)) {
      cancelPending("the session changed");
      return;
    }
    const previous = active;
    const candidate = request.candidate;
    if (previous === undefined || candidate === undefined) return;
    try {
      const ctx = latestCtx ?? request.ctx;
      if (switching || entrypoints > 0 || hostBusy || ctx.isIdle?.() === false || !previous.instance.isReloadSafe()) {
        scheduleAttempt();
        return;
      }
    } catch (error) {
      finish(request);
      await discard(request);
      notify(request.ctx, `reload preflight failed: ${reason(error)}. Current version remains active`, "error");
      return;
    }
    clearPoll();
    await cutover(request, previous, candidate);
  }

  async function requestReload(ctx: SessionContext): Promise<void> {
    if (stopped || active === undefined) {
      notify(ctx, stopped ? "reload is unavailable during shutdown" : "mesh is unavailable after failed rollback; restart the host", "error");
      return;
    }
    if (pending !== undefined || switching) {
      notify(ctx, pending?.phase === "queued" ? "reload is already queued for a safe point" : "reload is already in progress");
      return;
    }
    const request: ReloadRequest = { ctx, sessionId: sessionId(ctx), epoch, phase: "preparing", cancelled: false };
    pending = request;
    try {
      const revision = readReloadRevision(entryUrl);
      if (revision === active.revision) {
        finish(request);
        notify(ctx, `unchanged v${active.instance.version} (${revision.slice(0, 12)}); no reconnect`);
        return;
      }
      const source = await loadReloadSource(entryUrl);
      liveSources.add(source);
      request.source = source;
      if (!bound(request)) {
        await discard(request);
        finish(request);
        return;
      }
      if (source.revision === active.revision) {
        disposeSource(source);
        finish(request);
        notify(ctx, `unchanged v${active.instance.version} (${source.revision.slice(0, 12)}); no reconnect`);
        return;
      }
      const candidate = capture(source.factory, source.revision, source);
      request.candidate = candidate;
      preflight(active, candidate);
      request.phase = "queued";
      await attempt(request);
      if (pending === request && request.phase === "queued") notify(ctx, "reload queued until the host and mesh are idle; current work and reply waits continue");
    } catch (error) {
      finish(request);
      await discard(request);
      if (!request.cancelled && !stopped) notify(ctx, `reload preflight failed: ${reason(error)}. Current version remains active`, "error");
    }
  }

  let initialRevision: string | undefined;
  try {
    initialRevision = readReloadRevision(entryUrl);
  } catch {
    // A source-only install remains usable; requesting reload reports missing build output.
  }
  active = capture(factory, initialRevision);
  validateInstance(active.instance);
  if (!active.registrations.commands.has("mesh")) throw new Error("Mesh implementation must register the /mesh command");
  for (const event of active.registrations.hooks.keys()) installEvent(event);
  installEvent("session_start");
  installEvent("session_shutdown");
  installEvent("agent_start");
  if (nativeOmp) {
    installEvent("session_switch");
    installEvent("session_branch");
    installEvent("agent_end");
  } else {
    installEvent("agent_settled");
  }
  for (const [name] of active.registrations.messageRenderers) {
    api.registerMessageRenderer?.(name, (...args) => renderForward(() => {
      const callback = active?.registrations.messageRenderers.get(name);
      return active !== undefined && callback !== undefined ? { generation: active, callback } : undefined;
    }, args) as RenderComponent | undefined);
  }
  for (const [name] of active.registrations.entryRenderers) {
    api.registerEntryRenderer?.(name, (...args) => renderForward(() => {
      const callback = active?.registrations.entryRenderers.get(name);
      return active !== undefined && callback !== undefined ? { generation: active, callback } : undefined;
    }, args) as RenderComponent | undefined);
  }
  publishRegistrations(active);
}
