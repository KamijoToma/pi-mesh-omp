/**
 * Resolve the broker entry, in order
 * 1. $MESH_BROKER_ENTRY override (highest priority, returned as-is).
 * 2. Sibling candidate: <moduleDir>/../broker/broker.js — only if it exists on disk.
 * 3. Repo-root fallback: nearest package.json walking up from moduleDir, then
 *  <root>/dist/src/broker/broker.js — only if it exists (covers jiti-loaded
 *  TS sources, where the sibling is broker.ts, not broker.js).
 * 4. Otherwise throw a descriptive error listing the tried paths.
 * Never returns a path that does not exist on disk (except the explicit override).
 */
export declare function brokerEntryPath(env?: NodeJS.ProcessEnv, moduleDir?: string): string;
/** Try to connect to the broker socket. */
export declare function connectProbe(socketPath: string, timeoutMs: number): Promise<boolean>;
/** Try to connect to a remote tcp/tls broker endpoint (host/port probe). */
export declare function connectProbeTcp(host: string, port: number, timeoutMs: number): Promise<boolean>;
/** Share in-flight startup within a host; the PID lock coordinates other hosts. */
export declare function ensureBroker(runtimeDir: string): Promise<string>;
/** Reconnect backoff: 250 ms × 2^n, capped at 5 s. */
export declare function backoffMs(attempt: number): number;
