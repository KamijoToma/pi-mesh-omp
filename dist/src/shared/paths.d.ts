export declare const BROKER_SOCK_NAME = "broker.sock";
export declare const BROKER_LOCK_NAME = "broker.lock";
export declare const RUNTIME_DIR_PREFIX = "mesh-";
export declare const STATE_DIR_NAME = ".mesh";
export declare const POLICY_FILE_NAME = "policy.json";
export declare const CONFIG_FILE_NAME = "config.json";
export declare const LEDGER_FILE_NAME = "ledger.jsonl";
/** True on win32: AF_UNIX is unavailable there (listen → EACCES on most
 *  builds), so brokerSocketPath falls back to named pipes. */
export declare const IS_WINDOWS: boolean;
/** Runtime dir (socket, lock): $MESH_RUNTIME_DIR or $TMPDIR/mesh-<uid>. */
export declare function runtimeDir(env?: NodeJS.ProcessEnv): string;
/** State dir (ledger, transcripts, policy, config): $MESH_STATE_DIR or <cwd>/.mesh. */
export declare function stateDir(env?: NodeJS.ProcessEnv): string;
/**
 * Socket endpoint for a given runtime dir.
 *
 * Windows: AF_UNIX sockets are unsupported (server.listen on a path throws
 * EACCES on win32) → use a named pipe instead. The pipe name is derived from
 * a stable hash of the dir so every process agrees on the endpoint while
 * different runtime dirs (hermetic tests, MESH_RUNTIME_DIR overrides) stay
 * isolated. Pipes are kernel objects: no file to clean up, and stale pipes
 * simply refuse connections.
 *
 * POSIX: <dir>/broker.sock as before.
 */
export declare function socketPathForDir(dir: string): string;
export declare function brokerSocketPath(dir?: string, env?: NodeJS.ProcessEnv): string;
export declare function brokerLockPath(dir?: string): string;
export declare function policyPath(dir?: string, env?: NodeJS.ProcessEnv): string;
export declare function configPath(dir?: string, env?: NodeJS.ProcessEnv): string;
export declare function ledgerPath(dir?: string, env?: NodeJS.ProcessEnv): string;
