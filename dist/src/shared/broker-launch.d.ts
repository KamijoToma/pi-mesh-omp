/** The optional Bun capabilities used here; no Bun dependency is required. */
interface BunRuntime {
    readonly main?: string;
    readonly isStandaloneExecutable?: boolean;
    which?: (command: string) => string | null;
}
export interface BrokerRuntime {
    readonly execPath: string;
    readonly execArgv: readonly string[];
    readonly bun?: BunRuntime;
}
export interface BrokerLaunch {
    readonly executable: string;
    readonly args: readonly string[];
}
/** Select a JS runtime, never a compiled Bun application's entrypoint. */
export declare function brokerLaunch(entry: string, env?: NodeJS.ProcessEnv, runtime?: BrokerRuntime): BrokerLaunch;
export {};
