// cli/cmd/doctor.ts — resolved endpoint, socket reachability and mesh authentication.
// A reachable TCP port is not a healthy mesh. Tokens stay masked.
import { existsSync, readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { say } from "../out.js";
import { EXIT_OK, EXIT_MESH_FAILURE } from "../codes.js";
import { loadConfig } from "../../shared/config.js";
import { brokerLockPath, brokerSocketPath, configPath, policyPath, runtimeDir, stateDir } from "../../shared/paths.js";
import { loadPolicy } from "../../broker/policy.js";
import { diagnoseConnection } from "../../tailscale/diagnostics.js";
import { MESH_VERSION } from "../../shared/version.js";
function pidAlive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    }
    catch {
        return false;
    }
}
export async function cmdDoctor() {
    const dir = runtimeDir();
    const sock = brokerSocketPath(dir);
    const lock = brokerLockPath(dir);
    const cfg = loadConfig(stateDir());
    const diagnostic = await diagnoseConnection(cfg, dir);
    const url = diagnostic.target?.url ?? cfg.brokerUrl;
    const listen = cfg.listen;
    const reachable = diagnostic.reachable;
    let lockInfo = "absent";
    let stale = false;
    if (existsSync(lock)) {
        const pid = Number(readFileSync(lock, "utf8").trim());
        if (Number.isFinite(pid)) {
            const alive = pidAlive(pid);
            lockInfo = `pid=${pid} alive=${alive}`;
            stale = !alive;
        }
        else {
            lockInfo = "present (no pid)";
        }
    }
    let cfgInfo = "absent (defaults)";
    if (existsSync(configPath(stateDir())))
        cfgInfo = configPath(stateDir());
    // T1 visibility: force (and interrupt) is DENIED until the policy opts
    // in — say so instead of letting senders discover it the hard way.
    let forceInfo = "denied by default (forceAllowedFrom empty) — see README \"Unblocking a stuck agent\"";
    try {
        const policy = loadPolicy(policyPath(stateDir()));
        if (policy.forceAllowedFrom.length > 0)
            forceInfo = `allowed from: ${policy.forceAllowedFrom.join(",")}`;
    }
    catch {
        // unreadable policy — keep the default hint
    }
    const tokenInfo = cfg.brokerToken !== undefined && cfg.brokerToken !== ""
        ? `set (sha256:${createHash("sha256").update(cfg.brokerToken, "utf8").digest("hex").slice(0, 8)})`
        : "absent";
    for (const line of [
        `runtimeDir: ${dir}`,
        diagnostic.resolved
            ? (url !== undefined && url !== null ? `endpoint: ${url} reachable=${reachable}` : `socket: ${sock} reachable=${reachable}`)
            : "endpoint: unresolved reachable=false",
        `brokerUrl: ${url ?? (diagnostic.resolved ? "(local socket/pipe)" : "(unresolved)")}`,
        `source: ${diagnostic.target?.source ?? "unresolved"}`,
        `discovery: ${diagnostic.resolved ? "resolved" : "failed"}`,
        `authenticated: ${diagnostic.authenticated}`,
        `listen: ${listen ?? "(unix socket)"}`,
        `lock: ${lockInfo}${stale ? " STALE" : ""}`,
        `config: ${cfgInfo}`,
        `force: ${forceInfo}`,
        `token: ${tokenInfo}`,
        `protocol: mesh.v1`,
        `cliVersion: ${MESH_VERSION}`,
        ...(diagnostic.broker !== undefined
            ? [`meshId: ${diagnostic.broker.meshId}`, `brokerId: ${diagnostic.broker.brokerId}`, `bootId: ${diagnostic.broker.bootId}`]
            : []),
        ...(diagnostic.error !== undefined ? [`error: ${diagnostic.error}`] : []),
    ]) {
        say(line);
    }
    return diagnostic.authenticated ? EXIT_OK : EXIT_MESH_FAILURE;
}
