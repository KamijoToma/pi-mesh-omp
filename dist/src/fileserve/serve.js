// fileserve/serve.ts — out-of-band artifact transfer for cross-machine
// meshes. The broker stays a metadata router (issue #13 precedent): a
// one-shot token-guarded HTTP listener on the SENDER streams one file to
// any machine that can reach it, and the offer travels as an ORDINARY
// mesh message (size + sha256 + URL). No new frame types, no broker
// state, nothing persisted anywhere.
import { createHash, randomBytes } from "node:crypto";
import { createReadStream, statSync } from "node:fs";
import { createServer } from "node:http";
import { basename } from "node:path";
import { isIP } from "node:net";
import { FILE_SERVE_DEFAULT_TTL_MS, FILE_SERVE_HTTP_TIMEOUT_MS, FILE_SERVE_MAX_BYTES, FILE_SERVE_MAX_NAME_CHARS, FILE_SERVE_MAX_TTL_MS, FILE_SERVE_MIN_TTL_MS, } from "../shared/config.js";
export const FILE_SERVE_ROUTE = "/mesh-file";
/** After the ONE completed download, the listener lingers this long so a
 *  raced second request (keep-alive pipelining, retry) gets an honest 409
 *  instead of a raw socket close — then shuts down. */
export const FILE_SERVE_GRACE_MS = 1_000;
export class FileServeError extends Error {
    code;
    name = "FileServeError";
    constructor(code, message) {
        super(message);
        this.code = code;
    }
}
function isFileServeError(error) {
    return error instanceof FileServeError;
}
/** Clean public name: basename only, no traversal, control chars out. */
export function sanitizeServeName(path) {
    const raw = basename(path).slice(0, FILE_SERVE_MAX_NAME_CHARS);
    const clean = raw.replace(/[\x00-\x1f\x7f]/g, "").trim();
    if (clean.length === 0 || clean === "." || clean === "..") {
        throw new FileServeError("invalid_name", `cannot derive a safe file name from ${JSON.stringify(raw)}`);
    }
    return clean;
}
/** Parse one host candidate into a bindable address, or null. */
function bindable(host) {
    if (host === "localhost")
        return "127.0.0.1";
    if (isIP(host) !== 0)
        return host;
    return null; // DNS names are offered via their addresses, never bound by name
}
/**
 * Serve ONE file for download at most once, guarded by a one-time
 * 128-bit token, until the TTL closes the listener. Streaming only:
 * the file is never buffered or spooled by this module.
 */
export async function serveFileOnce(options) {
    const ttlMs = Math.min(FILE_SERVE_MAX_TTL_MS, Math.max(FILE_SERVE_MIN_TTL_MS, Math.floor(options.ttlMs ?? FILE_SERVE_DEFAULT_TTL_MS)));
    let stat;
    try {
        stat = statSync(options.path);
    }
    catch {
        throw new FileServeError("file_unreadable", `cannot stat ${options.path}`);
    }
    if (!stat.isFile())
        throw new FileServeError("file_unreadable", `${options.path} is not a regular file`);
    if (stat.size > FILE_SERVE_MAX_BYTES) {
        throw new FileServeError("too_large", `${stat.size}B exceeds FILE_SERVE_MAX_BYTES (${FILE_SERVE_MAX_BYTES}B) — use git/rsync/Taildrop for bigger artifacts`);
    }
    const name = sanitizeServeName(options.path);
    const tokenPath = `${FILE_SERVE_ROUTE}/${randomBytes(16).toString("hex")}`;
    const sha256 = await new Promise((resolve, reject) => {
        const hash = createHash("sha256");
        const stream = createReadStream(options.path);
        stream.on("data", (chunk) => hash.update(chunk));
        stream.on("end", () => resolve(hash.digest("hex")));
        stream.on("error", () => reject(new FileServeError("file_unreadable", `cannot read ${options.path}`)));
    });
    const server = createServer((req, res) => {
        void handleRequest(req, res).catch(() => {
            try {
                res.destroy();
            }
            catch { /* already gone */ }
        });
    });
    // Keep the process free to exit; the TTL timer below is the owner.
    server.unref();
    let settled = false;
    let closeTimer;
    let graceTimer;
    let onAbort;
    let served = false;
    // ES2022 target: no Promise.withResolvers yet.
    let serveResolve;
    const onceServed = new Promise((resolve) => { serveResolve = resolve; });
    const settle = (value) => {
        if (settled)
            return;
        settled = true;
        clearTimeout(closeTimer);
        options.signal?.removeEventListener("abort", onAbort);
        if (value === "downloaded") {
            // Linger a grace window so a raced second request gets an honest 409
            // (keep-alive can deliver it on the just-used connection), then shut
            // down. Requests landing after the window get a refused socket —
            // the honest signal that the offer is gone.
            graceTimer = setTimeout(() => {
                server.close(() => { });
                server.closeIdleConnections?.();
            }, FILE_SERVE_GRACE_MS);
            graceTimer.unref();
        }
        else {
            // Expiry/abort: never stream past the TTL — cut in-flight downloads.
            server.close(() => { });
            for (const res of active)
                res.destroy();
        }
        serveResolve(value);
    };
    const active = new Set();
    const handleRequest = async (req, res) => {
        const url = new URL(req.url ?? "/", "http://placeholder");
        if (req.method !== "GET" || url.pathname !== tokenPath) {
            res.statusCode = 404;
            res.end();
            return;
        }
        if (served) {
            // One download per offer — a second request is refused, not queued.
            res.writeHead(409, { connection: "close" });
            res.end("already downloaded");
            return;
        }
        served = true;
        active.add(res);
        res.on("close", () => {
            active.delete(res);
            // Complete download settles the offer; a client that vanished
            // mid-stream burns it too (partial bytes are never a delivery).
            settle(res.writableEnded && res.writableFinished ? "downloaded" : "expired");
        });
        res.writeHead(200, {
            "content-type": "application/octet-stream",
            "content-length": stat.size,
            "content-disposition": `attachment; filename="${name.replace(/"/g, "")}"`,
            "x-mesh-sha256": sha256,
            connection: "close",
        });
        const stream = createReadStream(options.path);
        stream.on("error", () => res.destroy());
        stream.pipe(res);
        req.setTimeout(FILE_SERVE_HTTP_TIMEOUT_MS, () => {
            res.destroy();
            stream.destroy();
        });
    };
    const addr = await (() => {
        let bindResolve;
        let bindReject;
        const promise = new Promise((resolve, reject) => {
            bindResolve = resolve;
            bindReject = reject;
        });
        const binds = options.hostCandidates.map(bindable).filter((h) => h !== null);
        if (binds.length === 0) {
            bindReject(new FileServeError("no_bindable_host", "no numeric host candidate (IP) to bind — pass a tailscale/LAN address"));
            return promise;
        }
        const tryBind = (index) => {
            if (index >= binds.length) {
                bindReject(new FileServeError("bind_failed", "every candidate address refused the listener"));
                return;
            }
            const host = binds[index];
            const onError = () => {
                server.removeAllListeners("error");
                tryBind(index + 1);
            };
            server.once("error", onError);
            server.listen({ host, port: 0 }, () => {
                server.removeAllListeners("error");
                bindResolve({ ...server.address(), address: host });
            });
        };
        tryBind(0);
        return promise;
    })().catch((error) => {
        server.close();
        throw error;
    });
    closeTimer = setTimeout(() => settle("expired"), ttlMs);
    if (options.signal !== undefined) {
        onAbort = () => settle("expired");
        options.signal.addEventListener("abort", onAbort, { once: true });
        if (options.signal.aborted)
            settle("expired");
    }
    // The URL names the address the listener actually bound, so it is honest
    // (a peer can never reach 0.0.0.0, and a wrong-name URL wastes the one shot).
    const url = `http://${addr.address}:${addr.port}${tokenPath}`;
    const sizeLabel = stat.size >= 1_048_576
        ? `${(stat.size / 1_048_576).toFixed(1)} MiB`
        : `${stat.size} B`;
    const announcement = [
        `[file-offer] ${name} (${sizeLabel})`,
        `sha256=${sha256}`,
        `url=${url}`,
        `expires in ${Math.round(ttlMs / 1000)}s — download once with curl; verify the sha256 before use`,
    ].join("\n");
    return {
        tokenPath,
        announcement,
        fields: { name, size: stat.size, sha256, ttlMs, host: addr.address, port: addr.port, url },
        close: () => settle("expired"),
        onceServed,
    };
}
export { isFileServeError };
