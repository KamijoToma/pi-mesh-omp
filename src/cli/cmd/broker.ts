// cli/cmd/broker.ts — local broker lifecycle (spawn detached / SIGTERM / probe).
import * as fs from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { say, sayErr } from "../out.js";
import { EXIT_OK, EXIT_MESH_FAILURE, EXIT_USAGE } from "../codes.js";
import { BROKER_STOP_POLL_MS, BROKER_STOP_TIMEOUT_MS, STATUS_REQ_TIMEOUT_MS } from "../../shared/config.js";
import { brokerLockPath, brokerSocketPath, runtimeDir } from "../../shared/paths.js";
import { connectProbe, ensureBroker } from "../../client/reconnect.js";


function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // Permission errors do not confirm that a process has exited.
    return !(error instanceof Error && "code" in error && error.code === "ESRCH");
  }
}

export async function cmdBroker(sub: string | undefined): Promise<number> {
  const dir = runtimeDir();
  const sock = brokerSocketPath(dir);
  switch (sub) {
    case "start": {
      try {
        await ensureBroker(dir);
        let pid = "?";
        try {
          pid = fs.readFileSync(brokerLockPath(dir), "utf8").trim();
        } catch {
          // Socket readiness also works for an externally managed broker.
        }
        say(`broker ready pid=${pid} sock=${sock}`);
        return EXIT_OK;
      } catch (error) {
        sayErr(error instanceof Error ? error.message : String(error));
        return EXIT_MESH_FAILURE;
      }
    }
    case "stop": {
      const lockPath = brokerLockPath(dir);
      let owner = "";
      try {
        owner = fs.readFileSync(lockPath, "utf8").trim();
      } catch {
        // An externally managed broker may not have a PID lock.
      }
      // A startup lock can still belong to the host; never signal it as a broker.
      if (!(await connectProbe(sock, STATUS_REQ_TIMEOUT_MS))) {
        say("no ready broker found");
        return EXIT_MESH_FAILURE;
      }
      const pid = Number(owner);
      try {
        // Readiness must apply to the same lock owner, not a new startup.
        if (!Number.isInteger(pid) || pid <= 0 || !pidAlive(pid) ||
          fs.readFileSync(lockPath, "utf8").trim() !== owner) {
          say("no live broker lock found");
          return EXIT_MESH_FAILURE;
        }
        process.kill(pid, "SIGTERM");
        const deadline = Date.now() + BROKER_STOP_TIMEOUT_MS;
        while (pidAlive(pid)) {
          const remaining = deadline - Date.now();
          if (remaining <= 0) {
            sayErr(`broker pid=${pid} did not exit within ${BROKER_STOP_TIMEOUT_MS}ms`);
            return EXIT_MESH_FAILURE;
          }
          await sleep(Math.min(BROKER_STOP_POLL_MS, remaining));
        }
        // Windows terminates without running JS signal handlers. Only remove
        // this stopped broker's lock, never a concurrent replacement owner's.
        try {
          if (fs.readFileSync(lockPath, "utf8").trim() === owner) fs.unlinkSync(lockPath);
        } catch (error) {
          if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
        }
        say(`broker stopped pid=${pid}`);
        return EXIT_OK;
      } catch (error) {
        sayErr(error instanceof Error ? error.message : String(error));
        return EXIT_MESH_FAILURE;
      }
    }
    case "status": {
      const alive = await connectProbe(sock, STATUS_REQ_TIMEOUT_MS);
      let pid = "?";
      try {
        pid = fs.readFileSync(brokerLockPath(dir), "utf8").trim();
      } catch {
        // no lock
      }
      say(`socket=${sock} reachable=${alive} lockPid=${pid}`);
      return alive ? EXIT_OK : EXIT_MESH_FAILURE;
    }
    default:
      sayErr("usage: mesh broker start|stop|status");
      return EXIT_USAGE;
  }
}
