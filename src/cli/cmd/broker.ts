// cli/cmd/broker.ts — local broker lifecycle (spawn detached / SIGTERM / probe).
import * as fs from "node:fs";
import { say, sayErr } from "../out.js";
import { EXIT_OK, EXIT_MESH_FAILURE, EXIT_USAGE } from "../codes.js";
import { STATUS_REQ_TIMEOUT_MS } from "../../shared/config.js";
import { brokerLockPath, brokerSocketPath, runtimeDir } from "../../shared/paths.js";
import { connectProbe, ensureBroker } from "../../client/reconnect.js";

function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
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
      // A startup lock can still belong to the host; never signal it as a broker.
      if (!(await connectProbe(sock, STATUS_REQ_TIMEOUT_MS))) {
        say("no ready broker found");
        return EXIT_MESH_FAILURE;
      }
      try {
        const pid = Number(fs.readFileSync(brokerLockPath(dir), "utf8").trim());
        if (Number.isInteger(pid) && pid > 0 && pidAlive(pid)) {
          process.kill(pid, "SIGTERM");
          say(`SIGTERM sent to broker pid=${pid}`);
          return EXIT_OK;
        }
      } catch {
        // no lock
      }
      say("no live broker lock found");
      return EXIT_MESH_FAILURE;
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
