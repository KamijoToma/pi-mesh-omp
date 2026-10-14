// shared/broker-identity.ts — standalone broker identity, never client/session state.
import { randomUUID } from "node:crypto";
import {
  closeSync, constants, fstatSync, linkSync, lstatSync, mkdirSync,
  openSync, readSync, unlinkSync, writeFileSync,
} from "node:fs";
import path from "node:path";
import { isBrokerId, MAX_BROKER_ID_CHARS } from "./config.js";

/** Explicit IDs override disk state; malformed disk state fails closed, never rotates silently. */
export function loadOrCreateBrokerId(stateDir: string, explicitId?: string): string {
  if (explicitId !== undefined) {
    if (!isBrokerId(explicitId)) throw new Error("broker_identity_invalid: invalid explicit brokerId");
    return explicitId;
  }
  mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  const target = path.join(stateDir, "broker-id");
  try {
    return readBrokerId(target);
  } catch (error) {
    if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
  }
  const candidate = randomUUID();
  const temporary = path.join(stateDir, `.broker-id-${candidate}.tmp`);
  const created = openSync(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL, 0o600);
  try {
    try {
      writeFileSync(created, `${candidate}\n`, "utf8");
    } finally {
      closeSync(created);
    }
    // Publish only a complete, closed file; a competing initializer cannot be replaced.
    try {
      linkSync(temporary, target);
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "EEXIST") throw error;
      return readBrokerId(target);
    }
    return candidate;
  } finally {
    unlinkSync(temporary);
  }
}

function readBrokerId(target: string): string {
  // lstat also rejects symlinks on Windows, where O_NOFOLLOW is unavailable.
  const expected = lstatSync(target);
  if (!expected.isFile()) throw new Error("broker_identity_invalid: malformed broker-id file");
  // O_NOFOLLOW and the fd identity check reject path replacement between lstat/open.
  const fd = openSync(target, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.dev !== expected.dev || stat.ino !== expected.ino
      || stat.size === 0 || stat.size > MAX_BROKER_ID_CHARS + 1) {
      throw new Error("broker_identity_invalid: malformed broker-id file");
    }
    const bytes = Buffer.alloc(MAX_BROKER_ID_CHARS + 2);
    const length = readSync(fd, bytes, 0, bytes.length, 0);
    const raw = bytes.toString("utf8", 0, length);
    const id = raw.endsWith("\n") ? raw.slice(0, -1) : raw;
    if (length > MAX_BROKER_ID_CHARS + 1 || !isBrokerId(id)) {
      throw new Error("broker_identity_invalid: malformed broker-id file");
    }
    return id;
  } finally {
    closeSync(fd);
  }
}
