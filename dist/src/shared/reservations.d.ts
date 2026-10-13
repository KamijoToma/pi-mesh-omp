import type { FileReservation } from "../protocol/envelope.js";
export interface ReservationConflict {
    path: string;
    alias: string;
    reservation: FileReservation;
}
/** Normalize a path/pattern for comparison: forward slashes, no "./", and
 *  case-folded on Windows (NTFS is case-insensitive). */
export declare function normalizePath(p: string): string;
/** True when `filePath` falls under reservation `pattern`. */
export declare function pathMatchesReservation(filePath: string, pattern: string): boolean;
/**
 * a reservation older than ttlMs is expired and does not block anyone.
 * ttlMs 0 = unlimited default).
 */
export declare function isReservationExpired(reservation: FileReservation, ttlMs: number, now?: number): boolean;
/** First conflicting reservation for `filePath`, or undefined. */
export declare function findConflict(filePath: string, reservationsByPeer: ReadonlyMap<string, readonly FileReservation[]>, selfAlias: string, ttlMs?: number): ReservationConflict | undefined;
/** Check every mutation target exposed by a host's tool event. */
export declare function findToolConflict(input: Readonly<Record<string, unknown>>, reservationsByPeer: ReadonlyMap<string, readonly FileReservation[]>, selfAlias: string, ttlMs?: number): ReservationConflict | undefined;
