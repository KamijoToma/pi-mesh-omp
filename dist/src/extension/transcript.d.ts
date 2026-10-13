import type { MeshFrame } from "../protocol/envelope.js";
export declare const TRANSCRIPT_MAX_BODY_BYTES: number;
export declare const TRUNCATED_MARKER = "\u2026[truncated]";
export interface SecretPattern {
    name: string;
    re: RegExp;
    replacement: string;
}
/** Redaction profile. */
export declare const SECRET_PATTERNS: SecretPattern[];
/** Apply every secret pattern. */
export declare function redactSecrets(text: string): string;
/** Truncate to ≤ maxBytes UTF-8 bytes WITHOUT splitting a codepoint. */
export declare function truncateUtf8(text: string, maxBytes?: number): string;
export interface TranscriptEntry {
    ts: string;
    dir: "in" | "out";
    frame: MeshFrame;
}
export declare class MeshTranscript {
    private readonly stateDir;
    private readonly retentionDays;
    private enabled;
    constructor(stateDir: string, enabled: boolean, retentionDays?: number);
    isEnabled(): boolean;
    /** /mesh log on|off. Enabling triggers a retention pass. */
    setEnabled(on: boolean): void;
    private transcriptsDir;
    private filePath;
    /** Record one frame; body is redacted + truncated. No-op when disabled. */
    record(dir: "in" | "out", frame: MeshFrame): void;
    /** Delete transcript day-files older than retentionDays. Returns count removed. */
    pruneRetention(now?: number): number;
}
