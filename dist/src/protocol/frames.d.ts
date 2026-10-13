export declare class FrameSizeError extends Error {
    readonly name = "FrameSizeError";
    constructor(message: string);
}
export declare function sha256(data: string): string;
export declare function nowIso(): string;
/** m_<time36>_<rand8hex>. */
export declare function makeMsgId(rand?: (size: number) => Buffer): string;
/** Encode one frame as a NDJSON line. Throws FrameSizeError over maxBytes. */
export declare function encodeFrame(value: unknown, maxBytes: number): Buffer;
/**
 * Incremental NDJSON decoder with chunk reassembly.
 * Throws FrameSizeError when a single line (or the partial buffer) exceeds maxBytes.
 */
export declare class FrameDecoder {
    private readonly maxBytes;
    private buf;
    private bufBytes;
    constructor(maxBytes: number);
    /** Feed a chunk; returns complete lines (without trailing \n). */
    push(chunk: Buffer | string): string[];
}
