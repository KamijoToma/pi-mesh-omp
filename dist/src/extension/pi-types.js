/** Helper: a single-paragraph text tool result. */
export function textResult(text, details) {
    const out = { content: [{ type: "text", text }] };
    if (details !== undefined)
        out.details = details;
    return out;
}
