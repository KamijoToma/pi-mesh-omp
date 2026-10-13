// cli/args.ts — zero-dependency argument parser (V3: no commander/yargs).
// Supports: --flag, --opt value, --opt=value, -o value, `--` end-of-options,
// repeated value options, and auto --help/-h detection. Every parse error is
// a usage error (exit 2) — the caller prints usage + the reason.
/** Parse argv against specs. Never throws — errors are honest strings. */
export function parseArgs(argv, specs) {
    const byLong = new Map();
    const byShort = new Map();
    for (const s of specs) {
        byLong.set(s.name, s);
        if (s.short !== undefined)
            byShort.set(s.short, s);
    }
    const flags = new Set();
    const values = new Map();
    const repeats = new Map();
    const positionals = [];
    let help = false;
    let noMoreOptions = false;
    for (let i = 0; i < argv.length; i += 1) {
        const a = argv[i] ?? "";
        if (noMoreOptions || a === "-" || !a.startsWith("-")) {
            positionals.push(a);
            continue;
        }
        if (a === "--") {
            noMoreOptions = true;
            continue;
        }
        // --help / -h : recorded, parsing continues (later args still validated)
        if (a === "--help" || a === "-h") {
            help = true;
            continue;
        }
        let name;
        let inlineValue;
        if (a.startsWith("--")) {
            const body = a.slice(2);
            const eq = body.indexOf("=");
            if (eq !== -1) {
                name = body.slice(0, eq);
                inlineValue = body.slice(eq + 1);
            }
            else {
                name = body;
            }
        }
        else {
            name = a.slice(1);
        }
        const spec = byLong.get(name) ?? byShort.get(name);
        if (spec === undefined) {
            return { ok: false, error: `unknown option: ${a}` };
        }
        if (spec.kind === "flag") {
            if (inlineValue !== undefined) {
                return { ok: false, error: `option --${spec.name} takes no value` };
            }
            flags.add(spec.name);
            continue;
        }
        // value / repeat: inline (--opt=v) or the next argv token
        let value = inlineValue;
        if (value === undefined) {
            const next = argv[i + 1];
            if (next === undefined) {
                return { ok: false, error: `option --${spec.name} requires a value` };
            }
            value = next;
            i += 1;
        }
        values.set(spec.name, value);
        const list = repeats.get(spec.name) ?? [];
        list.push(value);
        repeats.set(spec.name, list);
    }
    return { ok: true, parsed: { flags, values, repeats, positionals }, help };
}
/** Render the usage fragment for a spec list: [--room R] [--await] ... */
export function specUsage(specs) {
    return specs
        .map((s) => {
        const flag = s.short !== undefined ? `-${s.short}, --${s.name}` : `--${s.name}`;
        if (s.kind === "flag")
            return `[${flag}]`;
        return `[${flag} ${s.meta ?? s.name.toUpperCase()}]`;
    })
        .join(" ");
}
