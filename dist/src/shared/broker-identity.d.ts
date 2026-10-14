/** Explicit IDs override disk state; malformed disk state fails closed, never rotates silently. */
export declare function loadOrCreateBrokerId(stateDir: string, explicitId?: string): string;
