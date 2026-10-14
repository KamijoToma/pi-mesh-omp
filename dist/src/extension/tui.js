export let truncateToWidth;
export let visibleWidth;
export let wrapTextWithAnsi;
/** Each fresh implementation graph receives the host's public SDK functions. */
export function configureHostTui(host) {
    truncateToWidth = host.truncateToWidth;
    visibleWidth = host.visibleWidth;
    wrapTextWithAnsi = host.wrapTextWithAnsi;
}
