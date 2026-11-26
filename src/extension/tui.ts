/** SDK bindings belong to the unchanged host, not a snapshotted plugin graph. */
export interface MeshHostTui {
  truncateToWidth(text: string, width: number): string;
  visibleWidth(text: string): number;
  wrapTextWithAnsi(text: string, width: number): string[];
}

export let truncateToWidth: MeshHostTui["truncateToWidth"];
export let visibleWidth: MeshHostTui["visibleWidth"];
export let wrapTextWithAnsi: MeshHostTui["wrapTextWithAnsi"];

/** Each fresh implementation graph receives the host's public SDK functions. */
export function configureHostTui(host: MeshHostTui): void {
  truncateToWidth = host.truncateToWidth;
  visibleWidth = host.visibleWidth;
  wrapTextWithAnsi = host.wrapTextWithAnsi;
}
