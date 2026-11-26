/** SDK bindings belong to the unchanged host, not a snapshotted plugin graph. */
export interface MeshHostTui {
    truncateToWidth(text: string, width: number): string;
    visibleWidth(text: string): number;
    wrapTextWithAnsi(text: string, width: number): string[];
}
export declare let truncateToWidth: MeshHostTui["truncateToWidth"];
export declare let visibleWidth: MeshHostTui["visibleWidth"];
export declare let wrapTextWithAnsi: MeshHostTui["wrapTextWithAnsi"];
/** Each fresh implementation graph receives the host's public SDK functions. */
export declare function configureHostTui(host: MeshHostTui): void;
