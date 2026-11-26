// The host loads this boundary once; /mesh reload replaces the implementation graph.
import * as tui from "@oh-my-pi/pi-tui";
import createMeshExtension from "./implementation.js";
import { installReloadableMesh } from "./reload.js";
export default async function meshExtension(pi) {
    let host = tui;
    if (typeof host.truncateToWidth !== "function") {
        // omp 19 moved the text helpers to a subpath absent from the omp 18 SDK.
        const { truncate, visibleWidth, wrap } = await import("@oh-my-pi/pi-tui/text/width");
        host = { truncateToWidth: truncate, visibleWidth, wrapTextWithAnsi: wrap };
    }
    installReloadableMesh(pi, createMeshExtension, import.meta.url, host);
}
