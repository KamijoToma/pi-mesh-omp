// The host loads this boundary once; /mesh reload replaces the implementation graph.
import { truncateToWidth, visibleWidth, wrapTextWithAnsi } from "@mariozechner/pi-tui";
import createMeshExtension from "./implementation.js";
import { installReloadableMesh } from "./reload.js";
export default function meshExtension(pi) {
    installReloadableMesh(pi, createMeshExtension, import.meta.url, { truncateToWidth, visibleWidth, wrapTextWithAnsi });
}
