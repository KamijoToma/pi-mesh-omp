// The host loads this boundary once; /mesh reload replaces the implementation graph.
import { truncateToWidth, visibleWidth, wrapTextWithAnsi } from "@mariozechner/pi-tui";
import createMeshExtension from "./implementation.js";
import { installReloadableMesh } from "./reload.js";
import type { ExtensionAPI } from "./pi-types.js";

export default function meshExtension(pi: ExtensionAPI): void {
  installReloadableMesh(pi, createMeshExtension, import.meta.url, { truncateToWidth, visibleWidth, wrapTextWithAnsi });
}
