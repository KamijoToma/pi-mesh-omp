// The host loads this boundary once; /mesh reload replaces the implementation graph.
import * as tui from "@oh-my-pi/pi-tui";
import createMeshExtension from "./implementation.js";
import { installReloadableMesh } from "./reload.js";
import type { ExtensionAPI } from "./pi-types.js";
import type { MeshHostTui } from "./tui.js";

export default async function meshExtension(pi: ExtensionAPI): Promise<void> {
  let host: MeshHostTui = tui;
  if (typeof host.truncateToWidth !== "function") {
    // omp 19 moved the text helpers to a subpath absent from the omp 18 SDK.
    const { truncate, visibleWidth, wrap }: {
      truncate: MeshHostTui["truncateToWidth"];
      visibleWidth: MeshHostTui["visibleWidth"];
      wrap: MeshHostTui["wrapTextWithAnsi"];
    } = await import("@oh-my-pi/pi-tui/text/width" as string);
    host = { truncateToWidth: truncate, visibleWidth, wrapTextWithAnsi: wrap };
  }
  installReloadableMesh(pi, createMeshExtension, import.meta.url, host);
}
