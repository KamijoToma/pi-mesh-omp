import type { ExtensionAPI } from "./pi-types.js";
import type { MeshExtensionInstance } from "./reload.js";
import { type MeshHostTui } from "./tui.js";
export default function createMeshExtension(pi: ExtensionAPI, host: MeshHostTui): MeshExtensionInstance;
