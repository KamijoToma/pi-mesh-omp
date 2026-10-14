// extension/reload-source.ts — owned built-code snapshots, never mesh state.
import { createHash } from "node:crypto";
import type { Hash } from "node:crypto";
import * as fs from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { stateDir } from "../shared/paths.js";
import type { MeshExtensionFactory } from "./reload.js";

export interface ReloadSource {
  revision: string;
  factory: MeshExtensionFactory;
  dispose(): void;
}

const IMPLEMENTATION = "extension/implementation.js";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function packageRoot(entryUrl: string): string {
  let entryPath: string;
  try {
    entryPath = fileURLToPath(entryUrl);
  } catch (error) {
    throw new Error(`Mesh reload requires a file URL for the installed extension: ${entryUrl}`, { cause: error });
  }
  let directory = path.dirname(entryPath);
  for (;;) {
    const manifest = path.join(directory, "package.json");
    try {
      const metadata: unknown = JSON.parse(fs.readFileSync(manifest, "utf8"));
      if (metadata !== null && typeof metadata === "object" &&
        "name" in metadata && metadata.name === "pi-mesh-extension") return directory;
    } catch (error) {
      if ((error as NodeJS.ErrnoException | undefined)?.code !== "ENOENT") {
        throw new Error(`Mesh reload cannot read package metadata at ${manifest}: ${errorMessage(error)}`, { cause: error });
      }
    }
    const parent = path.dirname(directory);
    if (parent === directory) {
      throw new Error(`Mesh reload cannot locate the pi-mesh-extension package above ${entryPath}; use an installed package with package.json and a built dist/src tree.`);
    }
    directory = parent;
  }
}

/** Only runtime JS is copied; declarations, tests and message state stay out. */
function runtimeFiles(root: string): string[] {
  const builtDir = path.join(root, "dist", "src");
  const files: string[] = [];
  const visit = (directory: string, prefix: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const relative = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
      if (entry.isDirectory()) {
        visit(path.join(directory, entry.name), relative);
      } else if (entry.isFile() && entry.name.endsWith(".js")) {
        files.push(relative);
      } else if (entry.isSymbolicLink()) {
        throw new Error(`built tree contains a symbolic link at ${relative}; rebuild with regular compiled files so every mesh module can be snapshotted`);
      }
    }
  };
  try {
    visit(builtDir, "");
    files.sort();
    if (!files.includes(IMPLEMENTATION)) throw new Error(`missing ${IMPLEMENTATION}`);
    return files;
  } catch (error) {
    throw new Error(
      `Mesh reload cannot use the built JavaScript at ${builtDir}: ${errorMessage(error)}. Run \`npm run build\` in ${root}, let it finish, then retry /mesh reload; reload does not build or fetch code.`,
      { cause: error },
    );
  }
}

function addRevisionFile(hash: Hash, relative: string, contents: Buffer): void {
  // A NUL-terminated name and byte count make file boundaries unambiguous.
  // Names use '/' and deterministic code-unit sorting, never locale order.
  hash.update(relative).update("\0").update(String(contents.length)).update("\0").update(contents);
}

/** Read the same built bytes used for a reload, without creating a snapshot. */
export function readReloadRevision(entryUrl: string): string {
  const root = packageRoot(entryUrl);
  const files = runtimeFiles(root);
  const hash = createHash("sha256");
  try {
    for (const relative of files) {
      addRevisionFile(hash, relative, fs.readFileSync(path.join(root, "dist", "src", relative)));
    }
    return hash.digest("hex");
  } catch (error) {
    throw new Error(
      `Mesh reload cannot read the built JavaScript at ${path.join(root, "dist", "src")}: ${errorMessage(error)}. Run \`npm run build\` in ${root}, let it finish, then retry /mesh reload; reload does not build or fetch code.`,
      { cause: error },
    );
  }
}


export async function loadReloadSource(entryUrl: string): Promise<ReloadSource> {
  const root = packageRoot(entryUrl);
  const files = runtimeFiles(root);
  let snapshot: string | undefined;
  let disposed = false;
  const dispose = (): void => {
    if (disposed || snapshot === undefined) return;
    // Never sweep the cache parent or another process's generation.
    fs.rmSync(snapshot, { recursive: true, force: true });
    disposed = true;
  };
  try {
    const cacheDir = path.resolve(stateDir(), "reload-cache");
    fs.mkdirSync(cacheDir, { recursive: true, mode: 0o700 });
    snapshot = fs.mkdtempSync(path.join(cacheDir, `mesh-${process.pid}-`));
    fs.writeFileSync(path.join(snapshot, "package.json"), '{"type":"module"}\n', { mode: 0o600 });
    const hash = createHash("sha256");
    for (const relative of files) {
      // Hash exactly what is written, not a second read of a changing build.
      const contents = fs.readFileSync(path.join(root, "dist", "src", relative));
      addRevisionFile(hash, relative, contents);
      const target = path.join(snapshot, "src", relative);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, contents, { mode: 0o600 });
    }
    const revision = hash.digest("hex");

    // TS source can be loaded by Jiti, which rewrites ordinary import(). This
    // fixed importer uses the host's native Node/Bun module loader. The entry
    // passes public SDK functions explicitly, so no host aliases are required.
    // Unique paths refresh every local transitive module.
    const nativeImport = new Function("url", "return import(url)") as
      (url: string) => Promise<{ default?: unknown }>;
    const module = await nativeImport(pathToFileURL(path.join(snapshot, "src", IMPLEMENTATION)).href);
    if (typeof module.default !== "function") {
      throw new Error(`${IMPLEMENTATION} must export a default mesh extension factory; rebuild a version supporting /mesh reload`);
    }
    return { revision, factory: module.default as MeshExtensionFactory, dispose };
  } catch (error) {
    let cleanup = "";
    try {
      dispose();
    } catch (cleanupError) {
      cleanup = ` Snapshot cleanup failed at ${snapshot}: ${errorMessage(cleanupError)}; remove only that directory once unused.`;
    }
    throw new Error(
      `Mesh reload could not load ${path.join(root, "dist", "src", IMPLEMENTATION)}: ${errorMessage(error)}. Finish \`npm run build\` in ${root} and ensure the built version supports /mesh reload, then retry.${cleanup}`,
      { cause: error },
    );
  }
}
