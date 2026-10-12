/**
 * Loading and starting the WASM engine.
 *
 * The module is compiled once per process and instantiated per database, so opening many
 * embedded databases costs one compile. Each instance has its own linear memory and its own Go
 * runtime: embedded databases are isolated from one another the way separate processes are.
 */

import { LoopbackTransport, type EngineHandle } from "./loopback.ts";

export type WasmSource = WebAssembly.Module | BufferSource | URL | string | Response | Promise<Response>;

interface GoGlue {
  importObject: WebAssembly.Imports;
  run(instance: WebAssembly.Instance): Promise<void>;
}
type GoCtor = new () => GoGlue;

interface EmbedApi {
  open(namespaces: string[], onFrame: (frame: Uint8Array) => void): EngineHandle | { error: string };
}

const isNode = typeof process !== "undefined" && process.versions?.node !== undefined;

const defaultWasmUrl = (): URL => new URL("../wasm/kdb.wasm", import.meta.url);
const defaultGlueUrl = (): URL => new URL("../wasm/wasm_exec.js", import.meta.url);

let glueLoaded: Promise<void> | undefined;
/** Go's wasm_exec.js is a classic script that defines `globalThis.Go`; importing it runs it. */
function loadGlue(glue?: string | URL): Promise<void> {
  if ((globalThis as { Go?: unknown }).Go !== undefined) return Promise.resolve();
  glueLoaded ??= import(/* @vite-ignore */ String(glue ?? defaultGlueUrl())).then(() => {
    if ((globalThis as { Go?: unknown }).Go === undefined) throw new Error("kdb embed: wasm_exec.js loaded but did not define Go");
  });
  return glueLoaded;
}

async function bytesOf(url: URL): Promise<Uint8Array> {
  if (isNode && url.protocol === "file:") {
    const { readFile } = await import("node:fs/promises");
    return new Uint8Array(await readFile(url));
  }
  const response = await fetch(url);
  if (!response.ok) throw new Error(`kdb embed: fetching ${url} failed: HTTP ${response.status}`);
  return new Uint8Array(await response.arrayBuffer());
}

const compiled = new WeakMap<object, Promise<WebAssembly.Module>>();
const compiledByUrl = new Map<string, Promise<WebAssembly.Module>>();

export function compileEngine(source: WasmSource = defaultWasmUrl()): Promise<WebAssembly.Module> {
  if (source instanceof WebAssembly.Module) return Promise.resolve(source);
  if (typeof source === "string" || source instanceof URL) {
    const url = typeof source === "string" ? new URL(source, isNode ? undefined : globalThis.location?.href) : source;
    const key = url.href;
    let pending = compiledByUrl.get(key);
    if (pending === undefined) {
      pending = !isNode && typeof WebAssembly.compileStreaming === "function"
        ? WebAssembly.compileStreaming(fetch(url))
        : bytesOf(url).then((b) => WebAssembly.compile(b as BufferSource));
      compiledByUrl.set(key, pending);
      pending.catch(() => compiledByUrl.delete(key));
    }
    return pending;
  }
  if (source instanceof Response || source instanceof Promise) {
    return WebAssembly.compileStreaming(source as Response | Promise<Response>);
  }
  const cached = compiled.get(source as object);
  if (cached !== undefined) return cached;
  const pending = WebAssembly.compile(source as BufferSource);
  compiled.set(source as object, pending);
  return pending;
}

export interface StartedEngine {
  handle: EngineHandle;
  transport: LoopbackTransport;
}

export async function startEngine(
  namespaces: string[],
  module: WebAssembly.Module,
  glue?: string | URL,
): Promise<StartedEngine> {
  await loadGlue(glue);
  const Go = (globalThis as unknown as { Go: GoCtor }).Go;
  const go = new Go();
  const instance = await WebAssembly.instantiate(module, go.importObject);
  const takeApi = (): EmbedApi | undefined => {
    const g = globalThis as { KdbEmbed?: EmbedApi };
    const api = g.KdbEmbed;
    delete g.KdbEmbed;
    return api;
  };

  // Each Go runtime publishes its API on the global and we cannot tell instances apart there, so
  // take it immediately and remove it before anyone else can start - run() executes Go's main
  // synchronously up to its first block, which is where the API is registered.
  takeApi();
  const exited = go.run(instance);
  const api = takeApi();
  if (api === undefined) throw new Error("kdb embed: the WASM module did not register KdbEmbed");

  const transport = new LoopbackTransport();
  const handle = api.open(namespaces, (frame) => transport.deliver(frame));
  if ("error" in handle) throw new Error(`kdb embed: ${handle.error}`);
  transport.attach(handle);
  exited.then(
    () => transport.fail(new Error("kdb embed: the engine exited")),
    (cause: unknown) => transport.fail(cause instanceof Error ? cause : new Error(String(cause))),
  );
  return { handle, transport };
}
