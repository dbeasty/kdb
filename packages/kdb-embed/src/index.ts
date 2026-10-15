/**
 * @limidus/kdb-embed - the KDB engine, running in-process.
 *
 * `openEmbedded` starts the same Go engine the server runs, compiled to WebAssembly, and returns
 * a `Client` over a loopback transport. There is no connection and no server: the data is local.
 * Because it is a `Client`, it is a `KdbOperations`, so code written against that interface -
 * including everything @limidus/kdb-codegen generates - runs unchanged against a server or against this.
 *
 * Durability: the database is **in memory**. It lives as long as the returned client does and is
 * gone when the page reloads or the process exits. Persistence (IndexedDB/OPFS) is not part of
 * this release; saying otherwise would be dishonest.
 */

import { Client, type ConnectOptions } from "@limidus/kdb-client";
import { compileEngine, startEngine, type WasmSource } from "./engine.ts";
import { IndexedDbBackend, PersistentFs, type FsBackend } from "./fs.ts";

export type { WasmSource } from "./engine.ts";
export { IndexedDbBackend, MemoryBackend, PersistentFs } from "./fs.ts";
export type { FsBackend } from "./fs.ts";

export interface PersistOptions {
  /** Names the database. Opening the same name again later finds the same data. */
  name: string;
  /**
   * Where bytes are kept. Defaults to IndexedDB, which needs `indexedDB` (browsers, and runtimes that
   * provide it); anywhere else pass a backend. Any `FsBackend` works as long as `apply` is atomic.
   */
  backend?: FsBackend;
  /** IndexedDB only: "strict" (default) flushes to disk before a commit is acknowledged; "relaxed" is faster but a power cut may lose the last commits. */
  durability?: "strict" | "relaxed";
}

export interface EmbedOptions extends Pick<ConnectOptions, "requestTimeoutMs"> {
  /**
   * The namespace(s) this database serves, e.g. "app/users". Each is an independent store; a
   * request naming any other namespace is refused. Cross-namespace transactions are not
   * available in memory (they need the durable host's decision log).
   */
  namespace: string | string[];
  /**
   * Where the engine comes from: a compiled WebAssembly.Module (best when opening many databases),
   * bytes, a URL, or a fetch Response. Defaults to the `kdb.wasm` shipped next to this package.
   */
  wasm?: WasmSource;
  /** URL of Go's wasm_exec.js glue. Defaults to the copy shipped next to this package. */
  glue?: string | URL;
  /**
   * Keep the database across reloads. Without it the database is in memory and vanishes with the
   * client. With it, the engine runs its real file-backed storage on a filesystem that is persisted
   * to the backend, and a commit is acknowledged only after it is durably stored there.
   *
   * One persistent database can be open once at a time (across tabs too, where Web Locks exist);
   * a second open of the same name is refused rather than allowed to corrupt it.
   */
  persist?: PersistOptions;
}

const DATA_ROOT = "/kdb";

/** Names open in this realm. Web Locks (below) covers other tabs; this covers runtimes without them. */
const openNames = new Set<string>();

interface LockManagerLike {
  request(name: string, options: { mode: "exclusive"; ifAvailable: true }, callback: (lock: unknown) => Promise<void>): Promise<void>;
}

/** Takes a cross-tab exclusive lock on a database name, or returns undefined if another holder has it. */
async function acquireName(name: string): Promise<(() => void) | undefined> {
  if (openNames.has(name)) return undefined;
  const locks = (globalThis as { navigator?: { locks?: LockManagerLike } }).navigator?.locks;
  let release: () => void = () => undefined;
  if (locks !== undefined) {
    const held = await new Promise<boolean>((resolve) => {
      void locks.request(`kdb-embed:${name}`, { mode: "exclusive", ifAvailable: true }, (lock) => {
        if (lock === null) {
          resolve(false);
          return Promise.resolve();
        }
        resolve(true);
        // Held until release() - the lock lives exactly as long as the database is open.
        return new Promise<void>((done) => (release = done));
      });
    });
    if (!held) return undefined;
  }
  openNames.add(name);
  return () => {
    openNames.delete(name);
    release();
  };
}

/** An embedded database. Everything on `Client` that makes sense locally works the same way. */
export type EmbeddedKdb = Client;

export async function openEmbedded(options: EmbedOptions): Promise<EmbeddedKdb> {
  const namespaces = Array.isArray(options.namespace) ? options.namespace : [options.namespace];
  if (namespaces.length === 0 || namespaces.some((n) => typeof n !== "string" || n === "")) {
    throw new TypeError("kdb embed: options.namespace is required (a non-empty string, or a non-empty list of them)");
  }
  if (new Set(namespaces).size !== namespaces.length) {
    throw new TypeError("kdb embed: options.namespace lists the same namespace twice");
  }
  const module = await compileEngine(options.wasm);

  let storage: { fs: PersistentFs; dataRoot: string } | undefined;
  let release: (() => void) | undefined;
  let backend: (FsBackend & { close?: () => void }) | undefined;
  if (options.persist !== undefined) {
    const { name } = options.persist;
    if (typeof name !== "string" || name === "") throw new TypeError("kdb embed: persist.name is required");
    release = await acquireName(name);
    if (release === undefined) {
      throw new Error(`kdb embed: the persistent database ${JSON.stringify(name)} is already open (in this tab or another); close it first`);
    }
    try {
      backend = options.persist.backend;
      if (backend === undefined) {
        if (typeof indexedDB === "undefined") {
          throw new Error("kdb embed: no indexedDB here - pass persist.backend");
        }
        backend = new IndexedDbBackend(name, options.persist.durability);
      }
      storage = { fs: await PersistentFs.open(backend), dataRoot: DATA_ROOT };
    } catch (error) {
      release();
      throw error;
    }
  }

  let transport: Awaited<ReturnType<typeof startEngine>>["transport"];
  try {
    ({ transport } = await startEngine(namespaces, module, options.glue, storage));
  } catch (error) {
    release?.();
    throw error;
  }
  if (storage !== undefined) {
    const { fs } = storage;
    // Fires after the engine has drained, flushed and sealed: only then is the data final and the name free.
    const original = transport.close.bind(transport);
    transport.close = async () => {
      await original();
      try {
        await fs.close();
      } finally {
        backend?.close?.();
        release?.();
      }
    };
  }

  const connect: ConnectOptions = { namespaces };
  if (options.requestTimeoutMs !== undefined) connect.requestTimeoutMs = options.requestTimeoutMs;
  const client = new Client(transport, connect);
  try {
    await client.handshake(connect);
  } catch (error) {
    await transport.close();
    throw error;
  }
  return client;
}
