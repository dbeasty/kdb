/**
 * @kdb/embed - the KDB engine, running in-process.
 *
 * `openEmbedded` starts the same Go engine the server runs, compiled to WebAssembly, and returns
 * a `Client` over a loopback transport. There is no connection and no server: the data is local.
 * Because it is a `Client`, it is a `KdbOperations`, so code written against that interface -
 * including everything @kdb/codegen generates - runs unchanged against a server or against this.
 *
 * Durability: the database is **in memory**. It lives as long as the returned client does and is
 * gone when the page reloads or the process exits. Persistence (IndexedDB/OPFS) is not part of
 * this release; saying otherwise would be dishonest.
 */

import { Client, type ConnectOptions } from "@kdb/client";
import { compileEngine, startEngine, type WasmSource } from "./engine.ts";

export type { WasmSource } from "./engine.ts";

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
  const { transport } = await startEngine(namespaces, module, options.glue);

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
