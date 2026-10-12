# `@kdb/embed`

The KDB engine running in-process — in the browser, Node, or anywhere WebAssembly runs. Implements
[Component 63](../../docs/kdb-spec-layer12-component63-ts-client-sdk.md) Phase 3 (§11).

It is **not a reimplementation**. It is the Go engine the server runs (SQL planner, transaction
engines, unique keys, document locks, error classification), compiled to WebAssembly and fronted by
the server's own `FrameHost` — the seam the TCP, WebSocket and gRPC listeners plug into. A loopback
transport carries the same wire frames a WebSocket would. So `openEmbedded` returns a `@kdb/client`
`Client`, and anything written against `KdbOperations` — including everything `@kdb/codegen`
generates — runs unchanged against a server or against this.

```ts
import { openEmbedded } from "@kdb/embed";

const db = await openEmbedded({ namespace: "app/users" });

await db.putJSON("app/users", "6f9619ff-8b86-d011-b42d-00cf4fc964ff", { name: "ada" });
await db.exec("app/users", "CREATE TABLE users (name TEXT NOT NULL)");
const rows = await db.query("app/users", "SELECT name FROM users");

await db.close();
```

## What is true, and what is not

| | |
|---|---|
| Documents, conditional writes, `compareAndSwap`, multi-document `commit`, SQL, leases | Same engine as the server |
| Behaves like the Go server | Checked by a differential test: a 23-step script run against both must match, including error classes and messages |
| Runs in a browser | Verified in Chromium: `test/browser/smoke.html`, and `persist.html` on real IndexedDB across real reloads |
| Typed layer | `@kdb/codegen`'s whole suite passes against it |
| **Durable** | **Opt-in** with `persist` (below). Without it the database is in memory and is gone when the page reloads or the process exits. |
| Peer-sync / replication against a server | Not implemented. Embedded databases are local-only. |
| Cross-namespace transactions | Not available in memory (they need the durable host's decision log). |
| Size | `kdb.wasm` is ~31 MB raw, ~6.7 MB gzipped, **~4.7 MB brotli** (`wasm/kdb.wasm.br`, serve with `Content-Encoding: br`). It is the whole Go engine; it does not shrink much by dropping dependencies (see below). |

Each namespace is an independent store. Pass a list to serve several:
`openEmbedded({ namespace: ["a/one", "b/two"] })`. A namespace you did not list is refused
(`unknown namespace`) rather than silently aliased onto another.

## Persistence

```ts
const db = await openEmbedded({ namespace: "app/users", persist: { name: "my-app" } });
```

With `persist`, the engine runs its real file-backed storage (delta log, checkpoints, cross-namespace
transaction log) on a filesystem that is kept in IndexedDB. **`fsync` is the durability boundary:**
the engine acknowledges a commit only after fsync, and fsync returns only after the commit is in the
backend as one atomic IndexedDB transaction. So an acknowledged write survives a reload and an unclean
tab close, and a crash can lose only writes that were never acknowledged — never leave a torn state.

- **Verified:** reload round trips; crash recovery from a snapshot of the backend taken while the
  database was still running (after 8, 16 and 24 acknowledged commits, every one recovered, and a
  snapshot taken in the middle of 40 concurrent writes recovered intact documents only); a storage
  failure (quota) fails the commit instead of acknowledging it; and, in a real browser, three
  consecutive reloads without ever closing the database.
- **Cost:** a commit costs one IndexedDB transaction, a few ms (measured 4–9 ms with `durability:
  "strict"`). Pass `persist: { name, durability: "relaxed" }` to trade power-loss safety for speed.
  Files are stored in 64 KiB chunks and only changed chunks are written.
- **Memory:** the stored files are loaded into memory on open and the engine keeps its own working
  set too, so the cost is roughly the data twice.
- **One at a time:** a persistent name can be open once. A second open — in this tab or, via Web
  Locks, another tab — is refused with an error rather than allowed to corrupt it. The engine's lock
  file is never persisted, so a tab that died holding it cannot lock the database out.
- **SQL schemas are not restored on reopen.** A `CREATE TABLE` schema (constraints, required columns)
  is not persisted by the engine — the Go service behaves the same — so re-apply it at start. With
  `@kdb/codegen`, `await db.users.ensure()` does that idempotently. Documents and rows are persisted.
- **Backends:** IndexedDB by default. Pass `persist.backend` (any `FsBackend` with an atomic `apply`) for
  anything else — OPFS, a server, a file. `MemoryBackend` is for tests.
- Not part of this: syncing a persistent database with a server, and migrating the storage format.

## Loading

The package ships `wasm/kdb.wasm` and Go's matching `wasm/wasm_exec.js` glue. By default both are
found next to the module (`fetch` in a browser, `fs` in Node). To control it — a CDN, a bundler, or
opening many databases cheaply — compile once and pass the module:

```ts
const wasm = await WebAssembly.compileStreaming(fetch("/assets/kdb.wasm"));
const a = await openEmbedded({ namespace: "a/x", wasm, glue: "/assets/wasm_exec.js" });
const b = await openEmbedded({ namespace: "b/y", wasm, glue: "/assets/wasm_exec.js" });
```

Each database has its own linear memory and Go runtime, so they are isolated from one another the way
separate processes are. Opening one takes ~100 ms once the module is compiled.

### Why it is this big

The persistent engine links the AWS S3 client for replication. That alone was ~21 MB, so the build uses
a `kdb_lite` Go tag that swaps it for a stub (52.9 MB down to 31.7 MB). Measured, the rest of the
server's network code (`net`, TLS, x509) is a small fraction. What remains is the engine itself — the Go
runtime, SQL, the transaction and storage layers, the goja JavaScript interpreter behind stored
procedures, and `x/text` collation tables. Brotli is the effective lever from here; a real reduction
would mean splitting the engine into optional pieces, which is a design change rather than a build flag.

## Building

```sh
npm run build   # needs Go: builds go/wasm/embed to wasm/kdb.wasm, copies wasm_exec.js, runs tsc
npm test        # the above, then the suite
```

The build uses the `kdb_lite` Go tag, which swaps the AWS S3 client for a stub; the persistent
engine links that client and it alone was ~21 MB of the binary, for a network path a browser never
takes. `@kdb/client` (and `@kdb/codegen` for the tests) must be built first; they are `file:` dependencies.
