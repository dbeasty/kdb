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
| Runs in a browser | Verified in Chromium (`test/browser/smoke.html`) |
| Typed layer | `@kdb/codegen`'s whole suite passes against it |
| **Durable** | **No.** The database is in memory and is gone when the page reloads or the process exits. IndexedDB/OPFS persistence is not part of this release. |
| Peer-sync / replication against a server | Not implemented. Embedded databases are local-only. |
| Cross-namespace transactions | Not available in memory (they need the durable host's decision log). |
| Size | `kdb.wasm` is ~31 MB raw (~7 MB gzipped). The server package links the AWS SDK, `net/http` and TLS even though the embedded engine uses none of it; shrinking that is future work. |

Each namespace is an independent store. Pass a list to serve several:
`openEmbedded({ namespace: ["a/one", "b/two"] })`. A namespace you did not list is refused
(`unknown namespace`) rather than silently aliased onto another.

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

## Building

```sh
npm run build   # needs Go: builds go/wasm/embed to wasm/kdb.wasm, copies wasm_exec.js, runs tsc
npm test        # the above, then the suite
```

`@kdb/client` (and `@kdb/codegen` for the tests) must be built first; they are `file:` dependencies.
