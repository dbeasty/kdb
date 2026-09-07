# Component 63 — TypeScript Client SDK (`@kdb/client`)

Layer 12, alongside Component 40 (Go Client SDK), Component 41 (auth tokens) and Component 42
(native transport). This is the JavaScript/TypeScript counterpart to Component 40: the same
operations, over the same wire, with the same server on the other end.

**On the number**: component ids are allocated sequentially across the whole spec, not per layer —
Layer 12's original batch took 38–46, Layer 13 took 47–52, Layer 14 53–57, Layer 15 58–62. This is
a Layer 12 addition landing after those, so it takes the next free id, 63, rather than a Layer 12
number that is already spoken for (44 is the cross-write commit notification bridge, already
shipped). Component 40's own preamble records the same kind of renumber.

Depends on the wire contract defined by `go/kdb/wire` and `kdb-wire` (Kotlin) — **as a
specification, not as a dependency**: this component reimplements the codec in TypeScript rather
than compiling either existing one to JS (§13.1 explains why). Depends operationally on
Component 38's Go-native server for TCP, and on that server growing a **WebSocket listener**
before any browser can connect at all (§2.3 — this is the one hard prerequisite).

## 1. Purpose

### 1.1 The question this component answers

"Should KDB ship a full-feature TypeScript client matching the Go one, or something
mongoose-shaped instead?" — a false choice, for two reasons.

**First, mongoose is not an alternative to a driver.** A MongoDB application uses `mongodb` (the
driver: connection, wire protocol, commands) *and*, optionally, `mongoose` on top of it (schemas,
models, hooks, populate). Mongoose calls the driver; it does not replace it. KDB needs the driver
regardless of whether the sugar layer ever gets built.

**Second, matching "Go functionality" is a smaller job than it sounds.** The Go *module* is the
whole engine — storage, indexes, SQL planner, server, peer-sync. The Go *client* is
`go/kdb/client`: four files and roughly fifteen public methods. Full parity with that is
achievable in a first release, not aspirational. The wire helps: `go/kdb/wire/codec.go` is a
12-byte little-endian header plus a one-byte encoding tag plus a **JSON envelope** — no CBOR, no
bespoke binary payload framing, no varint tag stream. A `DataView` and `JSON.parse` cover almost
all of it. (Almost: see §6.3, the one real exception.)

So the plan is three packages shipped in three phases, not one package chosen out of two designs.

### 1.2 The three packages

| Package | What it is | Phase | Analogy |
|---|---|---|---|
| `@kdb/client` | Network driver. Wire codec + transports + the Component 40 operation set, hand-written TS. | 1 | `mongodb` |
| `@kdb/codegen` | Types generated **from the KDB schema**, plus a typed SQL builder over them. | 2 | Prisma / Kysely |
| `@kdb/embed` | The engine itself, in-process in the browser or Node. Not a client. | 3 | PouchDB / `wa-sqlite` |

Phases 2 and 3 are specified here (§10, §11) rather than deferred to separate components, because
their existence changes decisions in Phase 1 — notably that `@kdb/client` must not invent its own
schema representation (§10.2) and must keep its operation interface separable from its transport
so `@kdb/embed` can implement the same interface locally (§11.3).

### 1.3 Why the ODM layer is deliberately *not* mongoose-shaped

Mongoose exists because MongoDB has no server-side schema, so the schema has to live in the
client. KDB is not in that position: `kdb-schema` is real, enforced server-side, and already has
a JSON serialization (`EmbedSchemaDto`, consumed today by `KdbBrowser.openWithSchema`).
Hand-declaring a second schema in TypeScript would reproduce mongoose's worst failure mode — a
client-side schema that silently drifts from the server's, with the drift only visible as a
runtime write rejection.

The stronger construction is to *derive* the TypeScript types from the KDB schema
(§10). That is Prisma's model, and it is strictly better than mongoose's here because the
authority already exists. Two further reasons the mongoose analogy misleads:

- KDB speaks **SQL**. The idiomatic typed layer over a SQL engine is a query builder
  (Kysely, Drizzle), not a document ODM.
- KDB has **commits, branches, `baseVersion`, merge**. Mongoose has no vocabulary for any of it,
  and these are the parts of KDB a client should be making *easy*, not hiding.

## 2. Dependencies and prerequisites

### 2.1 Specification inputs (read these before implementing)

- `docs/kdb-spec-layer7-component21-wire-protocol-framing.md` — frame format.
- `docs/kdb-spec-layer0-codec.md` — the canonical value codec, needed only in the narrow subset
  §6.3 describes.
- `go/kdb/wire/` — the normative reference implementation for this component. `types.go` (message
  type codes), `payload_dto.go` (envelope and every payload's JSON key names), `document_ops.go`,
  `lock_ops.go`, `transaction_codec.go`, `frame.go`, `codec.go`.
- `go/kdb/client/` — the operation semantics being matched, method for method.

### 2.2 Runtime targets

Node 20+, modern browsers, Bun, Deno, and Workers-style edge runtimes. The implementation
constraint that falls out of "edge runtimes" is: **no Node built-ins in the core package.** Use
`WebSocket`, `crypto.subtle`, `TextEncoder`/`TextDecoder`, `DataView` — all present in every
target. The TCP transport is necessarily Node-only and therefore lives behind a subpath export
(§13.2), not in the main entry point.

### 2.3 The WebSocket server prerequisite — **resolved**

Originally this section recorded a blocker: `go/kdb/transport/ws/transport.go` answered every
upgrade with `501 websocket upgrade not implemented`. The Go side had a WebSocket *client* and no
WebSocket *server*, so the intersection of "production deployment target" and "browser-reachable"
was empty — a browser could not connect to the Go service at all.

**That listener now exists** (finish-up plan item 4.G, Go WS server side):

- `ws.Transport` gained `ListenBound` / `Serve` / a real `Listen`, mirroring `tcp.Transport`'s
  shape, including its connection cap and its `setNoDelay`-through-TLS handling.
- `wsConnection` became mask-aware in both directions, which RFC 6455 §5.1 requires and browsers
  enforce: a client masks every frame, a server masks none, and the server rejects an unmasked
  client frame.
- `server.ListenSqlWireWS` / `ListenSqlWireWSTLS` serve the SQL wire protocol over it, using the
  same codec, handler and admitter as the TCP path, so a browser client and a native client get
  identical behaviour above the transport.
- `kdb-service` gained `--ws-addr` (also `KDB_WS_ADDR` / `wsAddr`), off by default, and its
  scheme is upgraded `ws://` → `wss://` alongside the others when TLS is configured.

Consequently §9.2's live-interop tests are no longer blocked, and `test/interop.test.ts` runs the
TypeScript client against a real Go server over a real WebSocket.

### 2.4 Which server implements what

Message codes `0x14`–`0x1C` (`documentGet`, `documentGetResult`, `upsert`, `upsertResult`,
`commitPushAck`, and the four lock verbs) are **Go-server-only**; `go/kdb/wire/lock_ops.go` says
so explicitly ("Go-only for now, like 0x14-0x18: no Kotlin counterpart exists yet"). A TS client
pointed at the JVM `kdb-server` therefore has `sessionBegin` / `sqlExec` / `txCommit` and nothing
else.

Consequence for the client's design: `get`, `upsert` and the lock verbs must fail with a clear,
specific error against a server that doesn't implement them, rather than hanging. This is not
hypothetical — finish-up plan item 4.H records that unrecognized wire messages currently return
*nil* rather than an error frame on some paths (`peersync/host.go:224-226`,
`wire_listen.go:142-144`), which hangs the caller. The client needs its own per-request timeout as
a backstop (§7.5), independent of that server-side fix.

## 3. Package layout

```
packages/kdb-client/           # @kdb/client — Phase 1, this component's core deliverable
  src/
    wire/frame.ts              # header encode/decode, length prefix, bounds checks
    wire/envelope.ts           # payloadEnvelope <-> typed messages
    wire/messages.ts           # message type codes + payload interfaces (generated? see §13.3)
    wire/bytes.ts              # the ByteArray-as-number-array convention (§6.2)
    wire/hash.ts               # canonical DocumentBody encode + SHA-256 (§6.3)
    transport/websocket.ts     # browser + Node + edge
    transport/tcp.ts           # Node only, subpath export
    client.ts                  # Client: connect, sessions, correlation, the operations
    errors.ts                  # the error taxonomy (§8)
  test/
    golden/                    # fixtures shared with go/kdb/interop (§9.1)
```

## 4. Public Interface

Parity with `go/kdb/client`, method for method, translated to idiomatic TypeScript: promises
rather than `(value, error)` tuples, `AbortSignal` rather than `context.Context`, thrown typed
errors rather than sentinel returns.

```ts
// @kdb/client

export interface ConnectOptions {
  /** "user:secret", matching wire.HandshakePayload.Token. Omit against auth.AllowAll. */
  token?: string;
  /** Handshake authorization-scoping hint; per-namespace sessions are still opened lazily. */
  namespaces?: string[];
  /** Default per-request deadline. See §7.5 — this is a correctness backstop, not a nicety. */
  requestTimeoutMs?: number;   // default 30_000
  signal?: AbortSignal;
}

/** addr accepts ws://, wss://, tcp://, tcps:// or a bare host:port (TCP, Node only). */
export function connect(addr: string, options?: ConnectOptions): Promise<Client>;

export interface Client {
  close(): Promise<void>;

  // --- documents ---------------------------------------------------------------------
  putJSON(ns: string, docId: string, body: unknown): Promise<CommitHash>;
  get(ns: string, docId: string): Promise<{ body: unknown; commit: CommitHash }>;
  getWithHash(ns: string, docId: string): Promise<{ body: unknown; contentHash: string }>;
  upsert(ns: string, docId: string, body: unknown): Promise<CommitHash>;
  appendEvent(ns: string, docId: string, body: unknown): Promise<void>;

  // --- conditional writes ------------------------------------------------------------
  putIfAbsent(ns: string, docId: string, body: unknown): Promise<CommitHash>;
  replaceIf(ns: string, docId: string, body: unknown, expectedContentHash: string): Promise<CommitHash>;
  replaceIfPresent(ns: string, docId: string, body: unknown): Promise<CommitHash>;
  compareAndSwap(ns: string, docId: string, mutate: (current: unknown | null) => unknown | null,
                 options?: { attempts?: number; signal?: AbortSignal }): Promise<CommitHash>;

  // --- transactional CAS write -------------------------------------------------------
  commit(tx: Transaction): Promise<CommitHash>;

  // --- SQL ---------------------------------------------------------------------------
  query<T = Row>(ns: string, sql: string, args?: unknown[]): Promise<T[]>;
  queryRaw(ns: string, sql: string, args?: unknown[]): Promise<{ columns: string[]; rows: string[][] }>;
  exec(ns: string, sql: string, args?: unknown[]): Promise<void>;

  // --- document leases ---------------------------------------------------------------
  acquireLock(ns: string, docId: string, ttlMs?: number): Promise<Lease>;
  renewLock(ns: string, docId: string, ttlMs?: number): Promise<Lease>;
  releaseLock(ns: string, docId: string): Promise<void>;
}

export type CommitHash = string;          // 64 hex chars
export type Row = Record<string, string>; // SQL rows arrive string-typed — see §7.6

export interface Transaction {
  baseVersion: CommitHash;
  namespace: string;
  writes: DocWrite[];
  preconditions?: Precondition[];
}

export interface DocWrite { docId: string; body: unknown; }

export type Precondition =
  | { opIndex: number; kind: "EXPECT_ANY" }
  | { opIndex: number; kind: "EXPECT_ABSENT" }
  | { opIndex: number; kind: "EXPECT_PRESENT" }
  | { opIndex: number; kind: "EXPECT_CONTENT_HASH"; contentHashHex: string };

// fence is a uint64 on the wire, so it arrives as a JSON number: `number`, not `string`, since
// a string would imply arbitrary precision the wire does not preserve. Values past 2^53 lose
// precision; fences are small monotonic counters and the client only compares them for equality
// across a renew.
export interface Lease { namespace: string; docId: string; fence: number; expiresAtMs: number; }
```

Two deliberate departures from the Go client:

1. **`body: unknown`, not `Uint8Array`.** Go passes `[]byte` because Go callers already hold
   marshaled JSON. TS callers hold objects, and a driver that made them call `JSON.stringify`
   themselves would be conspicuously worse than every other TS database client. The client
   serializes; `queryRaw` and a `bodyRaw` escape hatch exist for callers who genuinely have bytes.
2. **`compareAndSwap` takes a mutator, not a value.** Go's signature is shaped by its retry loop;
   in TS a callback is the natural expression of "read, transform, retry on precondition failure."

## 5. Wire mapping, message by message

The normative source is `go/kdb/wire/payload_dto.go`. Every JSON key below is copied from it —
the JSON key names *are* the contract, and they are the thing most likely to be got subtly wrong.

### 5.1 Frame layout

Little-endian throughout (`go/kdb/wire/frame.go`):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | `frameLength` (i32) — **total** frame size including this header |
| 4 | 2 | `typeCode` (u16) — see §5.2 |
| 6 | 2 | `protocolVersion` (i16) — currently `1` |
| 8 | 4 | `correlationID` (i32) |
| 12 | 1 | encoding tag byte (first payload byte) |
| 13 | n | JSON envelope (UTF-8) |

`FrameHeaderSize = 12`, `DefaultMaxFrameBytes = 16 * 1024 * 1024`, `KdbWireProtocolVersion = 1`.

Both length checks in `DecodeHeader` must be reproduced: `frameLength` against `maxFrameBytes`,
**and** `frameLength` against the buffer actually received. The second one is not defensive
padding — the comment at `frame.go:41-47` records that a WebSocket message arrives whole and
unvalidated, so a truncated or hostile prefix reaches the decoder directly. In Go that produced a
slice-bounds panic; in TS a `DataView` read past the end throws `RangeError`, which is just as
wrong to surface to a caller. Validate, then slice.

The envelope is `{"kind": "<name>", "<name>": { ...payload... }}` — the `kind` string and the
key holding the payload are the same name (`payloadEnvelope` in `payload_dto.go`).

### 5.2 Message table

| Code | `kind` | Direction | Used by | Payload keys |
|---|---|---|---|---|
| `0x01` | `handshake` | C→S | `connect` | `nodeId`, `namespaces`, `localHeads`, `supportsZstd`, `supportsIndexHints`, `supportsDirectDeltaIngest`, `maxFrameBytes`, `preferredEncodings`, `clientMode`, `protocolVersion`, `user?`, `password?`, `token?` |
| `0x01` | `handshakeAck` | S→C | `connect` | `accepted`, `negotiatedEncoding`, `protocolVersion`, `remoteHeads`, `rejectionReason?` |
| `0x0E` | `sessionBegin` | C→S | every op, lazily per namespace | `namespace`, `sessionId` (null on first), `readConsistency`, `baseVersionHex` |
| `0x13` | `sessionBeginAck` | S→C | ditto | `namespace`, `sessionId`, `headHex`, `readConsistency`, `error?` |
| `0x0F` | `sqlExec` | C→S | `query`, `queryRaw`, `exec` | `namespace`, `sessionId`, `sql`, `parametersJson` |
| `0x10` | `sqlResult` | S→C | ditto, **and `commit`** | `columns`, `rows`, `rowsAffected`, `resolvedCommitHex`, `readOnly`, `error?`, `generatedIds`, `errorCode?`, `retryAfterMs?` |
| `0x11` | `txCommit` | C→S | `putJSON`, `commit`, conditional writes, `exec` follow-up | `namespace`, `sessionId`, `transactionBytes` |
| `0x12` | `txRollback` | C→S | not used in v1 | `namespace`, `sessionId` |
| `0x07` | `conflictReport` | S→C | `commit` failure path | `namespace`, `reportBytes`, `errorCode?`, `retryAfterMs?` |
| `0x14` | `documentGet` | C→S | `get` | `namespace`, `docId` |
| `0x15` | `documentGetResult` | S→C | `get` | `namespace`, `docId`, `json?`, `commitHex`, `error?`, `errorCode?`, `retryAfterMs?` |
| `0x16` | `upsert` | C→S | `upsert` | `namespace`, `docId`, `json`, `sessionId` |
| `0x17` | `upsertResult` | S→C | `upsert` | `namespace`, `commitHex`, `error?`, `errorCode?`, `retryAfterMs?` |
| `0x19` | `lockAcquire` | C→S | `acquireLock` | `namespace`, `sessionId`, `docId`, `ttlMillis` |
| `0x1A` | `lockRenew` | C→S | `renewLock` | ditto |
| `0x1B` | `lockRelease` | C→S | `releaseLock` | `namespace`, `sessionId`, `docId` |
| `0x1C` | `lockResult` | S→C | all three | `namespace`, `sessionId`, `docId`, `granted`, `fence`, `expiresAtMillis`, `holderSessionId`, `error?`, `errorCode?` |

**Dispatch on `kind`, not on the type code.** `handshakeAck` is sent with `MessageType =
MsgHandshake = 0x01`, the same code as the request it answers (`go/kdb/server/wire_listen.go:240`
and `stream/coordinator.go:138` both construct it that way) — the two are told apart only by the
envelope's `kind` field. Every other request/reply pair in the table has a distinct code, which
makes this one an easy asymmetry to miss: a decoder switching on `typeCode` alone decodes an ack
as a request and fails at the first field. The type code is a routing hint; `kind` is the
discriminant.

Codes `0x02`–`0x0D` and `0x18` are peer-sync, stream and DAG messages. A **client** neither sends
nor receives them (§12), but the decoder must not choke on one: an unexpected inbound frame is
dropped with a warning, not treated as a protocol violation that tears down the connection.

`clientMode` for this component is always the string `"SQL_CLIENT"`. `readConsistency` is
`"READ_COMMITTED"`, matching `go/kdb/client/client.go:400`.

### 5.3 Operation flows

**`connect`** — dial, then one `handshake` with `nodeId: "kdb-client-ts"`,
`clientMode: "SQL_CLIENT"`, `token` if supplied. Reject unless `handshakeAck.accepted`; surface
`rejectionReason` in the thrown error.

**Lazy per-namespace sessions** — every operation that carries a `sessionId` first resolves one,
caching `{sessionId, head}` per namespace. `sessionBeginAck` with an empty `sessionId` means
rejected: throw `KdbUnauthenticatedError` carrying `error` if present. This mirrors
`ensureNamespace` exactly, including the cache, because the cached `head` is what subsequent
writes anchor their `baseVersion` on.

**`putJSON`** — build a `Transaction` with `operations: [{kind: "write", docId, patch}]`,
`baseVersion` = the cached head, a fresh random UUID `id`, `timestampMicros`, `authorNodeId`;
encode per §6.1; send `txCommit`. On `sqlResult`, advance the cached head to `resolvedCommitHex`
and return it. On `conflictReport`, decode and throw (§8.2).

**Conditional writes** — the same `txCommit`, with a `preconditions` entry at the operation's
index. `putIfAbsent` → `EXPECT_ABSENT`; `replaceIfPresent` → `EXPECT_PRESENT`; `replaceIf` →
`EXPECT_CONTENT_HASH` plus `contentHashHex`. `kind` travels as the **enum constant name**, not an
ordinal — kotlinx.serialization's convention, noted in `transaction_codec.go:29-31`.

**`upsert`** — a single `upsert` frame, no transaction, no `baseVersion`, and it cannot conflict.
`sessionId` is carried anyway, and this is load-bearing rather than incidental: it is how the
server distinguishes a lease holder's own upsert from a stranger's, and an empty value is treated
as "not the holder" (`document_ops.go:41-47`). A TS client must send it.

**`query` / `exec`** — `sqlExec` with `parametersJson` = `JSON.stringify(args)` when args are
present, omitted otherwise. If the reply's `readOnly` is false, `exec` must follow with a
`txCommit` carrying **no** `transactionBytes` to auto-commit the statement — one client-visible
unit of work, matching `query.go:44-72`. Forgetting this leaves writes uncommitted, which is the
kind of bug that passes every unit test and fails in production.

**`commit`** — encode the caller's `Transaction` per §6.1 and send `txCommit`. Reply is
`sqlResult` on success or `conflictReport` on failure; anything else is a protocol error.

## 6. Encoding hazards

These four are the places a from-scratch TS implementation will silently diverge. Each is listed
because the Go implementation has a comment recording that it *did* diverge before being fixed.

### 6.1 `transactionBytes` is JSON, not a binary blob

`txCommit.transactionBytes` is a byte array whose contents are UTF-8 JSON matching
`transactionDto` (`transaction_codec.go:14-25`), which in turn matches Kotlin's
`WireKdbTransactionDto` field for field:

```
{ "id", "baseVersionHex", "timestampMicros", "authorNodeId", "operations": [...], "preconditions"? }
```

Operations use `opDto`: `{"kind": "write" | "delete" | "fileWrite" | "schemaMigration", "docId"?,
"patch"?, "path"?, "blobHashHex"?, "migrationId"?, "migrationPayload"?}`. A document write is
`{"kind": "write", "docId": "<uuid>", "patch": "<the document JSON, as a string>"}` — note the
patch is a JSON **string** containing JSON, not a nested object.

`preconditions` is `omitempty` by design: a transaction with none must encode byte-for-byte as it
did before the field existed, so an older peer decodes it unchanged. A TS encoder that emits
`"preconditions": []` breaks that guarantee. **Omit the key entirely when empty.**

### 6.2 Kotlin `ByteArray` crosses the wire as an array of numbers

Every `jsonByteArray` field — `transactionBytes`, `reportBytes`, `commitsPayload`,
`snapshotBytes`, `schemaBytes`, `schemaDeltaBytes` — serializes as `[123, 34, 105, ...]`, not as
a base64 string. This is kotlinx.serialization's default for a plain `ByteArray`, and the JVM's
strict decoder throws `JsonDecodingException` on an unexpected string token and **tears down the
connection**.

The Go comment at `payload_dto.go:5-14` is worth reading in full before implementing this: Go's
own `encoding/json` defaults to base64 for `[]byte`, the bug was invisible to `go test ./...`
because nothing there exercises real cross-language JSON, and it was caught only by an interop
test against a live JVM server. TypeScript's `JSON.stringify` will do the same wrong thing to a
`Uint8Array` — it produces `{"0":123,"1":34,…}`, an object, which is a third distinct
encoding and equally wrong. Implement an explicit converter and use it on every one of those
fields.

### 6.3 Content hashes need the canonical codec, not JSON

This is the only part of the client that is not JSON, and the only part that is genuinely
intricate.

`getWithHash` and `replaceIf` compare a **content hash**, and `client.go:640-650` records the
design: the hash is computed *locally* from the returned body, never carried on the wire, because
it is defined as a pure function of `(documentId, json)`. That is a good design — no extra round
trip, no message to keep in sync — but it means a TS client that computes it differently produces
a hash the server will never match, and every `replaceIf` fails with a precondition error that
looks like a phantom concurrent write.

The definition (`go/kdb/document/kdb_document.go:107-114`): SHA-256 over
`codec.EncodeBytes` of the `DocumentBody` record — `RecordValue{1: UUID(docId), 2: String(json)}`
— under the wire type registry.

The good news is how narrow the required subset is: one record type, two fields, one UUID and one
string. This is not a port of Layer 0's codec; it is the encoder for exactly that shape, plus
`crypto.subtle.digest("SHA-256", …)`. **It must be driven by golden fixtures from
`go/kdb/interop/codec_golden_test.go` rather than written from the prose spec** (§9.1) — a
canonical encoder that is 99% right is 100% broken, and only differential testing finds the 1%.

Note the consequence for the API: `crypto.subtle.digest` is async, so `getWithHash`, `replaceIf`
and `compareAndSwap` are async for a reason beyond the network round trip. They stay async even
in `@kdb/embed`, where there is no round trip at all.

### 6.4 Document ids are UUIDs

`PutJSON` calls `codec.UUIDFromString(docID)` and fails on anything else
(`client.go:434-437`). Validate client-side and throw a clear error; do not pass an arbitrary
string through and let it fail as an opaque server error. `upsert`/`get` take the id as a plain
string on the wire, so this validation is the client's job, not the codec's.

## 7. Contracts

1. **One `Client` = one connection = one KDB session.** Hold it for the process lifetime; do not
   open one per request. Same guidance as the Go client.
2. **Correlation ids** are client-assigned, monotonic from 1, and match replies to in-flight
   requests via a pending-request map. Concurrent outstanding requests on one connection are
   required, not optional — a browser UI issues overlapping reads and writes as a matter of course.
3. **Namespace per call.** Every method takes `ns`; sessions are cached per namespace behind it.
4. **Cached head advancement.** After a successful `txCommit`, the namespace's cached head becomes
   `resolvedCommitHex`. This cache is what makes `putJSON` a single round trip; getting its
   invalidation wrong produces spurious conflicts.
5. **Every request has a deadline.** Default 30s, overridable per call via `AbortSignal`. This is
   a correctness backstop, not ergonomics: §2.4 documents server paths that currently return
   nothing at all for an unrecognized message. A driver that can hang forever on a server bug is
   not shippable.
6. **SQL rows are strings.** `sqlResult.rows` is `string[][]` on the wire. `query<T>` maps columns
   to object keys but does **not** invent types — the coercion belongs in `@kdb/codegen` (§10),
   where the schema is known. Phase 1 must not guess, because a driver that silently parses
   `"0123"` as the number `123` corrupts data.
7. **Reconnection is the caller's, in Phase 1.** `close()` and connection-loss errors are honest;
   automatic reconnect with request replay is deferred (§12) because replaying a `txCommit` whose
   reply was lost is not obviously safe, and the client cannot decide that for the caller.

## 8. Error cases

### 8.1 Taxonomy

All errors extend `KdbError` and carry `code?: ErrorCode` and `retryAfterMs?: number`, mapping the
additive `errorCode`/`retryAfterMs` fields present on `sqlResult`, `upsertResult`,
`documentGetResult` and `conflictReport`.

| Class | Raised when | Go counterpart |
|---|---|---|
| `KdbConflictError` | `conflictReport` with ordinary conflicts | `ConflictError` |
| `KdbPreconditionError` | `conflictReport` containing `PRECONDITION_FAILED` | `PreconditionError` |
| `KdbNotFoundError` | `documentGetResult.json` is null | `ErrNotFound` |
| `KdbUnauthenticatedError` | handshake rejected, or `sessionBeginAck` with empty `sessionId` | `ErrUnauthenticated` |
| `KdbBusyError` | `errorCode: "BUSY"` | `BusyError` |
| `KdbUnavailableError` | `errorCode: "UNAVAILABLE"` | `UnavailableError` |
| `KdbDeadlineExceededError` | `errorCode: "DEADLINE_EXCEEDED"` | `DeadlineExceededError` |
| `KdbLockError` | `lockResult.granted === false` | `LockError` |
| `KdbTransportError` | socket/WebSocket failure | `TransportError` |
| `KdbClosedError` | operation on a closed client | `ErrClosed` |
| `KdbUnsupportedError` | op the connected server does not implement (§2.4) | *(none — new)* |

Server `errorCode` values, from `go/kdb/wire/error_code.go`: `BUSY`, `UNAVAILABLE`,
`DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `CONFLICT`, `SCHEMA_VIOLATION`, `UNIQUE_VIOLATION`,
`UNAUTHORIZED`, `INTERNAL`.

### 8.2 Conflict report decoding

`reportBytes` decodes to `{transactionId, baseHash, targetHash, conflicts: [{documentId,
operationType, actualContentHash}]}`. Reproduce the Go ordering rule exactly
(`client.go:596-608`): **scan for `operationType === "PRECONDITION_FAILED"` first** and throw
`KdbPreconditionError` if any is found, before constructing an ordinary `KdbConflictError`. A
failed assertion and a lost race arrive on the same wire message but mean different things to the
caller; conflating them makes `compareAndSwap` retry a write that will never succeed.

### 8.3 `KdbUnsupportedError`

New in this component, with no Go counterpart, because the Go client has never faced a server
missing `0x14`–`0x1C`. When `get`, `upsert` or a lock verb is issued against a connection whose
server does not implement it, fail with a message that names the operation and suggests the SQL
path where one exists. Detection: the server's error frame if it sends one, otherwise the
request deadline from §7.5.

## 9. Test cases

### 9.1 Conformance, against shared fixtures

The claim "wire-compatible" has to be proven, not asserted. `go/kdb/interop/` already holds
`codec_golden_test.go`, `wire_interop_test.go` and `jvm_server_interop_test.go`; this component
adds a third consumer of the same fixtures.

1. **Frame round trip** against every golden frame — decode, re-encode, compare bytes.
2. **Envelope round trip** for all 14 client-relevant message kinds, key names included.
3. **`transactionBytes` round trip**: a TS-encoded transaction decodes in Go via
   `wire.DecodeTransaction`, and a Go-encoded one decodes in TS. Both directions, including the
   empty-`preconditions` omission (§6.1) and each precondition kind.
4. **`jsonByteArray` fixtures** — a byte array encoded by TS is byte-identical to the Go/Kotlin
   encoding for the same input, including the empty case (`[]`).
5. **Content-hash fixtures** — for a set of `(docId, json)` pairs, the TS hash equals the Go hash
   exactly. Include non-ASCII, embedded quotes, nested objects, and an empty object.
6. **Malformed-frame rejection**, ported from `go/kdb/wire/malformed_frame_test.go`: a frame
   whose declared length exceeds the buffer must throw a decode error, never a `RangeError`.

### 9.2 Live server, end to end

7. Connect / `putJSON` / `get` round trip against the **Go** server over TCP (Node).
8. The same over **WebSocket**, once §2.3's listener exists — the browser-path proof.
9. The same against the **JVM** server for the three messages it implements, with `get` and
   `upsert` producing `KdbUnsupportedError` rather than hanging.
10. `commit` succeeds on a current `baseVersion`, throws `KdbConflictError` on a stale one, with
    no partial write.
11. Two concurrent `commit` calls racing on one `baseVersion` — exactly one wins.
12. `replaceIf` succeeds with a hash from `getWithHash` and throws `KdbPreconditionError` with a
    stale one. **This is the test that proves §6.3**, and it will fail loudly if the canonical
    encoder is wrong, which is the point.
13. `compareAndSwap` converges under real contention (N concurrent mutators, all increments land).
14. `upsert` both creates and replaces; carries `sessionId`; is refused while another session
    holds a lease on the document.
15. `exec` with a write statement is durable afterward — the §5.3 auto-commit follow-up.
16. `acquireLock` / `renewLock` / `releaseLock`, including a fence token that changes across renew
    and a lapsed lease being refused.
17. `AbortSignal` mid-request rejects promptly and leaves the connection usable.
18. A large document (~30 fields, nested arrays/objects, non-ASCII) round trips byte-identically.

### 9.3 Runtime matrix

19. The package loads and runs its WebSocket path under Node 20+, Chrome, Firefox, Safari, Bun and
    a Workers-style runtime — the last of these being what catches an accidental Node built-in
    import (§2.2).

## 10. Phase 2 — `@kdb/codegen`

### 10.1 What it generates

`kdb schema codegen --namespace <ns> --out src/kdb.ts` reads the namespace's `kdb-schema`
(already serializable as `EmbedSchemaDto` JSON, already consumed by `KdbBrowser.openWithSchema`)
and emits TypeScript: one interface per document type, correctly nullable, plus a typed handle
that narrows `@kdb/client`'s `unknown` bodies and `string[][]` rows to real types.

### 10.2 Why generation rather than declaration

Restating §1.3 as an implementation constraint, because it is the decision most likely to be
quietly reversed later: the schema authority is server-side and already exists. A generated
client type cannot drift from it without the generator noticing at build time. A hand-declared
mongoose-style schema can, and will, and the drift will surface as a runtime write rejection in
production rather than a compile error in CI.

This also settles a Phase 1 constraint: **`@kdb/client` must not define any schema type of its
own.** Its bodies stay `unknown`. Everything typed is generated on top.

### 10.3 The typed SQL surface

Over the generated types, a Kysely-style builder produces SQL text and an argument array for
`query`/`exec`, and — the part that matters — supplies the per-column coercion Phase 1 refuses to
guess at (§7.6). The schema knows a column is an integer; the builder can then honestly return
`number`. Without the schema, nobody can.

### 10.4 Commits and branches are first-class

The generated handle exposes `baseVersion`, `commit`, and the branch operations as typed methods
rather than hiding them behind an ORM's illusion of mutable objects. This is the part of KDB with
no MongoDB analogue at all, and the part a client should be making *legible*.

## 11. Phase 3 — `@kdb/embed`

Included here as a phased deliverable of this component, not deferred to a separate one, so that
Phase 1's interface stays compatible with it (§11.3). It is later, not lesser: an embedded engine
in the browser is the capability that makes KDB's commit/branch model interesting to a front end,
and `kdb-browser-demo` already proves the shape works.

### 11.1 What it is

The engine running in-process — `openMemoryRuntime`, local commits, local SQL, and peer-sync or
stream-subscribe against a server for replication. Not a client: there is no connection, and the
data is local. `KdbBrowser`/`KdbBrowserHandle`
(`kdb-embed/src/jsMain/kotlin/dev/kdb/embed/js/KdbBrowser.kt`) already implements exactly this
today, including `subscribe` with reconnect/backoff and peer-sync recovery, `acceptRemote` /
`rejectRemote` / `mergeBranches`, and `head` / `getBaseVersion` / `setBaseVersion`.

### 11.2 Two candidate implementations

**(a) Kotlin/JS**, packaging what already exists. Fastest to a demo; it works today. Its costs are
real, though: nothing currently generates `.d.ts` (no `binaries.library()`, no
`generateTypeScriptDefinitions()`; `kdb-embed` declares `binaries.executable()`), `@JsExport`
cannot carry suspend functions, Kotlin collections or `Long`, and the existing facade works around
that by passing **JSON strings across the boundary** — `put(json: String): Promise<String>`,
`query(sql: String): Promise<String>`. That is a fine internal bridge and a poor public TS API,
and the bundle carries the coroutines runtime.

**(b) Go → WASM.** `go/wasm/demo/` exists and `js/wasm` is already a cross-compile target in the
release plan. This aligns with the fixed decision that Go is the production implementation, so
embed and server would share one engine rather than two.

**Recommendation: (b), with (a) kept as the working demo in the meantime.** Reassess at the start
of Phase 3 rather than now — Go's WASM binary size and its `syscall/js` boundary cost are the
open questions, and neither is answerable from the current code.

Known blockers either path must clear first, all already recorded in the finish-up plan:

- Browser "durable" storage is an in-memory map (`FileBackedPlatformIoShimFactory.js.kt:23-24`) —
  data vanishes on reload, appends are quadratic, only ~5MB localStorage snapshots persist. Real
  IndexedDB/OPFS persistence is a prerequisite for calling this durable, and saying otherwise
  would be dishonest.
- Kotlin's JS/Native zstd is an identity passthrough, so segments written there are not portable
  to a JVM/Go reader. Path (a) inherits this; path (b) does not.
- `go/kdb/embed/dir_lock.go` needed build tags for `GOOS=js` (finish-up plan item 0.3) — confirm
  it is fixed before path (b).

### 11.3 The Phase 1 constraint this imposes

`@kdb/client`'s operation set must be expressible as an **interface** that both a network client
and an embedded runtime can implement — `putJSON`, `get`, `query`, `commit`, `getWithHash` all
make sense locally. Phase 1 should therefore define `KdbOperations` as a standalone interface that
`Client` implements, so `@kdb/embed` can satisfy the same contract and application code can move
between local-first and server-backed without a rewrite. This costs nothing in Phase 1 and is
awkward to retrofit.

The pieces that do **not** generalize — `connect`, `close`, leases, `AbortSignal`-shaped
cancellation — stay on `Client`, outside that interface.

## 12. Non-goals

- **Peer-sync, stream-subscribe and DAG messages in `@kdb/client`.** Codes `0x02`–`0x0D` are a
  peer's vocabulary, not a client's. They belong to `@kdb/embed` (Phase 3), which is what actually
  holds a local DAG to sync. The client's decoder tolerates them; it does not speak them.
- **A hand-declared, mongoose-style client-side schema.** §1.3, §10.2.
- **Automatic reconnection with request replay** in Phase 1. §7.7.
- **An ORM with change tracking, lazy loading, or `populate`.** KDB has SQL and joins; a typed
  builder over them (§10.3) is the better shape.
- **Compiling the Kotlin or Go wire codec to JS for `@kdb/client`.** §13.1.
- **A `pg`/`mysql2`-compatible driver interface.** Nothing in the ecosystem would consume it.
- **Client-side query caching or connection pooling.** One long-lived connection; caching is the
  application's.

## 13. Implementation notes

### 13.1 Hand-written, not compiled — the reasoning

The tempting shortcut is to compile the existing Kotlin (or Go, via WASM) wire codec to JS and
wrap it. For the *network client* this is the wrong trade, for four reasons that compound:

1. **API quality.** `@JsExport` cannot express the interface in §4. The existing facade's
   JSON-string-in, JSON-string-out signatures are the evidence, not a hypothetical.
2. **Bundle size.** A network client should be ~10KB. Compiling an engine to ship a codec is two
   orders of magnitude off, and it is the difference between a package a front-end team adopts and
   one they refuse.
3. **Reach.** A hand-written client runs on Workers and Deno without a WASM loading story.
4. **The codec is small.** §5 fits in a table. The only intricate piece is §6.3's narrow record
   encoder. The shortcut is not saving much.

None of this applies to `@kdb/embed`, where the artifact *is* the engine and compilation is the
only sane approach. Different problems, different answers.

The cost of hand-writing is a third implementation to keep in sync. §9.1's shared fixtures are the
mitigation, and they are load-bearing rather than nice-to-have — which is why they are specified
before the live-server tests, not after.

### 13.2 Packaging

- ESM-first, with a CJS build for Node consumers.
- `"exports"`: `.` (core, no Node built-ins), `./tcp` (Node-only transport), `./node` (convenience
  re-export). The subpath split is what keeps the core edge-compatible.
- Hand-written `.d.ts` shipped from source; TypeScript is the source language.
- No runtime dependencies. Every primitive needed is a platform global in all targets.
- Published as `@kdb/client`. The finish-up plan currently lists npm publishing as deferred
  (item 2.10, and the Phase-4 deferred list); this component is the thing that changes that, and
  the release workflow needs an npm publish step added alongside the existing artifact steps.

### 13.3 Generating the message layer

`wire/messages.ts` could be hand-written or generated from `go/kdb/wire/payload_dto.go`'s struct
tags. **Recommendation: hand-write it in Phase 1** — it is ~200 lines, it is read constantly
during implementation, and a generator is a second thing to debug while the first thing is still
unproven. Revisit if the message set starts changing often; §9.1's fixtures catch drift either
way, which is the actual safety net.

### 13.4 Suggested order

1. Go WebSocket server listener (§2.3) — prerequisite, and not this component's code. **Done.**
2. `wire/` — frame, envelope, messages, bytes. Green against §9.1 fixtures 1, 2, 4, 6.
3. `wire/hash.ts` — the canonical subset. Green against fixture 5. Do this **before** the
   conditional writes that depend on it, not alongside them.
4. Transports: WebSocket first (both browser and Node), TCP second.
5. `Client`: connect, sessions, correlation, then documents, then SQL, then conditionals, then
   leases.
6. Live interop (§9.2) against the Go server; then the JVM server for the subset it implements.
7. Publish `@kdb/client`. Phase 2 and 3 follow as their own work.

### 13.5 Implementation status

Steps 1–6 are **done**. `packages/kdb-client` has the wire layer, both transports, the full
operation set and 81 passing tests; the Go WebSocket listener of step 1 landed alongside it
(§2.3). Fixtures are generated by `go/cmd/kdb-tsfixtures` from the real Go encoder and cover
frames, envelopes, byte arrays, transaction encoding and content hashes; the TS canonical
encoder reproduces Go's `DocumentBody` bytes and SHA-256 exactly.

Step 6's live interop (§9.2) runs in `test/interop.test.ts` against a real Go server started by
`go/cmd/kdb-tsinterop`, over a real WebSocket — handshake, documents, conditional writes,
concurrent compare-and-swap, multi-document commit, SQL. The content-hash agreement is checked
end to end there by handing a locally-computed hash back as an `EXPECT_CONTENT_HASH`
precondition and requiring the server to accept it, which is the only way to test an encoder
whose output never crosses the wire. Only step 7 (publishing to npm) remains.

Two departures from what §3–§8 anticipated, both found during implementation and corrected in
this document:

- `Lease.fence` is a `number`, not a `string` — the wire carries a uint64 as a JSON number
  (`go/kdb/wire/lock_ops.go:99`), and typing it as a string would imply precision the wire does
  not preserve. See §4's inline note.
- The handshake must send `namespaces: []` and `localHeads: {}` rather than `null`. Kotlin
  declares both non-nullable with no default, so a JSON null makes the JVM's strict decoder
  throw and the connection dies with **no response at all** rather than a clean rejection. Go
  hit exactly this and fixed it the same way (`go/kdb/wire/payload_mapper.go:31-45`). This is a
  third instance of the §6.2 failure mode — a Kotlin-side non-nullable with no default — and
  worth checking for on any new field.

Remaining before step 7: step 1 (the Go WS listener), which is not this component's code, and
step 6, which needs a running server. Until step 6 runs, the compatibility claim is proven at
the byte level and unproven end to end; the package README states that limitation plainly.

## 14. Estimated lines

Phase 1, `@kdb/client`: **1,600–2,400 NBNC**, split roughly as — 250 wire framing and envelope,
200 message definitions, 150 the `jsonByteArray` convention and JSON helpers, 250 the canonical
content-hash encoder (§6.3, the densest part per line), 200 transports, 500 the client and its
operations, 150 the error taxonomy, plus 600–900 of tests, of which the conformance fixtures
(§9.1) are the larger and more valuable half.

Phase 2, `@kdb/codegen`: 800–1,200, dominated by the SQL builder rather than the type emitter.

Phase 3, `@kdb/embed`: not estimable until §11.2 is decided; the blockers listed there are larger
than the packaging work either way.
