# Layer 17 — One Runtime, Many Spaces + gRPC Transport

## Status: Phase A IMPLEMENTED; everything else PROPOSED

Two asks, one document, because the second one only gets cheap once the first one lands:

```
Layer 17 — Multi-Namespace Runtime + gRPC
  [~] 64. Multi-namespace embedded runtime (one host, N spaces)
        [x] A. Split the open path                 - go/kdb/embed/host.go
        [ ] B. Routing adapter
        [ ] C. One budget, arbitrated
        [ ] D. Whole-database maintenance
        [ ] E. Cross-namespace transactions
  [ ] 65. gRPC transport hooks (frame service over HTTP/2)
```

Every claim below was checked against source at `aeb56ca` and, where it was checkable by
running something, by running it. Probe output is quoted verbatim.

---

## 1. Why a consumer ends up with nine engines

The observation that started this: Zolik (`/Users/davidj/devel/grpc-bench/zolik`) runs nine
fully independent embedded KDB engines, one per namespace, mirroring its nine Mongo
collections. That is not a choice its author made freely. It is the only shape the current
embed API allows, and Zolik's own source says so:

```go
// server/internal/db/kdb.go:186
// One data root per namespace: the engine's directory lock is
// per-root, so two runtimes over one root would refuse to open.
root := filepath.Join(path, name)
```

That comment is correct. `OpenFileRuntimeWithOptions` takes exactly one `namespaceID`
(`go/kdb/embed/file.go:36`) and, before it does anything else, takes the data root's *writer*
lock exclusively (`acquireDirLock`, `go/kdb/embed/dir_lock_unix.go:44`). `flock(2)` is scoped
to the open file description, so a second `open()`+`flock(LOCK_EX|LOCK_NB)` conflicts even
inside the same process. Probe:

```
=== RUN   TestProbeTwoNamespacesOneDataRoot
    SECOND OPEN REFUSED: data directory locked: .../001/.kdb.write.lock
```

So a consumer that wants N namespaces has exactly one option today: N data roots, N runtimes.
The on-disk layout already reserves `dataRoot/ns/<namespaceID>/` for multiple namespaces under
one root (`ensureNamespaceDirs`, `file.go`) — the directory structure anticipated this; the
open path never caught up.

### 1.1 What nine roots actually cost

Not what you would guess. Nine *empty* runtimes are cheap:

```
PROBE nine-empty-runtimes: goroutines 2 -> 11 (+9), heapInUse 1.6 -> 1.9 MiB (+0.3 MiB)
```

The cost is not allocation, it is **nine independent ceilings that cannot see each other**.
`OpenFileRuntimeWithOptions` hardcodes a 64 MiB default hot-tier budget per runtime
(`file.go:94`), and every derived budget is a fraction of it
(`go/kdb/storage/memory_budget.go`):

| Derived budget | Fraction | Per runtime | × 9 |
|---|---|---|---|
| Document version cache | 0.50 | 32 MiB | 288 MiB |
| Commit-ops (DAG history) | 0.25 | 16 MiB | 144 MiB |
| Memtable flush threshold | 0.25 | 16 MiB | 144 MiB |
| History-tree cache | 0.25 | 16 MiB | 144 MiB |
| **Total ceiling** | **1.25** | **80 MiB** | **720 MiB** |

`StorageOptions.MemoryBudgetBytes`' own doc comment already names the problem — "an
application that opens several runtimes in one process is multiplying that default by each
open" — and the only remedy it offers is for the caller to divide by hand. Dividing by hand is
the wrong answer for a skewed workload, which is every real workload. Zolik's `matches` is hot
and `oauth_flows`/`login_codes` are nearly empty; a static ninth-share makes the hot namespace
evict against 3.5 MiB while eight cold ones sit on headroom they will never touch.

Secondary costs, all linear in N and all avoidable:

- **Nine attach + writer lock pairs.** Maintenance tooling (`LockDataDir`, and every
  `kdb-inspect verify/repair/restore/backup`) is per-root, so a whole-database backup is nine
  separate operations with no common instant between them. There is no crash-consistent
  snapshot of "the database" — only of one namespace at a time.
- **Nine platform-IO shims**, each with its own `FileBackedPlatformIOFactory` and, when
  `KDB_S3_BUCKET` is set, its own S3 client and replication tier (`file.go:76`).
- **Nine background goroutines** (one per runtime, confirmed by the probe).
- **Nine `KdbServerRuntime`s** if the consumer wraps them, which Zolik does — and therefore
  nine `MemoryGuard`/`Admission` pairs if resource governance is ever switched on
  (`SetMemoryBudget`, `server_runtime.go:479`). Each would be handed the same cgroup limit and
  each would measure the same process RSS, so all nine trip at once: nine copies of the
  machinery to do one node's job. (Zolik does not enable it today, so this is latent, not live.)
- **No cross-namespace transaction, ever.** Nine DAGs, nine commit logs, nine write locks.
  Zolik works around this with a per-namespace `sync.Mutex` and application-level
  read-check-write, which is correct only because the deployment is single-process by design.

### 1.2 What is *not* the problem

Worth being precise, because it bounds the fix. `ServerEngine` is genuinely per-namespace and
should stay that way: it owns a WAL, a memtable, a sharded version store, a running document
tree, and a delta writer, all keyed to one namespace's files. Merging those into one structure
would be a rewrite with no upside — the physical per-namespace split is the correct design and
is what makes per-namespace compaction, eviction, and replay possible.

The good news is that the interface above it already assumes multiplexing. Every method on
`storage.Adapter` takes `namespaceID` as its first parameter (`go/kdb/storage/adapter.go`),
and `ServerEngine` ignores it in every implementation because the engine *is* the namespace:

```go
// go/kdb/storage/engine/server_engine.go:330
func (e *ServerEngine) PutDocument(namespaceID string, doc document.Document) error {
	e.pending.Put(doc)   // namespaceID unused
	return nil
}
```

That unused parameter is the seam. A routing adapter that dispatches on it satisfies
`storage.Adapter` exactly, with no interface change anywhere.

---

## 2. Component 64 — Multi-namespace embedded runtime

**Shape:** one *host* that owns the data root, the lock, the IO shim and the memory budget;
N per-namespace engine sets underneath it. Not one engine internally — one engine *host*.

```go
// go/kdb/embed

// OpenFileHost takes dataRoot's lock and builds the shared I/O shim, and opens
// no namespace at all.
func OpenFileHost(dataRoot string, opts FileRuntimeOptions) (*Host, error)

type Host struct { /* dataRoot, dirLock, storio shim, (Phase C) *BudgetArbiter */ }

// Namespace opens namespaceID under this host, or returns the already-open runtime
// for it. Idempotent: two engines over one namespace's files is exactly what the
// writer lock exists to prevent.
func (h *Host) Namespace(catalog, namespaceID string, sch schema.KdbSchema) (*EmbeddedKdbRuntime, error)

// NamespaceWithOptions is Namespace with per-namespace storage tuning - a hot
// namespace and a nearly-empty one rarely want the same budgets or durability.
func (h *Host) NamespaceWithOptions(catalog, namespaceID string, sch schema.KdbSchema, sopts StorageOptions) (*EmbeddedKdbRuntime, error)

func (h *Host) Namespaces() []string
func (h *Host) CloseNamespace(namespaceID string) error  // one namespace; host keeps its lock
func (h *Host) Close() error                             // every namespace, then the lock
```

`EmbeddedKdbRuntime` keeps its current shape and its current single-namespace meaning — it is
what a caller holds and what `KdbServerRuntime` wraps. Nothing that exists today changes
semantics; the host is new surface above it.

### 2.1 Work, in dependency order

**Phase A — split the open path.** `OpenFileRuntimeWithOptions` currently does five things in
one function: lock, resolve history strategy, build the IO shim, open the engine handle,
replay. Extract the per-namespace tail (strategy → handle → DAG → replay → `EmbeddedKdbRuntime`)
into an unexported `openNamespace(host, catalog, nsID, sch, opts)` that takes an already-locked
host and a already-built IO shim. `OpenFileRuntimeWithOptions` becomes
`OpenFileHost(...).Namespace(...)` with a host that closes when the runtime does — so its
signature, behaviour, and every existing caller and test are untouched. This phase should
change no test.

The `alreadyLocked` option (`storage_options.go`) already exists for exactly this pattern —
`MigrateHistoryStrategy` takes the maintenance lock and then opens a runtime inside it. Phase A
generalises that from a one-off escape hatch into the normal path, and `alreadyLocked` folds
into the host.

**Phase B — the routing adapter.**

```go
// go/kdb/storage/engine
type MultiplexAdapter struct{ /* map[string]*ServerEngine, RWMutex */ }
```

Implements `storage.Adapter` by dispatching on the `namespaceID` every method already carries,
returning a typed `unknown namespace` error rather than silently using a default. Needed only
by callers that want one adapter across namespaces (SQL over multiple spaces, cross-namespace
transactions); Phase A alone does not require it. Keep it in `engine`, next to the thing it
routes to.

**Phase C — one budget, arbitrated.** This is the phase that pays for the whole component.

```go
// go/kdb/storage
type BudgetArbiter struct{ /* total int64, per-namespace claims + demand signals */ }
```

The host holds one; each namespace's `StorageEngineConfig` gets its share from it instead of a
hardcoded 64 MiB. Start deliberately dumb — proportional-to-demand reallocation on a slow
timer, floor per namespace so a cold space can still serve a read, hysteresis so a burst
doesn't thrash — and measure before making it clever. The existing eviction machinery does not
change; only the number it evicts against becomes dynamic.

Two properties to preserve, both already true per-namespace and both easy to lose here:
versions reachable from the current tree are pinned and do not answer to the budget (see
`ResolvedDocumentCacheBytes`' doc comment), and a shrinking budget must evict lazily rather
than synchronously on the resize path.

**Phase D — whole-database maintenance.** With one root, `LockDataDir` already covers every
namespace, so `kdb-inspect verify/backup/restore` gain a real database-wide mode and a
genuinely crash-consistent backup instant. Mostly plumbing once A lands, but it is the phase
that turns "nine directories" into "a database" for operators.

**Phase E — cross-namespace transactions.** Deliberately last, and deliberately optional. It
needs a commit protocol across N DAGs and is the only phase that touches correctness of
existing data. Nothing else here depends on it, and Zolik's per-namespace mutex works without
it. Do not bundle it into A–D.

### 2.2 What Phase A settled

Landed in `go/kdb/embed/host.go`, with `OpenFileRuntimeWithOptions` reduced to a host with one
namespace under it. Its signature, behaviour and every existing caller are unchanged, and the
regression is pinned by `TestTwoNamespacesShareOneHost` and `TestNineNamespacesOneRoot`.

Two decisions worth recording, both narrower than the first draft assumed:

- **Namespace lifetime is the host's, not reference-counted** — see the note above.
- **The I/O shim is per host, so SyncMode, the S3 replica tier and the replication policy are
  host-wide.** Nothing observable changes today, because setting two of any of them under one
  root required two runtimes over it, which the lock refused. But it bounds one future
  consolidation: `FileAuthRegistry` already opens several namespaces under one root with the
  caller holding the lock — the same shape as a `Host`, arrived at independently — while
  deliberately building a *local-only* shim, so that auth writes don't go to S3 while the data
  namespaces beside them do. Folding it onto a `Host` needs the shim, not just the budget, to
  become per-namespace. One shim per host is the cheaper arrangement until something actually
  needs that, and it is what makes one S3 client serve nine namespaces instead of nine.

### 2.3 Migration

The layout changes from `<path>/<name>/ns/zolik/<name>/` to `<path>/ns/zolik/<name>/`. A
consumer with existing data needs a move, not a rewrite — the per-namespace directory contents
are unchanged. `kdb-inspect migrate` already exists (`go/cmd/kdb-inspect/migrate.go`); this is
a new mode on it. **Old and new must not both be openable against the same tree**, or a
half-migrated database looks fine until the namespaces disagree.

---

## 3. Component 65 — gRPC transport hooks

The trade the consumer is proposing — out-of-process over gRPC instead of in-process embed,
accepting a network hop and serialization for process isolation and independent restart — is a
reasonable one, and the surface it needs turns out to be tiny.

### 3.1 The seam already exists

`sqlWireConnHandler.handleFrame(frame []byte) ([]byte, error)`
(`go/kdb/server/wire_listen.go:271`) decodes, dispatches and encodes one frame synchronously,
and its own doc comment calls it "the in-process path tests and StreamHub use". Everything
above the socket — the 30 wire message types (`go/kdb/wire/types.go`), handshake, session
lifecycle, RBAC at handshake *and* at commit time, the per-request panic backstop
(`dispatchRecovering`) — sits behind it and is transport-agnostic already.

So the gRPC service is an adapter over bytes, not a second protocol:

```proto
service KdbWire {
  // One stream == one connection: same session lifecycle, same per-connection
  // auth, same "sessions die with the connection" cleanup as the TCP listener.
  rpc Connect(stream Frame) returns (stream Frame);
}
message Frame { bytes payload = 1; }
```

Estimate: promote `handleFrame` to an exported per-connection `FrameHandler` seam (~20 lines),
plus a `ListenGRPC` mirroring `ListenSqlWire`'s shape (~150 lines), plus the proto. No new
semantics, no second dispatch table to keep in sync, RBAC and session cleanup inherited rather
than reimplemented.

A native message-per-RPC service (`Commit`, `GetDocument`, `Query`, …) would be nicer to hand
to a non-Go client, but it is a second surface to keep in step with the wire types forever.
Recommendation: ship the frame service first, and only add typed RPCs for the handful of ops
that actually need them once there is a client asking. For reference, Zolik's entire use of the
embed API today is five entry points — `srv.Commit`, `srv.GetDocument`, `rt.DAG`, `rt.Storage`,
`rt.Close`.

### 3.2 Separate module, decided

`go/go.mod` currently has one non-trivial dependency tree (the AWS SDK, for the S3 tier) and a
`gobind` tool dependency — the module is built for gomobile and for `embedbundle`. Adding
`google.golang.org/grpc` + `google.golang.org/protobuf` to it puts gRPC's transitive tree into
every embedded and mobile build that will never open a socket.

**Decided: gRPC lives in its own module.** `go/grpc/` with its own `go.mod`, importing
`github.com/limidus/kdb/go`. Never the reverse — the core module must not gain a gRPC
dependency in any build configuration, including a test-only or tool-only one. The exported
`FrameHandler` seam from §3.1 is what makes that possible without an import cycle, and it is the
main reason to do the seam extraction properly rather than inlining the adapter.

A build tag inside the core module is **not** an acceptable substitute. Build tags gate
compilation, not the module graph: `google.golang.org/grpc` would still land in the core
`go.mod` and `go.sum`, and every consumer of the embed API would inherit it whether or not they
build the tag. The separation has to be at the module boundary to mean anything.

### 3.3 Off unless a flag turns it on

**Decided: the gRPC listener is opt-in by configuration, never on by default.** There is already
a precedent to follow exactly — `ServiceConfig.WSAddr`, whose doc comment reads "Empty disables
it" (`go/kdb/config/service.go:27`). gRPC gets the same shape:

```go
// GRPCAddr is the gRPC listen address (grpc:// or grpcs://). Empty — the
// default — disables it, and a build without the gRPC module linked in
// refuses a non-empty value rather than ignoring it. See §3.3.
GRPCAddr string
```

So: `--grpc-addr` / `grpcAddr` in the config file / `KDB_GRPC_ADDR`, defaulting to empty, and no
listener started unless it is set. Nothing about an existing deployment changes.

The field lives in the **core** `ServiceConfig` even though the listener does not. Config is
data, a string costs nothing, and one config schema across both binaries is worth more than the
purity of splitting it. What that buys is the important half of the design:

- **`kdb-service` (core module) never links gRPC.** If it is handed a non-empty `grpcAddr` it
  exits with a clear error — "this build has no gRPC listener linked; run kdb-service-grpc" —
  rather than silently ignoring the setting. A flag that quietly does nothing is worse than no
  flag.
- **`go/grpc/cmd/kdb-service-grpc`** composes the same server runtime and the same listeners,
  plus the gRPC one when `grpcAddr` is set. It is the only artifact that carries the dependency.

This is what makes "we may migrate to that model" safe to explore: the gRPC path can be built,
deployed and benchmarked against a real workload without the default binary, the embedded API,
or a mobile build changing at all.

### 3.4 Two things to settle before writing code

Both are consequences of HTTP/2 rather than of KDB:

- **Ordering.** The TCP listener answers frames on one connection in order, and sessions assume
  it. A single bidi stream preserves that; multiple concurrent streams do not. One stream per
  session, not one per request.
- **Frame size.** gRPC defaults to a 4 MiB receive limit. Scan and snapshot responses can
  exceed it. Either raise it explicitly or chunk — silently truncating a snapshot is the worst
  available outcome.

The existing spec (`docs/kdb-spec-layer7-component21-wire-protocol-framing.md:350`) says
"gRPC / HTTP/2 — out of scope". That line needs updating, with a pointer here, when this lands.

---

## 4. Recommended order

1. **64 Phase A** (split the open path) — no behaviour change, unblocks everything, smallest
   reviewable unit. Nothing else here should start before it merges.
2. **65** (gRPC over the frame seam) — independent of A, and the seam extraction is worth
   having regardless. Can run in parallel.
3. **64 Phase C** (shared budget) — the phase that actually pays; needs A.
4. **64 Phases B and D** — as demand appears.
5. **64 Phase E** (cross-namespace transactions) — only on a concrete requirement.

Phases A and C together are what turn "nine engines because the lock says so" into "one
database with nine spaces sharing one budget". That is the whole point; B, D and E are
follow-ons, not prerequisites.
