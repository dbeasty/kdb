# Layer 17 — One Runtime, Many Spaces + gRPC Transport

## Status: Component 65 and Phases A/C IMPLEMENTED; Phases B, D, E PROPOSED

Two asks, one document, because the second one only gets cheap once the first one lands:

```
Layer 17 — Multi-Namespace Runtime + gRPC
  [~] 64. Multi-namespace embedded runtime (one host, N spaces)
        [x] A. Split the open path                 - go/kdb/embed/host.go
        [ ] B. Routing adapter
        [x] C. One budget, arbitrated              - go/kdb/storage/budget_arbiter.go
        [ ] D. Whole-database maintenance
        [ ] E. Cross-namespace transactions
  [x] 65. gRPC transport hooks (frame service over HTTP/2) - go/grpc/ (separate module)
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

**Phase C — one budget, arbitrated.** The phase that pays for the whole component. *Shipped;
see §2.4 for what it actually does.*

```go
// go/kdb/storage
type BudgetArbiter struct{ /* total, per-namespace participants + reservations */ }

// One namespace's side of the pool.
type BudgetParticipant interface {
	DemandBytes() int64
	SetBudgetBytes(int64)
}
```

The host holds one; each namespace's budget is cut from it instead of a hardcoded 64 MiB.

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

### 2.3 What Phase C settled

Landed as `go/kdb/storage/budget_arbiter.go`, plus the resize and demand surfaces it needs
underneath. The policy is deliberately dumb and should stay that way until something measured
says otherwise: **floor, then proportional to demand, then damped.**

Every namespace gets `DefaultNamespaceFloorBytes` (4 MiB) so a quiet one stays able to serve a
read — slower, not broken. What is left is split in proportion to demand. A share is only
applied when it has moved more than 12.5% from what is installed, because shrinking a share
evicts. Rebalance runs every 10s, and also immediately whenever membership changes, so a new
namespace never runs unbounded until the first tick and a closing one hands its room back at the
moment it stops using it.

**Demand is not residency.** Reporting what a namespace currently holds would be a feedback loop
with the wrong sign: a namespace squeezed to its floor holds little, would therefore report
little demand, and would stay at its floor forever. So a namespace that has taken cold loads
since it was last measured — versions re-read from the delta log, the engine's existing signal
that the working set did not fit — asks for 1.5× what it holds. That is what lets a starved
namespace climb back over successive passes.

Measured, with a 64 MiB pool and the nine-namespace zolik shape:

| | hot namespace | idle namespace |
|---|---|---|
| Nine independent runtimes (before) | 80 MiB ceiling | 80 MiB ceiling, 720 MiB total |
| Static ninth of one pool | 7.5 MiB | 7.5 MiB |
| **Arbitrated** | **30.4 MiB** | **4 MiB floor** |

Four times what dividing by hand gives the namespace that needs it, and the total is bounded.

**What resizes, and what deliberately does not.** `shardedDocByHashStore` and `boundedTreeStore`
already had `SetBudget`; the memtable threshold was read from the open-time config on every
flush check, so it gained an atomic override rather than a mutation of a struct several paths
read without synchronisation. The commit DAG gained `SetOperationsBudget`, the resize half of
`SetOperationsLoader`.

The version store is the one piece the arbiter will not touch without a cold loader installed.
Its budget is what lets it drop versions, and dropping a version nothing can re-read is data
loss, not eviction — which is exactly why `SetColdLoader` installs the loader and the budget
together. An engine with no delta log behind it keeps an unbounded version store whatever the
arbiter says, and only its trees and memtable move. `SetOperationsBudget` refuses on the same
grounds: bounding a DAG that cannot fetch operations back would silently turn history into
commits that wrote nothing.

**Two bugs the tests caught, both worth recording.**

The first was mine and structural: `StorageOptions.MemoryBudgetBytes` means two different things
at the two levels — on a host it is the *pool*, on a namespace it is that namespace's fixed,
non-arbitrated share. `Host.Namespace` passed the host's straight through, so a host given an
explicit 256 MiB total handed 256 MiB to *each* of nine namespaces: the exact arithmetic this
component exists to remove, reintroduced by the API that was supposed to fix it.

The second was subtler and is now a property test. Hysteresis can suppress one namespace's
*decrease* while letting another's increase through, and the installed total then drifts above
the pool — 2.8% over, with nine namespaces registering one at a time. That is not a reporting
artifact; those are real ceilings on real caches. The damping is now explicitly a comfort and
never a licence to exceed the pool: when honouring it would over-commit, the pass forgoes it
entirely, since the computed plan sums to the pool by construction.

**Not done here, deliberately.** The pool is per host, so it bounds the namespaces under one data
root and not the process. A process opening several hosts is back to ceilings that cannot see
each other — the same shape as before, one level up. The same is true of
`KdbServerRuntime`'s `MemoryGuard`, which is still per server runtime. Worth a process-wide
parent pool eventually; not worth inventing before something needs it.

### 2.4 Migration

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

### 3.4 Two HTTP/2 details, settled before writing code

Both are consequences of HTTP/2 rather than of KDB:

- **Ordering.** The TCP listener answers frames on one connection in order, and sessions assume
  it. A single bidi stream preserves that; multiple concurrent streams do not. One stream per
  session, not one per request.
- **Frame size.** gRPC defaults to a 4 MiB receive limit. Scan and snapshot responses can
  exceed it. Either raise it explicitly or chunk — silently truncating a snapshot is the worst
  available outcome.

The existing spec (`docs/kdb-spec-layer7-component21-wire-protocol-framing.md:350`) says
"gRPC / HTTP/2 — out of scope". That line needs updating, with a pointer here, when this lands.

### 3.5 What Component 65 settled

Shipped as the `go/grpc` module, `go/kdb/server/frame_host.go`, and `GRPCAddr` in the core
`ServiceConfig`.

**The seam turned out to be smaller and better than §3.1 described.** `FrameHost` takes a
`FrameConn` — a channel of inbound frames and a `Send` — which `stream.ConnectionHandle` already
satisfied, so the TCP and WebSocket listeners needed no change at all. The gRPC transport is
about 200 lines that adapt one bidirectional stream to that interface. Handshake auth, RBAC,
sessions, SQL, shedding and the panic backstop are reached, not reimplemented.

**A correction to §3.1.** It claimed "the TCP listener answers frames on one connection in order,
and sessions assume it". That is not what the code does. Frames are dispatched *concurrently*,
bounded by `MaxInFlightFrames`, with ordering guaranteed **per session** by `sessionTicket` —
tickets taken on the reader goroutine, so two frames naming the same session run in the order
they were sent while frames on different sessions may not. Serializing everything, as the
original design note implied, would have been a throughput regression against the TCP path for
no correctness gain. Reusing the existing handler gets the real semantics for free, which is the
argument for the seam rather than a bespoke gRPC dispatch loop.

**Admission is inherited, not skipped.** The byte-stream transports install the memory-pressure
gate in their frame reader, where a shed request's body is never read. gRPC delivers whole
messages, so there is no I/O left to save — but the *behaviour* still has to match, or a server
under pressure would refuse a TCP client and serve an identical request from a gRPC one. That is
the divergence `ListenSqlWireWSTLS`'s doc comment already warns about, so `FrameHost.Admit`
applies the same gate to a complete frame.

**Frame size was a real bug, now a regression test.** gRPC defaults to a 4 MiB receive limit and
a KDB frame may be 16 MiB, so a large scan or snapshot would have been refused by the transport
with a failure that looked like a KDB error. `Listen` raises both directions to the protocol's
own bound; `TestFrameLargerThanTheGrpcDefaultIsCarried` sends 6 MiB through and reads it back.

**The service split.** Making `--grpc-addr` do something required a binary that links the
listener, and that required `kdb-service`'s `main` to be reusable. It moved verbatim to
`kdb/service.Main` — no control-flow changes, `os.Exit` calls untouched — leaving
`cmd/kdb-service` a three-line shim. `service.RegisterGRPCListener` is the hook; the gRPC module
registers through `go/grpc/register`, and `go/grpc/cmd/kdb-service-grpc` is the same service with
that package linked. Verified end to end: `kdb-service --grpc-addr ...` exits 2 with the
remediation, `kdb-service-grpc` logs `grpc="enabled (127.0.0.1:19099)"`.

**The cost of the split, paid in the Makefile.** `cd go && go test -race ./...` does not reach a
separate module, so the same boundary that keeps gRPC out of the gomobile build also keeps it out
of CI unless swept explicitly. `test-go` and `build-go` now have a second line each. This is the
recurring tax on the decision in §3.2 and it is worth naming: anything that enumerates packages or
binaries has to learn about `go/grpc`.

**Left open, deliberately.** `kdb-service-grpc` is built by `build-go` but is *not* in
`release-binaries`' artifact list — adding a published release artifact is a distribution
decision, not a build one, and belongs to whoever owns the release matrix.

---

## 4. Recommended order

1. ~~**64 Phase A** (split the open path)~~ — **done**. See §2.2.
2. ~~**65** (gRPC over the frame seam)~~ — **done**. See §3.5.
3. ~~**64 Phase C** (shared budget)~~ — **done**. See §2.3.
4. **64 Phases B and D** — as demand appears.
5. **64 Phase E** (cross-namespace transactions) — only on a concrete requirement.

Phases A and C together are what turn "nine engines because the lock says so" into "one database
with nine spaces sharing one budget". Both are in. B, D and E are follow-ons, not prerequisites,
and Component 65 is independent of all of them.
