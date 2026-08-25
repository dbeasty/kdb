# Component 38 — Go-Native Server

Layer 12. Depends on the existing Go engine port (`go/kdb/storage`, `go/kdb/transaction`,
`go/kdb/document`, `go/kdb/dag`, `go/kdb/schema`, `go/kdb/sql`, `go/kdb/embed` — all real per the
maturity audit) and, for RBAC parity, the Kotlin `kdb-auth-store` design (`RegistryAuthStore`,
`PasswordHasher`, `AuthorizingTransactionEngine`) as a reference to port, not a runtime dependency.

## 1. Purpose

`go/kdb/server/server_runtime.go`'s `Commit` returns `errNotImplemented("commit")`, and
`go/cmd/kdb-service/main.go` prints `sql=disabled (wire listeners not ported)` at startup — the Go
engine port is complete for embedded/in-process use but cannot act as a server anything else
connects to. This component finishes exactly that: wire listeners (TCP, matching what
`kdb-transport-tcp`'s JVM side already does), a SQL/document request handler wired to the existing
Go `TransactionEngine`, and RBAC enforcement ported from the Kotlin reference. The payoff is
direct and was the reason this component got prioritized above everything else in this layer: a
cloud deployment running `kdb-service` as a Go binary needs no JVM, which drops the realistic
minimum Lightsail tier from ~2GB RAM (JVM headroom) to a plausible ~1GB or less — see §9.

## 2. Dependencies

- `go/kdb/transaction` — `TransactionEngine`, conflict policies, already real (write-phase
  rollback + document lock manager per the maturity audit's confirmation this exists in both Go
  and Kotlin).
- `go/kdb/storage/...` — WAL/memtable/sstable/delta, already real.
- `go/kdb/sql` — parser/planner, port status not fully audited component-by-component; confirm
  during implementation how much of `SqlExecutor`/`QueryPlanner`'s Kotlin feature set the Go side
  already covers vs. needs finishing alongside this component (the Kotlin `SqlWireHost` this
  component's wire handler is modeled on assumes a feature-complete SQL executor underneath it).
- `go/kdb/wire`, `go/kdb/transport/tcp` — codec and socket layer already exist and are proven
  real (used by the Go peer-sync client, `go/kdb/peersync/client.go`) — this component's TCP
  listener is the accept-side counterpart of a dial-side implementation that already works.
- Kotlin `kdb-auth-store` (`RegistryAuthStore.kt`, `PasswordHasher.kt`) and
  `kdb-transaction`'s `AuthorizingTransactionEngine.kt` — read as the reference design to port;
  the Go side today has only `permission_matching.go`/`resource_path.go`/`types.go` (grant-matching
  logic, no store, no hashing, no commit-time enforcement).

## 3. Public Interface

```go
// go/kdb/server/server_runtime.go — filling in the existing (currently error-returning) shape,
// not inventing a new one. Signature shown is illustrative; match whatever ServerRuntime's
// existing method set already declares where it doesn't conflict with finishing Commit.
type ServerRuntime struct { /* unexported: engine registry, per-namespace TransactionEngine cache */ }

func (r *ServerRuntime) Commit(namespaceID string, tx Transaction, principal *auth.Principal) (CommitResult, error)
func (r *ServerRuntime) Query(namespaceID string, sql string, principal *auth.Principal) (QueryResult, error)
func (r *ServerRuntime) Release(namespaceID string) error  // currently a documented no-op in Kotlin's v1 too (KdbServerRuntime.kt:87) — port the same limitation, don't silently promise more
```

```go
// go/kdb/server/wire_listen.go — new file, TCP accept loop + per-connection session handling,
// modeled directly on kdb-server's SqlWireListen.kt/SqlWireHost.kt (same wire message types,
// same handshake, same framing — this is a port, not a redesign).
func ListenSqlWire(addr string, runtime *ServerRuntime, authEngine auth.AuthEngine) (*Listener, error)

type Listener struct { /* unexported */ }
func (l *Listener) Close() error
```

```go
// go/kdb/auth/registry_store.go — new, port of kdb-auth-store's RegistryAuthStore + PasswordHasher.
type RegistryAuthStore struct { /* unexported: backed by a DAG-committed namespace, per Kotlin's design */ }

func NewRegistryAuthStore(dag dag.CommitDag, usersNamespace, rolesNamespace string) *RegistryAuthStore
func (s *RegistryAuthStore) CreateUser(username, password string, roles []string) error
func (s *RegistryAuthStore) VerifyPassword(username, password string) (bool, error)  // PBKDF2, matching PasswordHasher.kt's parameters (120k iterations, HMAC-SHA256, random salt) so credential hashes are portable between a JVM-created and Go-created deployment
func (s *RegistryAuthStore) Grants(username string) ([]Grant, error)
```

## 4. Data Structures

```go
// Mirrors kdb-wire's WireTypes.kt HandshakePayload/CommitPayload shapes exactly — this is a
// codec compatibility requirement, not a design choice: a Go server must produce byte-identical
// frames to a Kotlin one, since Zolik's Go client (Component 40) and any future non-Go client
// must not need to know which language they're talking to.
type Transaction struct {
    BaseVersion string
    Ops         []Op
}

type Op struct {
    Kind      string // "write" | "delete" | "upsert" — see §9 on the upsert addition
    Namespace string
    DocID     string
    JSON      []byte
}

type CommitResult struct {
    CommitHash string
    Conflicts  []ConflictDetail // empty on success
}

type Grant struct {
    Role     string
    Resource string // "database" | "collection" | "document" scoped, per RBAC's existing model
    Kind     string // matches AuthAction's existing enumeration
}
```

## 5. Contracts

- **Wire compatibility is the load-bearing constraint.** Every frame this component's listener
  produces or accepts must be byte-identical to what `kdb-server`'s JVM implementation produces —
  `go/kdb/interop`'s existing golden/wire-interop tests are the acceptance mechanism (extend them
  to cover the new SQL/commit message types, don't invent a parallel test strategy).
- **Commit semantics match Component 39's fix, not Component 39's bug.** This component's `Commit`
  must perform real ancestry-checked conflict detection (`STRICT`/`LAST_WRITE`/`CUSTOM`/
  `APPEND_ONLY`, per the existing, correct `TransactionEngine` conflict policies) — it is not
  peer-sync, and must not inherit peer-sync's blind-head-move bug (§1.2 of the gap analysis). This
  is a reminder for implementation discipline, not a dependency: the two components touch
  unrelated code paths, but both are "make peer-to-peer-ish writes conflict-safe" work landing in
  the same season, and a reviewer should confirm this component's commit path was never modeled on
  peer-sync's.
- **RBAC enforcement happens at commit time, not just at connection auth**, matching what the
  Kotlin `AuthorizingTransactionEngine` already does correctly — port that design decision, not
  just its data shapes. A `Commit` call must re-check the principal's grants against every op in
  the transaction immediately before the write lands, the same defense-in-depth property the
  maturity audit confirmed is real on the Kotlin side.
- **Password hash portability.** `VerifyPassword` must use identical PBKDF2 parameters to the
  Kotlin `PasswordHasher` (iteration count, hash algorithm, salt length/encoding) so a user created
  against a Kotlin `kdb-server` deployment can still log in against a Go one and vice versa, if a
  deployment ever runs both (unlikely for Zolik specifically, but a Go server producing
  hashes incompatible with the reference implementation would be a correctness bug worth catching
  in review regardless).
- **`Release`/session cleanup**: port Component 45's disconnect-cleanup fix (§5.2 of the gap
  analysis) into this component's connection lifecycle from the start, rather than shipping the
  same "session/lock leaks forever on disconnect" bug a second time in a second language. This is
  the one place this spec explicitly asks for *better* behavior than a literal port of today's
  Kotlin code, because the bug being ported is known and already scheduled to be fixed on the
  Kotlin side.

## 6. Error Cases

- `errNotImplemented(...)` — retained for any op kind or SQL feature genuinely not yet ported
  (e.g., if `go/kdb/sql`'s planner doesn't yet cover the full grammar `kdb-sql` does) — must be a
  distinct, named error type a client can detect and report clearly, not a generic panic or a
  silently-wrong result.
- `ConflictError` — wraps `ConflictResult`/`ConflictDetail`, returned on a genuine `STRICT`-policy
  conflict; matches the wire-level `ConflictReport` message type byte-for-byte with the Kotlin
  server's equivalent.
- `AuthorizationError` — RBAC denial, distinct from a conflict, matching `KdbAuthorizationException`'s
  wire representation.
- `AuthenticationError` — bad credentials at handshake, before any namespace/commit context exists.

## 7. Test Cases

1. **Byte-for-byte wire compatibility**: a Go client (Component 40, or a raw test client) talks to
   this Go server and produces frames indistinguishable from a client talking to the JVM
   `kdb-server`, verified against the existing `go/kdb/interop` golden fixtures extended to cover
   handshake + commit + query frames.
2. **Cross-implementation interop, live sockets this time** (not file-format, per the maturity
   audit's finding that today's interop proof is file/codec-level only): a Go client connects to
   this Go server AND, separately, to a real running JVM `kdb-server`, and gets equivalent results
   from equivalent operations against equivalent data. This is the test that actually proves the
   "no JVM required" claim without silently requiring one anyway for some operation.
3. **Concurrent commits from multiple connections, same document** — real conflict detection under
   real concurrency (not sequential), mirroring the JVM side's own proven behavior.
4. **RBAC denial at commit time**, not just at handshake — a principal authenticated successfully
   but lacking a grant for the specific document being written is rejected at `Commit`, matching
   the Kotlin `AuthorizingTransactionEngine`'s behavior.
5. **Password hash cross-verification**: a hash produced by the Kotlin `PasswordHasher` verifies
   successfully via this component's `VerifyPassword`, and vice versa (generate with one, verify
   with the other) — proves parameter compatibility rather than assuming it.
6. **Disconnect mid-transaction releases locks and session state** — the Component 45 fix, proven
   from day one in this component rather than inherited as a known bug.
7. **`errNotImplemented` surfaces cleanly** for at least one deliberately-unported SQL feature (if
   any remain after this component ships), rather than crashing or misbehaving.
8. **Load: sustained small-message throughput on the target Lightsail tier** (§9) — this is the
   test that actually validates the cost claim; run it on hardware/VM specs matching the proposed
   $7/mo tier, not a developer laptop.
9. **Restart durability of the RBAC registry** — create a user, restart the server process, confirm
   the user still exists and can authenticate (this requires the registry's `CommitDag` to be
   file-backed, not the in-memory one `KdbServiceMain.kt` currently wires up by default on the
   Kotlin side — port the file-backed version, or flag this as a follow-on if the Go storage
   engine's persistence isn't ready by the time this component ships).
10. **Zero-JVM deployment smoke test**: build and run this component with no JVM anywhere on the
    host (verify via `ps`/process list in the test harness, not just "I didn't install one") —
    the literal claim this component exists to make true.

## 8. Non-Goals

- Full feature parity with every `kdb-sql` grammar extension on day one — port the subset Zolik's
  Go client (Component 40) actually needs first (document get/put/upsert/query with exact-match
  and range predicates), and treat anything beyond that as a fast-follow, tracked via the
  `errNotImplemented` error case rather than silently blocking this component's ship date.
- WebSocket transport for this server — TCP only in v1, matching Component 40's client scope;
  browser clients aren't part of Zolik's near-term plan.
- Multi-node/distributed deployment of this server — single process, matching today's JVM
  `kdb-server`'s own deployment shape; horizontal scaling is out of scope.
- Fixing peer-sync's conflict-detection bug (Component 39) — related, landing in the same season,
  but a separate component with separate code paths. Do not attempt to solve both in one change.

## 9. Implementation Notes — the cost claim, made concrete

The reason this component is Priority 1 in the gap analysis: a Lightsail instance running the
existing Zolik Go service plus a JVM `kdb-server` needs ~2GB RAM (a $12/mo plan) mostly to give the
JVM room to breathe — production guidance for even a "small" JVM workload is 1–2GB of heap alone,
before metaspace/thread-stack/JIT overhead. Replace the JVM half with this component and the same
two-process deployment (Go service + Go `kdb-service`) plausibly fits in under 1GB total (tens to
low hundreds of MB per Go process, no JVM tax), making the **$7/mo tier (1GB RAM, 2 vCPU, 2TB
transfer)** a realistic target instead of $12/mo — roughly a 40% cut to the fixed monthly floor,
independent of and additive with the traffic-volume savings the distributed architecture itself
provides. Test 8 and test 10 above exist specifically to make this a measured claim by the time
this component ships, not an estimate.

Port order recommendation: wire listener + framing first (lowest risk, `go/kdb/wire` and
`go/kdb/transport/tcp` already prove the codec/socket layer works), then plain
`Commit`/`Query` against the existing Go `TransactionEngine` with no auth (prove the SQL/document
path end-to-end), then RBAC (`RegistryAuthStore` + `AuthorizingTransactionEngine` port) last, since
it's the highest-effort single piece and the other two are independently useful/testable without
it (e.g., for Zolik's own dev/local-testing deployments where RBAC may not matter yet).

## 10. Estimated Lines

1,800–2,800 NBNC: ~400 for the wire listener/session shell (largely a mechanical port of
`SqlWireListen.kt`/`SqlWireHost.kt`'s structure), ~300 for `Commit`/`Query` request handling atop
the existing `TransactionEngine`, ~700–1,000 for the RBAC port (`RegistryAuthStore`,
`PasswordHasher`, `AuthorizingTransactionEngine` equivalent — the largest single piece, since it's
new logic, not just wire plumbing), ~400–800 for tests (test 2 and test 8 in particular need real
multi-process/real-hardware test infrastructure, not unit-test-only coverage).
