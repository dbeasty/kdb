# Component 40 — Go Client SDK (`kdb-client-go`)

Layer 12 (renumbered from Revision 1's Component 34 — see the gap analysis §7; no content
collision, just moved to keep the whole proposed batch together under one layer number).
**Revised in Revision 2**: reuses the Go wire codec that turned out to already exist
(`go/kdb/wire`, `go/kdb/transport/tcp`) instead of building one from scratch, adds an explicit
`Upsert` method per Zolik's stated need, and targets Component 38's Go-native server as the primary
destination once it exists, with the JVM `kdb-server` as the interim/fallback target.

Depends on `go/kdb/wire` (frame codec, already real), `go/kdb/transport/tcp` (socket layer, already
real and already proven by `go/kdb/peersync/client.go`'s dial-side usage), Component 38 (Go-native
server — the intended long-run counterpart) and, until that ships, the existing JVM `kdb-server`
(interim counterpart, wire-compatible by construction).

## 1. Purpose

Zolik's cloud server and its `gomobile`-embedded on-device match runtime are both Go. This
component is a minimal, purpose-built Go client — not a general ANSI-SQL/JDBC-equivalent driver —
covering exactly the operations Zolik's `game.Repository`/`match.Repository`/`stats.Repository`/
`auth.SessionRepository`/`user.Repository` pattern needs: connect, run one SQL statement or a raw
document get/put, commit a transaction with optimistic-concurrency semantics matching Mongo's
`ReplaceOne({_id, version: expected})`, and — new this revision — **upsert** a document
unconditionally, matching Mongo's `ReplaceOne(filter, doc, upsert=true)` semantics that
`stats.Repository.UpsertPlayerStats`/`auth.SessionRepository.CreateSession` already assume.

**What changed since Revision 1 and why it matters here**: a full Go engine port exists in this
repo (`go/kdb/...`, confirmed real by the maturity audit) — including a working wire codec
(`go/kdb/wire`) and TCP transport (`go/kdb/transport/tcp`) already proven by the Go peer-sync
client's dial-side usage. **This component should reuse those packages directly rather than
reimplementing framing from scratch**, which was Revision 1's assumption before their existence was
confirmed. This cuts real scope: the codec/socket layer is done, this component becomes mostly
request/response semantics and Zolik-shaped ergonomics on top of it.

## 2. Dependencies

- `go/kdb/wire` — frame codec (length-prefixed, typed messages, correlation IDs), already real.
  Import it directly; do not reimplement.
- `go/kdb/transport/tcp` — `net.Dialer`-based socket client, already real, already used by
  `go/kdb/peersync/client.go` for exactly this kind of dial-and-handshake flow. This component's
  `Connect` should look structurally like that file's connection setup, not be written from a blank
  page.
- **Target server, two phases**: in the near term, the only real, connectable server is the JVM
  `kdb-server` — this component must be verified against it first (Component 38 doesn't exist yet).
  Once Component 38 ships, this component should also be verified against it, and — because both
  ends are now Go, sharing `go/kdb/wire` — that path carries materially lower interop risk than the
  cross-language one, per Component 38 §7 test 2's explicit live-socket interop test.
- **Verify before implementing, don't assume, still true**: the audit confirmed `kdb-server`'s
  `SqlWireHost`/`SessionManager` are real, but did not fully enumerate the SQL-execution wire
  message shapes as distinct from the peer-sync ones the master spec's §8.5 does fully specify.
  Read `SqlWireHost.kt`/`WriteCoordinator.kt` (Kotlin side) directly, and cross-check against
  whatever `go/kdb/server` ends up implementing for Component 38, before finalizing payload
  encodings here.

## 3. Public Interface

```go
// github.com/limidus/kdb/go/kdb/client (in-repo, alongside the rest of the Go port — see §9)
package client

import "context"

type Client struct { /* unexported: go/kdb/transport/tcp connection, session, correlation-id counter */ }

// Connect dials host:port and performs the wire handshake, authenticating with a bearer token
// per Component 41. Blocks until the handshake completes or ctx is cancelled.
func Connect(ctx context.Context, addr string, token string) (*Client, error)

func (c *Client) Close() error

// --- document operations -----------------------------------------------------------------

// PutJSON writes one document as a new commit in namespace ns — used for the create-only /
// insert-then-later-CAS-update lifecycle (match/game documents' first write). Returns the
// resulting commit hash, retained as the BaseVersion anchor for a later Commit call.
func (c *Client) PutJSON(ctx context.Context, ns string, docID string, json []byte) (commitHash string, err error)

// GetJSON reads one document's current JSON by id, plus the commit hash it was last written at.
func (c *Client) GetJSON(ctx context.Context, ns string, docID string) (json []byte, commitHash string, err error)

// Upsert writes a document unconditionally — create it if it doesn't exist, replace it if it
// does, no BaseVersion, no conflict possible. This is the direct analogue of Mongo's
// ReplaceOne(filter, doc, upsert=true), and is what stats.Repository.UpsertPlayerStats and
// auth.SessionRepository.CreateSession/CreateGuestSession need — those call sites have no
// version to anchor on and don't want one; forcing them through Commit/BaseVersion would be
// modeling a CAS write for a case that explicitly isn't one. Targets a namespace whose
// ConflictPolicy is LAST_WRITE server-side (see gap analysis §2 — the namespace-level policy
// choice is a server/namespace-config concern, not something this call negotiates per-request).
func (c *Client) Upsert(ctx context.Context, ns string, docID string, json []byte) (commitHash string, err error)

// --- transactional CAS write --------------------------------------------------------------

// Transaction mirrors the wire Transaction object from kdb-spec.md §7.1 — this is the
// operation match.Repository.UpdateWithVersion needs: "write iff nothing else committed since
// I read BaseVersion."
type Transaction struct {
    BaseVersion string        // commit hash this write is anchored on; from a prior GetJSON/PutJSON
    Writes      []DocWrite
}

type DocWrite struct {
    Namespace string
    DocID     string
    JSON      []byte  // full document; JSON Merge Patch support (partial) is a possible follow-on,
                       // not required for Zolik's v1 usage, which always writes the whole match doc
}

// Commit submits a Transaction. On success returns the new commit hash. On a conflict (something
// else committed against the same BaseVersion first) returns ErrConflict, wrapping whatever
// ConflictReport detail the server sent — the direct analogue of Zolik's match.ErrVersionConflict.
func (c *Client) Commit(ctx context.Context, tx Transaction) (commitHash string, err error)

var ErrConflict = errors.New("kdb: version conflict")

// --- SQL --------------------------------------------------------------------------------

// Query runs one SQL statement (SELECT) against ns and decodes each result row's `_doc` column
// (plus any selected scalar columns) into dest, which must be a pointer to a slice of a
// caller-defined struct — mirrors mongo-driver's cur.All(ctx, &out) idiom Zolik's Go code
// already uses throughout stats.Repository, so porting call sites is close to mechanical.
func (c *Client) Query(ctx context.Context, ns string, sql string, args []any, dest any) error

// Exec runs one non-SELECT SQL statement (schema/DDL, or a write expressed as SQL rather than
// PutJSON/Upsert/Commit — most of Zolik's write paths should prefer the document API above).
func (c *Client) Exec(ctx context.Context, ns string, sql string, args []any) error

// --- append-only log ----------------------------------------------------------------------

// AppendEvent writes one entry to an APPEND_ONLY namespace (Zolik's per-match action log and,
// per the gap analysis, its match_results namespace too — see kdb-spec.md §10's own
// myapp/events example). Always succeeds per that namespace mode's conflict policy — no
// BaseVersion needed. Distinct from Upsert: this is for a namespace where every write is a new,
// independent record (never replacing a prior one by the same docID), Upsert is for a namespace
// where a given docID's document is meant to be replaced wholesale on each write.
func (c *Client) AppendEvent(ctx context.Context, ns string, docID string, json []byte) error
```

## 4. Data Structures

```go
// ConflictDetail carries enough of the server's ConflictReport for Zolik's error handling to
// log something actionable, without modeling KDB's full internal conflict-classification types.
type ConflictDetail struct {
    Namespace string
    DocID     string
    Kind      string // "CONCURRENT_WRITE" | "DELETE_WRITE" | "WRITE_DELETE"
}

// Frame representation is go/kdb/wire's own type — this component does not define its own,
// per §2's reuse decision. If go/kdb/wire's existing frame type needs a field this component
// can't get at, that's a signal to extend go/kdb/wire itself, not to fork a parallel type here.
```

## 5. Contracts

- **Connection lifetime:** one `*Client` = one TCP connection = one KDB session. Zolik's cloud
  server should hold one long-lived `*Client` per process (or a small pool), not one per request —
  matches how it already holds one `*mongo.Client` today.
- **Correlation IDs:** every request frame carries a client-assigned correlation id (via
  `go/kdb/wire`'s existing mechanism — reused, not reinvented); the client matches responses to
  in-flight calls by that id, enabling concurrent outstanding calls on one connection (needed
  because Zolik's `Manager.HandleAction`/`Hub` broadcast pattern issues several concurrent
  reads/writes across goroutines against one game).
- **`Commit` conflict semantics:** unchanged from Revision 1 — `ErrConflict` wrapping
  `[]ConflictDetail`, no partial write, matching `match.Repository.UpdateWithVersion`'s existing
  all-or-nothing contract.
- **`Upsert` never conflicts and never needs a `BaseVersion`.** If the target namespace's
  server-side policy is not `LAST_WRITE`, the server should reject the call outright (a
  configuration mismatch, not a runtime race) rather than silently applying CAS semantics the
  caller didn't ask for — surfaced as a distinct error (`ErrWrongConflictPolicy` or similar; name
  finalized during implementation against whatever Component 38/the JVM server actually returns).
- **Context cancellation:** every method respects `ctx`; a cancelled context aborts the in-flight
  wire call and returns `ctx.Err()`, leaving the connection itself reusable for the next call.
- **Namespace-per-call, not connection-wide:** every method takes `ns` explicitly, since a single
  Zolik process legitimately touches several namespaces (`matches`, `match_results`, `player_stats`,
  `users`, `sessions`) from the same connection/session.

## 6. Error Cases

- `ErrConflict` — see §5. The only error type Zolik's CAS call sites are expected to branch on
  explicitly (mirroring today's `errors.Is(err, match.ErrVersionConflict)` pattern).
- `ErrWrongConflictPolicy` — `Upsert` called against a `STRICT`-policy namespace, or `Commit`
  called against a `LAST_WRITE`-policy one — a caller/config error, not a race.
- `context.DeadlineExceeded` / `context.Canceled` — propagated from `ctx`, standard Go idiom.
- `ErrNotFound` — `GetJSON`/`Query` found no matching document.
- `ErrDuplicateKey` — a `unique = true` schema field violation on write (analogue of Mongo's
  `mongo.IsDuplicateKeyError`, which `stats.Repository.InsertMatch` branches on to produce
  `ErrAlreadyRecorded`) — contingent on unique-constraint enforcement surfacing a distinguishable
  error server-side; verify against whichever server (JVM or Component 38) this is tested against.
- Transport-level errors — wrapped in a `*TransportError` carrying the underlying `go/kdb/transport`
  error, not swallowed or re-typed as a protocol error.
- Server-side auth failure — `ErrUnauthenticated`, returned from `Connect` if the handshake's auth
  step (Component 41) fails.

## 7. Test Cases

1. **Connect, PutJSON, GetJSON round trip against the JVM `kdb-server`** — end-to-end, not mocked;
   proves cross-language interop at the wire level, the interim target until Component 38 exists.
2. **Same round trip against Component 38's Go-native server**, once it exists — the intended
   long-run target; per Component 38 §7 test 2, this is expected to be materially lower-risk than
   test 1 since both ends share `go/kdb/wire`.
3. **Commit succeeds when BaseVersion is current HEAD; returns ErrConflict when stale**, with no
   partial write on conflict.
4. **Two concurrent Commit calls racing on the same BaseVersion** — exactly one succeeds, proven
   under actual concurrency.
5. **Upsert creates a document that doesn't exist yet, and replaces one that does — both in one
   method, no prior existence check needed by the caller.** This is the test that actually
   validates the create-on-first-write behavior the gap analysis §2 flags as unverified at the
   engine level — if the underlying `Write` op requires prior existence, this test fails and the
   gap analysis's engine-level item needs fixing first, not this component.
6. **Upsert against a STRICT-policy namespace returns `ErrWrongConflictPolicy`**, not a silent
   CAS-shaped behavior the caller didn't ask for.
7. **Query decodes multiple rows into a Go struct slice**, including a field sourced from `_doc`
   alongside a schema-indexed scalar column.
8. **AppendEvent against an APPEND_ONLY namespace never conflicts under concurrent writers.**
9. **Context cancellation mid-call** returns promptly with `ctx.Err()`, connection stays usable.
10. **Large document (a full Zolik match document, ~30 fields, nested maps/arrays) round trips
    byte-for-byte** through PutJSON/GetJSON and Upsert.

## 8. Non-Goals

- A general-purpose `database/sql`-compatible driver — unchanged from Revision 1.
- Client-side query caching, connection pooling beyond "hold one long-lived connection," or
  retries — kept in the caller.
- Peer-sync protocol support in this component's v1 — unchanged from Revision 1; a LAN host
  embedding KDB directly (Component 43, if it proceeds — see that component's revised status) talks
  peer-sync at the embed level, not through this client.
- TLS in v1 — add once a server-side TLS story is confirmed stable enough to be worth a client-side
  counterpart.
- Reimplementing wire framing — explicitly a non-goal this revision, having confirmed
  `go/kdb/wire`/`go/kdb/transport/tcp` already do this correctly.

## 9. Implementation Notes

- **Package location, resolved this revision**: given the Go engine port already lives at
  `go/kdb/...` in this repo, this component belongs there too — `go/kdb/client` — rather than a
  separate `sdks/go` directory or external repo, per Revision 1's open question. Living alongside
  `go/kdb/wire`/`go/kdb/transport` makes wire-protocol drift between them impossible to miss in one
  CI run, and Zolik simply `go get`s the existing `github.com/limidus/kdb/go` module.
- Read `SqlWireHost.kt`/`WriteCoordinator.kt` (Kotlin) for the interim JVM-target payload shapes;
  read Component 38's actual implementation for the long-run Go-target ones once it exists — they
  must end up identical by construction (§1's wire-compatibility contract), so this is really one
  encoding to nail down, verified against two servers.
- Zolik-side integration shape (context, not this component's deliverable): a
  `zolik/server/internal/kdbrepo` package implementing the same method signatures
  `match.Repository`/`stats.Repository`/`auth.SessionRepository`/`user.Repository`/`game.Repository`
  already expose (see `zolik`'s own in-progress interface-extraction work), backed by this SDK
  instead of `*mongo.Collection`. `UpsertPlayerStats` and `CreateSession`/`CreateGuestSession` map
  directly onto this component's `Upsert`; the CAS-shaped repositories map onto `Commit`.

## 10. Estimated Lines

600–1,000 NBNC (down from Revision 1's 900–1,400, since the codec/transport layer is now reused
rather than built): ~100 for connection/handshake atop `go/kdb/transport/tcp`, ~250 for the
document operations (PutJSON/GetJSON/Upsert/Commit) once payload shapes are confirmed, ~150 for SQL
Query/Exec marshaling, ~100 for error mapping, ~250–400 for tests (test 1 needs a real JVM
`kdb-server` in CI; test 2 needs Component 38, so is necessarily a later addition, not part of this
component's initial ship).
