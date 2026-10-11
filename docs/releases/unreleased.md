# Unreleased (intended as v0.7.0)

Notes for changes on `main` since v0.6.2. Rename this file to `vX.Y.Z.md` (and give it a
proper heading and summary) in the change that bumps `VERSION` — see
`docs/kdb-release-plan.md` §4.

This release is mostly about **multi-node correctness**. A new end-to-end suite runs real
clusters of `kdb-service` processes — several nodes, concurrent writers on every one,
`kill -9`, drains, partitions, late joiners — and checks that every node ends on the same head
commit with the same documents. On v0.6.2 it found three ways for a cluster to get stuck or
stop converging, each silent, each fixed here. It also carries a SQL correctness fix whose
behaviour change existing clients must know about, and crash recovery that opens about 400
times faster.

It is **v0.7.0, not v0.6.3**, because of the first behaviour change below.

## Behaviour changes — read before upgrading

**A query whose result exceeds the result bound now fails instead of being silently
truncated (#115).** v0.6.2 collected the first 10,000 document ids and *then* applied the
`WHERE` clause, so on a namespace with more documents `SELECT COUNT(*)` said 10000, a filter
matching only later documents returned nothing, and `UPDATE`/`DELETE` by id could report
success while changing nothing. Results are now exact, and a result larger than the bound fails
with `RESOURCE_EXHAUSTED` — a non-retryable error in every client — rather than handing back a
prefix the caller cannot tell is incomplete. The bound is `--max-result-rows` /
`KDB_MAX_RESULT_ROWS` / `maxResultRows` (default 10000, restart to change; it also feeds the
admission cost model's memory estimate). `kdb_id = ?` and `kdb_id IN (...)` are now served by
key, so updates and deletes by id no longer scan the namespace. The TypeScript client gains
`delete()` and `Transaction.deletes`. Kotlin has the same semantics
(`SqlResultLimitException`, `KdbErrorCode.RESOURCE_EXHAUSTED`).

**A node may now move `main` to a commit that is not a descendant of its previous head
(#114).** When two heads hold identical content, a node no longer stacks another pairwise merge
on top; it builds one canonical "join" whose parents are the content-carrying commits
underneath, so every node holding the same commits builds the identical head. Only empty merge
commits ever drop off `main`'s path; documents are never touched, and startup replay accepts a
logged join that supersedes the head. Anything outside KDB that assumed `main` only moves to
descendants — an external tool walking first parents — should read `docs/kdb-distributed-plan.md`.

**`kdb branch checkout` is refused (#117).** It used to set a branch to the head it already had
and print "checked out" while changing nothing; the CLI keeps no current branch. Read another
branch as the revision `branch:<name>`.

**A delta-log segment that cannot be read now stops startup (#121)** with an error naming the
segment, instead of being skipped — which could silently open a node behind its last
acknowledged write. A torn tail left by a crash still recovers as before.

**Kotlin/JS and Kotlin/Native read and write real zstd (#116, #109).** They used to label
uncompressed blocks as zstd, which Go and the JVM could not read. A Kotlin/Native data directory
written before #109 is refused (no Native build was ever released).

## Fixes — distributed sync

**A five-node mesh could fall into a merge storm and never converge (#114).** Nodes holding
identical documents kept creating empty merge commits faster than they spread: over 10,000
after writes had stopped, with every sync succeeding and no node ever settling. Needed three or
more nodes and slow syncs (a loaded machine). See the `main` behaviour change above.

**A node that had drained during replication came back permanently broken (#113).** A clean
shutdown could checkpoint a peer commit the node had received but not yet logged; after restart
every sync from or to that node failed with `commit ... has no frame in the delta log`. Seen in
about half of drain-under-load runs. Checkpoints now describe only logged commits, shutdown
stops inbound peer sync first, and **a data directory already in this state heals** on the next
sync, from the peer's copy of the commit.

**A snapshot-bootstrapped node got stuck forever once the cluster committed on a branch forked
behind its snapshot (#111).** Every sync failed with `storing commit ...: missing parent ...`.
The joiner now fetches just that fork point's state — one transfer per fork point, never the
history — and a node that cannot is told so with a typed error that names `deepen`. The serving
node must have this fix (see Upgrading).

**The ops loader indexed the delta log once and never again (#113)**, so a commit logged after
its first cold load and later evicted from memory reported "no frame in the delta log" though
its frame was on disk. Needed only a small enough memory budget.

## Fixes — durability and recovery

**Crash recovery opens about 400 times faster (#123).** Every replayed commit probed the blob
store, and each probe rebuilt the SSTable reader and read the whole table just to learn its
size: 14,000 commits with no checkpoint took 149 s to open, now 0.36 s. Replay after a crash
covers everything since the last checkpoint (up to the maintenance interval, 5 minutes by
default), so a busy node could take minutes to come back.

**A table flushed during a compaction disappeared until the next restart (#123).** Compaction
replaced the whole table list with its output, dropping tables added while it ran.

**Replayed commits that are stored but not applied are counted and logged (#121)**, and exposed
as `recovery` in `GET /v1/ns/{ns}/status`. A single-writer log reports zero.

**Issue #43** (an acknowledged write reported lost after `kill -9`): a new reproducer kills the
real server 620 times across five durability configurations without losing any of 110,950
acknowledged writes, and runs nightly on Linux (#122). The old e2e test reported any failed read
as a lost write; it now tells missing, rolled-back and unreadable documents apart (#112). #43
stays open until the Linux soak has run.

## Fixes — SQL, storage, clients

- **SQL silently truncated results at 10,000 documents (#115)** — see Behaviour changes.
- **Go `json` path accessors panicked instead of returning an error (#107)**, including a path
  reachable from SQL `UPDATE ... SET` on a JSON path with an out-of-range or negative index.
- **Kotlin's WAL ignored its configured segment size (#119)**; `StorageEngineConfig.walMaxSegmentBytes`
  now takes effect (default 64 MiB).
- **Kotlin TCP and WebSocket servers leaked exceptions on stop (#110)**, the WebSocket server on
  every stop, and cancelling a WebSocket `listen` never closed its listening socket. `stop()` now
  waits for its connection handlers.
- **The in-memory stream transport dropped frames, and the stream subscriber could drop a desync
  error (#120)**, after which a subscriber never learned its state was wrong. A full event buffer
  now ends the subscription with an error the consumer sees.
## Fixes — embeddable Go bundle

**The embeddable Go source bundle now includes `kdb/server` (#108).** This is the known issue
listed in v0.6.2. A downstream project that vendors the bundle and imports `kdb/server`
directly — to build its own server binary or image around the native engine — got a zip
without it, because the bundle's entry points covered the in-process embedding surface
only. `kdb/server` is now an entry point, and the bundle carries its closure: the wire
protocol, the TCP and WebSocket transports, peer sync, streams, stored procedures
(`kdb/script`) and the full-text and vector index packages it reaches. The bundle grows
from 27 to 40 packages.

**Dependency change for bundle consumers.** The bundle's `go.mod` now requires
`github.com/dop251/goja` (the JavaScript engine behind stored procedures) and
`golang.org/x/sync`, plus their indirects (`dlclark/regexp2/v2`, `go-sourcemap/sourcemap`,
`google/pprof`, `golang.org/x/text`). All are pure Go; the bundle still builds with
`CGO_ENABLED=0` on every release platform. A consumer that imports only the in-process
packages compiles none of this in, but the requirements are in the bundle's module graph.

## Upgrading

**Upgrade one node at a time.** Stop a node (cleanly, or after a crash), start the new binary on
the same data directory and addresses, and move on. No flag changes are needed, and old and new
nodes replicate with each other throughout. A nightly test does exactly this from v0.6.2 — a
3-node rolling upgrade under writes, a node killed with `kill -9` and restarted on the new
binary, and a 5-node mesh of old and new nodes under writes and kills — and every acknowledged
write survives and every node converges (#125).

**A new node joining by snapshot through a v0.6.2 peer can stop with an error naming deepen.**
Folding a fork point's state needs the serving node to have #111; an older server can't, so the
joiner reports a typed `ShallowBoundaryError` instead of converging. Recover it with
`POST /v1/ns/{ns}/deepen` or `kdb deepen`, or bootstrap with `bootstrap=snapshot,deepen=true`
from the start. Upgrading the serving peers first avoids it. (The error message also suggests
setting `deepen=true` on an already-stuck joiner; that does not work yet — see Known issues.)

**Rolling back to v0.6.2 is only safe from a clean stop.** v0.6.2 does not understand the
canonical join commits this release writes (#114). If a node running this release crashed, do
not start v0.6.2 on its data directory: v0.6.2's replay silently rolls back acknowledged writes,
and upgrading again afterwards does not bring them back (its checkpoint names the old head). Start
the node on this release first, let it recover, stop it cleanly, and only then downgrade.

## Build, tests and CI

- `./gradlew build` passes again and is the Kotlin CI gate (#106).
- A multi-node e2e suite (`test_cluster.py`, #112); crash and cluster tests keep their data
  directories and logs, and CI uploads them on failure.
- A server-path `kill -9` reproducer with a nightly Linux soak (#122).
- Dead stubs removed: the Kotlin `kdb-storage-compaction` module and the Go `kdb/tier` package,
  neither ever released or imported (#118).

## Known issues

- **Damage in the middle of a delta-log segment silently truncates it.** If bytes in the middle
  of a segment that is no longer the newest are damaged — disk corruption, not a crash — startup
  treats the damage as the end of the segment and opens without the commits after it, with no
  error. A fix is in progress (N5 in `docs/kdb-open-issues-plan.md`); it should land before this
  release.
- **A lookup racing a compaction can transiently miss a key** whose old table compaction is
  deleting. A fix is in progress (table reference counting); it should land before this release.
- Issue #43 stays open pending the nightly Linux soak (see above).
- **The `deepen=true` peer option cannot rescue a snapshot joiner that is already stuck** on a
  fork point an older server cannot fold, although the error suggests it: the replicator skips
  deepen for a namespace whose sync just failed. Use `POST /v1/ns/{ns}/deepen` or `kdb deepen`.
- `Coordinator.updateSubscriber` can still drop subscriber-state updates when its buffer is full,
  and closed in-memory stream clients are never removed from their hub (#120's follow-ups).
