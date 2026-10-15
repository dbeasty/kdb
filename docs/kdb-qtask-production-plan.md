# KDB plan: serve qtask's production workload

Written 2026-10-15 against `main` at `2739e24` (v0.8.1). It turns what migrating qtask (a
task-management backend, today on MongoDB) onto KDB found into units of KDB work. Every finding is
labelled **measured** (with the number and how it was measured) or **hypothesis** (a likely cause
that the unit must confirm before fixing). Nothing here has met qtask's real production data: all
numbers come from generated data shaped like it.

Where the evidence lives (all in the qtask repo, `github.com/dbeasty/qtask`, branch
`feat/data-layer-abstraction`, PR #10):

- `bench/` - Docker benchmark: MongoDB vs KDB, each database capped at **2 CPU / 4 GB**, driven
  through qtask's own data layer with the operation mix its services issue (`bench/README.md`,
  with file:line references). `npm run bench:docker -- --scale prod --duration 120`.
- `scripts/rehearsal/`, `scripts/kdb-restore-drill.ts` - migration rehearsal and backup/restore
  drill against an on-disk `kdb-service` (results: qtask `docs/KDB_MIGRATION.md`, section 8).
- `src/data/kdb/store.ts` - the KDB store. Its comments name each KDB behaviour it works around.

## The headline

Production-scale dataset: 517,464 documents in one namespace (1,000 users, 100,004 tasks with
768-number embeddings, 105k embedding jobs, 200k activities, 50k comments, ...). kdb-service
v0.8.0, `--durability sync`, `--data-dir` on a container volume, 2 CPU / 4 GB; qtask's store with
pushdown on and its indexes and constraints declared. **Measured** (2026-10-15, before the store
changes in "What qtask already did", which a second run is measuring now):

| workload (actions/s) | MongoDB, `j:true` | KDB, sync |
| --- | ---: | ---: |
| mixed, 1 user | 161 | 0.2 |
| mixed, 8 users | 447 | 0.7 (32 errors) |
| mixed, 64 users | 506 | 0.2 (273 errors) |
| write-heavy, 32 users | 726 | 0.8 (119 errors) |
| queue claim, 1 worker | 297 | 54 |
| queue claim, 64 workers | 61 | 6 |
| container start to first task list | 1.1-1.7 s | 250-257 s |

KDB sat at its 2-CPU limit (196-198%) throughout; MongoDB at 30-130%. KDB `async` durability was
no better, so this is not fsync.

The rehearsal (qtask `docs/KDB_MIGRATION.md` section 8, 552,552 documents, 4 CPU / 4 GiB):
the heaviest user's task list, its count and its search were **refused** (`RESOURCE_EXHAUSTED`,
"estimated 1.79 GB > 1.56 GB budget"); a collection-wide `COUNT(*)` over 100k tasks took
`kdb-service` past 4 GiB and it was **OOM-killed** even with `--memory-budget-mb 1536`; restoring a
backup took 21.5 minutes of index rebuilding.

Part of this was qtask's store (it fetched every matching document whole to keep a page or a
count). That part is fixed on the qtask side. The units below are what KDB itself must change.

## What qtask already did (so KDB need not)

- Splits an indexed `$or` into one query per branch (until U3 ships).
- Counts on the server when the filter has an exact SQL form, and otherwise decides pages,
  claims and counts on lean rows (ids plus the filter's fields) before fetching only the chosen
  documents by key.
- Pages every scan; declares lookup indexes on `_c` and unique fields; lets the server enforce
  unique constraints instead of checking in-process.
- Records which fields a document stores as explicit `null` (`_nulls`), because SQL cannot tell
  `null` from absent (U8 would remove the need).

## How to execute a unit

As in `docs/kdb-open-issues-plan.md`: one unit = one branch = one PR, worktree off fresh `main`,
check `ListAgents` first, **reproduce before fixing** (a test that fails on `main`), Go and Kotlin
together for wire/format changes, progress as a new file under `docs/progress/`. In addition, every
unit here re-runs its qtask reproduction (named in the unit) against a build of the branch, via
qtask's `KDB_SERVICE_BIN` / `KDB_CLIENT_MODULE` overrides, and records before/after numbers.

---

## Phase P0 - blockers for qtask's cutover

### U1. Expiry and `COUNT(*)` without reading document bodies - M (the biggest single cause)

**Measured, profiled (2026-10-15):** the expiry sweeper alone keeps a 2-CPU `kdb-service`
saturated. With no client connected, on the benchmark's 517k-document namespace and
`--expire-interval 1m`, CPU sat at ~0.1% and then rose to 117% for most of each minute. A 60 s CPU
profile (`/debug/pprof/profile` on the admin address) put **85% of samples under
`KdbServerRuntime.SweepExpiredNow` → `ServerEngine.ScanDocuments` → `DocumentTree.Walk`**, mostly
`delta.(*DefaultReader).ReadCommitAt` (51%: commit frames re-read and zstd-decompressed per
document, plus CRC32 and SHA-256) and `prefetchWindow` (68% cumulative). So every minute the
sweep reads, decompresses and checks every document in the namespace to find the few that expired,
and user queries queue behind it (their 30 s client timeouts in the benchmark). This is consistent
with the benchmark: 127% CPU at a single user doing 0.4 actions/s.

Also **measured**: with `--expire-field` set, a collection-wide `COUNT(*)` or any wide query reads
every document body (rehearsal: 0.8-20 s at 27,600 documents depending on how they were written;
the namespace-wide count is what OOM-killed the server at 100k tasks; reading the same tasks by
key was fine, 13 s at 761 MiB). **Hypothesis:** the expiry filter is evaluated per row from the
parsed body, and `COUNT(*)` only uses index counts for a single `=` (`countByIndex`).

Do: keep each document's expiry deadline in an ordered index (or in the tree entry) so the sweep
visits only documents whose deadline has passed - a range read on that index, O(expired) not
O(namespace) - and "is it expired" is answered without the body; until then, at least make the
sweep yield to queries (bounded batch per tick, lower priority); serve `COUNT(*)` from index cardinalities/id sets whenever the WHERE is
index-servable (incl. AND of indexed `=`/`IN`, and the OR union from U3), checking expiry from that
index. Reproduce: qtask `tests/data-kdb-large-namespace.test.ts` plus a new KDB test that counts
100k bodies-heavy documents under `--expire-field` and asserts bodies were not decoded (counter or
heap bound). Accept: `SELECT COUNT(*) FROM docs WHERE _c = 'tasks'` at 100k tasks with embeddings
in < 1 s and < 200 MiB extra heap.

### U2. Memory accounting that actually bounds a query - M/L

**Measured:** queries are refused on an estimate ("1.79 GB > 1.56 GB budget" for one user's 8,136
tasks, about 1 GB on disk for the whole namespace), and others run unbounded until the kernel kills
the process (> 4 GiB with `--memory-budget-mb 1536`). Both are wrong in opposite directions.
**Hypothesis:** the admission estimate multiplies row count by a worst-case document size, while
the execution path materialises parsed documents and whole result frames without charging them.

Do: charge what a query really holds - parsed documents, sort buffers, frames being built - against
the budget as it runs, stream rows into result pages instead of materialising the result, and
refuse (or spill) only when the real charge would cross the budget. Estimate admission from index
cardinalities and observed average document size, not a worst case. Reproduce: the rehearsal's
heavy-user list (`scripts/rehearsal/run-migration-rehearsal.sh`, `--memory-budget-mb 1536`).
Accept: no OOM kill and no false refusal on that list; RSS stays within budget + a fixed margin.

### U3. Release the OR index union (#298) - S

#298 (Go and Kotlin) serves `a = ? OR b = ?` from a union of index lookups (20k docs: 26.6 ms to
4.5 ms). Merge, include in the next release. qtask then drops its client-side branch splitting
after a re-run of its pushdown tests against the release.

### U4. Cheap reads of many documents - M/L

**Measured:** with the qtask store fixed to fetch only the documents it needs, KDB's CPU is still
the bottleneck in the smoke benchmark (181% of 2 CPU at 8 users vs MongoDB 33%); task list p50
88 ms vs 9 ms. Rehearsal: commits of 4 MiB made later reads 10-20x slower than 64 KiB commits.
**Hypotheses** (profile first - `pprof` on `kdb-service` under `npm run bench:docker -- --scale
small --backends kdb --profiles read-heavy`): (a) cold reads decompress a whole multi-document
commit frame to return one document; (b) each returned document is parsed to evaluate the residual
WHERE and re-encoded as a JSON string for the `_doc` cell; (c) per-row allocation in the executor.

Do: whatever the profile says, in order of cost. Likely candidates: per-document addressable
storage (or a block index) so one document is decompressed alone; evaluate residual predicates on
the encoded document without a full parse; pass `_doc` through as stored bytes. Accept: qtask's
`read-heavy` cell at 8 users reaches at least half of MongoDB's actions/s on the same limits.

### U5. Fast open and restore - M/L

**Measured:** container start to first answered query 250 s at 517k documents (MongoDB 1.1 s), of
which ~220 s before the server accepts work; restart after `kill -9` 192-205 s; a namespace
restore needs its constraints and indexes declared again by the application, which took 1,293 s
at 110k documents; an offline whole-database backup restored with indexes opened in 74 s vs 2 s
namespace-only. **Hypothesis:** open replays or rebuilds what a checkpoint could carry (index
snapshots stale after an unclean stop, the tree segment written on first open).

Do: include index and constraint definitions (and current snapshots) in namespace backups and
checkpoints; on open, catch indexes up from the log tail instead of rebuilding from documents;
accept connections early and answer with `BUSY` + retry-after rather than refusing the socket.
Accept: open of a 500k-document namespace after `kill -9` in < 30 s; restore + first query in
< 2 min without the application re-declaring anything.

## Phase P1 - needed for a good production experience

### U6. Group commit for transactions - M

**Measured:** queue claims (read, then a conditional `replaceIf`) top out at 54/s with one worker
and fall to 6/s at 64 workers; MongoDB 297/s and 61/s. A single-document `replace` costs one fsync
each (22 ms on macOS) whatever the concurrency. `upsert` already shares fsyncs; transaction commits
(`replace`, `replaceIf`, multi-op commits) do not. Do: the same group commit for them. Accept: claim
throughput at 8 workers within 2x of MongoDB `j:true`.

### U7. Top-k `ORDER BY ... LIMIT` from an ordered index - M

qtask pages task lists by a sort (`LIST_SORT`) with `LIMIT 200`. Today the store must read every
candidate's sort fields to pick the page. Do: when the leading `ORDER BY` column has a BTREE index
(or the candidate set is small), produce the first `LIMIT` rows without materialising the rest;
document the sort semantics for missing/null/mixed types so a client can rely on them matching
MongoDB's for scalar fields. Accept: a 200-row page of a user with 8k tasks reads ~200 documents.

### U8. SQL and client gaps that force workarounds - S each

1. **Absent vs null:** a selected field returns NULL for both. Add `EXISTS(path)` / `IS MISSING`
   (or a typed-cell kind for "absent"). qtask stores a `_nulls` list per document to compensate.
2. **Typed paged queries:** `@kdb/client` has `queryValues` (typed) and `queryRawPages` (paged)
   but not both. Add `queryValuesPages`. qtask pages typed reads by hand with `kdb_id > ?`.
3. **Booleans as booleans:** a typed cell for a top-level boolean comes back `0`/`1` (nested
   booleans inside a JSON cell are fine). Return a boolean.
4. **Paths through arrays:** `SELECT subs.u` returns only the first element; return the array of
   values (it is multikey everywhere else, e.g. in `WHERE` and in indexes).
5. **Keyed `IN` after another conjunct:** `_c = ? AND kdb_id IN (...)` stops being a key lookup
   (2.7 s vs 6 ms for 100 ids at 27k documents). Keep the key path whatever the conjunct order.

### U9. Several namespaces in one `kdb-service` - M/L

qtask keeps every collection in one namespace because a standalone service hosts one; every scan
the indexes cannot serve then examines every collection, and `--scan-row-budget` (1,000,000
default) is reached by the namespace's total, not the collection's. Host several namespaces per
service (the embed API already can) with one connection, so a client can use a namespace per
collection.

## Phase P2 - release and operations

### U10. Deterministic tests on CI - S

Two release attempts failed on fixed timing deadlines that a slow runner misses
(`TestStreamFarBehindOverTheFrameLimitCatchesUp`: a 5 s wait that took 12 s under `-race`; earlier
`TestMemoryGuardStopWaitsForTheSamplerToActuallyExit`). Replace wall-clock deadlines with
progress-based waits or generous bounds, and audit the suite for the same pattern.

### U11. Distribution - S

- An **arm64** `kdb-service` image (v0.8.0's is linux/amd64 only; qtask's benchmark had to build
  its own from the release binary).
- Publish `@kdb/client` to npm (the release workflow skips it without `NPM_TOKEN`); qtask installs
  the release tarball by URL meanwhile.

### U12. Credentials beyond static pairs - M

Users can be provisioned (`kdb-service user create`, documented since v0.8.1), but a token is a
static `user:password` sent on every connection, and the control plane accepts any `Authorization`
without `--rbac`. Issue revocable tokens, and make the control plane refuse without `--rbac` unless
explicitly opened.

## Order and acceptance for qtask

P0 in the order U3 (merge only), U1, U2, U5, U4 - U1 and U2 remove the refusals and the OOM, U5 the
restart time, U4 is the long one. Then U6 and U8 (small and they delete qtask workarounds), U7, U9.
P2 can run alongside.

qtask's cutover gate, re-measured with `npm run bench:docker -- --scale prod` on a release that
carries P0: zero errors in every cell; mixed and read-heavy actions/s within **2x** of MongoDB
`j:true` at 8 and 32 users; no OOM and no `RESOURCE_EXHAUSTED` in the rehearsal; open after
`kill -9` and restore-to-first-query within the U5 targets.
