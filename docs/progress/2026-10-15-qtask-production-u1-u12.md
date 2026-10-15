# qtask production plan: U1–U12

Branch `feat/qtask-production`. This works through every unit of `docs/kdb-qtask-production-plan.md` (PR #301) in one branch, one commit per unit or group of units. Every change is additive: new paths, options and functions; existing behaviour is kept unless the unit asked for it to change.

## How it was measured

`go/cmd/kdb-qtask-sim` (new) reproduces qtask's workload over the real wire protocol:
- **Dataset:** shaped like qtask's. One namespace, every collection tagged by `_c`, tasks with 768-number embeddings, a skewed user distribution, a TTL collection.
- **Indexes:** the 38 that qtask declares.
- **Statements:** the SQL qtask's store issues (lean reads paged by `kdb_id`, keyed `IN` reads, paged scans, counts, conditional replaces).

The numbers below come from a 20,000-task dataset (116,400 documents, 347 MB of JSON):
- `kdb-service --durability sync --expire-field _expireAt`
- loaded through the wire in commits of 200 documents, as qtask's backfill does
- run on a shared macOS laptop (load 5–15)

They compare `main` (`e899e4f9`, v0.8.2) with this branch on copies of the same data directory, written by `main`. They are not qtask's 2-CPU Docker numbers; qtask should re-run its bench with `KDB_SERVICE_BIN` pointed at this build.

```
kdb-qtask-sim load  -addr HOST:PORT -tasks 20000 -user-count 400 -skew 0.9
kdb-qtask-sim probe -addr HOST:PORT -tasks 20000 -user-count 400 -skew 0.9
kdb-qtask-sim bench -addr HOST:PORT -tasks 20000 -user-count 400 -skew 0.9 -profile mixed -users 8
```

| statement (heaviest user: 2,278 tasks) | main | this branch |
| --- | ---: | ---: |
| `COUNT(*)` of tasks (20k), under `--expire-field` | 3 m 39 s | 1.5 ms |
| `COUNT(*)` of the namespace | 16 s | 0.2 ms |
| unread-notification count (exact, indexed) | 25 s | 0.7–4 ms |
| lean read of the user's accessible tasks | 26 s | 144 ms |
| keyed read of 200 tasks | 2.3 s | 28 ms |
| whole-document scan of the user's tasks | 35 s | 0.5 s |
| task list (2 lean reads + 200 keyed) | 50 s | 0.3 s |
| **mixed workload, 8 users** | **1.1 actions/s** | **121.6 actions/s** |
| queue claims, 1 / 8 / 64 workers | — | 29 / 182 / 164 per s |
| restart after `kill -9` until listening | 47–54 s | 2.1 s |

## What landed, by unit

- **U1:** expiry and `COUNT(*)` without reading bodies.
  - A system BTREE index (TIMESTAMP keys) on `--expire-field` nominates the expired documents. Their bodies are read and confirmed, but nothing else is read.
  - `COUNT(*)` is answered from index id sets for an AND of exact equalities and `IN` lists, corrected for the hidden documents; anything else is counted by streaming.
  - **Found along the way:** the expiry sweeper read every document in the namespace every minute. It now uses the index.
- **U2:** memory accounting.
  - Admission sizes a query from its plan: index candidates (`sql.CandidateEstimator`), page size, LIMIT, nothing at all for `COUNT(*)`, and `ORDER BY kdb_id` as a stream.
  - A grant is capped at half the node's capacity instead of being refused.
  - What a query really holds is charged as it runs (`QueryContext.RetainedLimit`). Past that limit it fails `RESOURCE_EXHAUSTED` instead of the process being OOM-killed.
- **U3:** #298 was already merged. It still needs a release.
- **U4:** cheap reads of many documents. The profile showed 98% of the time decompressing whole commit frames.
  - **Bulk-commit bodies:** a commit that writes 64 KiB or more of bodies files each document's body by value in the blob store, instead of a pointer into its multi-megabyte frame. That record shape was always readable. A cold read that meets a large frame re-files that frame's documents once, which repairs existing data.
  - **One-document commits:** these keep their pointer, so there is no second copy.
  - **Field-at-a-time decoding:** predicates and projections decode only the top-level members they touch, never the embedding.
  - **Early stop:** id-ordered pages stop at their LIMIT.
- **U5:** fast open and restore.
  - **Index catch-up:** after an unclean stop, indexes are caught up from their snapshots by diffing document trees, instead of being rebuilt from every document.
  - **Definitions in backups:** control-plane namespace backups carry the namespace's definitions (schema, unique constraints, indexes). `kdb-inspect restore` and the control-plane restore leave them in place, and `kdb-service` adopts them at its first open.
  - **Faster builds:** missing indexes are built in one scan, and every index shares one parse per document (also on the commit path).
- **U6:** group commit already covered transaction commits (`runTransaction` releases the write gate before the fsync wait). Measured: 182 claims/s at 8 workers and 164 at 64, with no collapse. The collapse qtask saw was the claim's lean read of every pending job, which U4 fixed.
- **U7:** `ORDER BY … LIMIT` keeps only the best LIMIT+OFFSET rows as candidates stream. An oracle test checks it against the full sort.
- **U8:**
  - `IS [NOT] MISSING` and `EXISTS(path)` tell an absent field from a stored null.
  - `ALL_VALUES(path)` returns every value a path through an array reaches. A bare column still projects the first value, per spec §3.5.
  - A projected JSON boolean is typed BOOL; its wire text is still `1`/`0`.
  - `QueryValuesPages` (Go) and `queryValuesPages` (TS) give typed, paged reads.
  - Keyed `IN` after another conjunct already held on main.
- **U9:** `kdb-service` already serves several namespaces over one connection, with their own indexes. `--expire-namespaces` extends expiry to namespaces opened on demand.
- **U10:** `internal/testtime.Scale` multiplies progress waits by 10 under `-race`. It is applied to the shared wait helpers and the tightest single sites an audit found; about 130 short bounds remain, mostly borderline. The two tests that failed releases were already fixed on main.
- **U11:**
  - **Image:** published for linux/arm64 as well as amd64. The Go build cross-compiles, and a local arm64 build was verified.
  - **npm:** publishing still needs the `NPM_TOKEN` repository secret.
- **U12:**
  - **Tokens:** revocable access tokens (`kdbt_…`) are issued per client and carry the user's roles. They can expire, and the registry keeps only their SHA-256. Manage them with `kdb-service token create|revoke|list`.
  - **Control plane:** without `--rbac`, it now refuses a non-loopback address unless `--control-open` is given.

## Not done / follow-ups

- **Kotlin parity** for the SQL additions (`IS MISSING`, `EXISTS`, `ALL_VALUES`, top-k) and for access tokens. qtask runs against the Go service.
- **U5 "accept connections while opening, answer BUSY":** not done. Open after a crash is now about 2 s at this scale.
- **Atomic server-side claim** (`UPDATE … ORDER BY … LIMIT 1 RETURNING`): not done. It would remove qtask's claim retries under contention.
- **U7 with a BTREE-driven top-k** (reading about LIMIT documents): not done. The order would have to match `CompareCells` for NULLs, mixed types and arrays exactly.
- **Large-commit bodies cost disk:** filing bodies by value stores bulk-written documents twice (log and blob store). `StorageEngineConfig.LargeCommitBodyBytes` / `KDB_LARGE_COMMIT_BODY_BYTES=-1` turns it off.
