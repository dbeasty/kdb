# Scan cap: finish the fix

Handoff written 2026-10-11. The Go server fix is committed on branch `fix/scan-cap-before-filter`
(`27336e1`, based on `d35d044`) and has **not** been pushed, reviewed or released. This document is
everything the next agent needs to land it and finish the work around it. It follows the unit
conventions of `docs/kdb-open-issues-plan.md` (one branch and PR per unit, reproduce before fixing,
update the progress log, work in a worktree).

## The defect

Found while porting qtask (a Mongo application, `~/devel/qtask`) onto KDB. In v0.6.2 a non-indexed
scan collected the first `MaxRows` (10,000) document ids and **then** ran the residual `WHERE`
(`go/kdb/sql/executor.go`, `fullScan`). On a namespace with more documents, with no error:

| Statement | Result on v0.6.2 with 12,000 documents |
|---|---|
| `SELECT COUNT(*) FROM docs` | `10000` |
| `SELECT kdb_id FROM docs WHERE n >= 11990` | 0 rows (should be 10) |
| `SELECT kdb_id FROM docs WHERE kdb_id = '<doc 11999>'` | 0 rows |
| `DELETE FROM docs WHERE kdb_id = '<doc 11999>'` | reports success, deletes nothing |

Index lookups (`indexLookup` path in `chooseAccessPath`) were truncated at `MaxRows` before the
residual filter in the same way. `kdb-service` opens exactly one namespace, so every collection a
client keeps shares the 10,000.

Reproduce on the release binary (`dist/bin/kdb-service-<os>-<arch>` at v0.6.2), any client:

```
kdb-service --memory --namespace c/docs --peer-addr "" --stream-addr "" \
            --sql-addr "tcp://127.0.0.1:7821?bind=true"
# upsert 12,000 documents {"_c":"t","n":i} with ids 00000000-0000-0000-0000-<i, 12 digits>
# then run the four statements above
```

`--peer-addr ""` and `--stream-addr ""` matter: v0.6.2 binds `:9091` and `:9092` by default, so a
second instance on a host fails to start.

## What is done (commit `27336e1`)

- `fullScan(ctx, stopAt)`: reads every document unless `stopAt > 0`; work is bounded by
  `RowBudget`, as before. `MaxRows` is no longer applied to ids.
- `ExecuteSelect` computes `stopAt` only when nothing runs after id resolution and nothing needs
  every row (no residual, `ORDER BY`, `DISTINCT` or grouping): `offset + min(limit, MaxRows+1)`.
- Index lookups are no longer truncated.
- A result with more than `MaxRows` rows is `*sql.ResultRowLimitError`, classified
  `RESOURCE_EXHAUSTED` (`server/error_classification.go`). Chosen over truncation deliberately: a
  client given the first N rows of an answer cannot know it is incomplete.
- Tests: `go/kdb/sql/scan_result_bound_test.go` (selective filters and ranges past the bound,
  `COUNT(*)`, limit/offset, overflow error, exact-bound result). `go test ./...` passes.

Not yet done on that commit: confirming the new tests **fail on `main`** (revert the executor change
once, locally). They were written against the fix and the pre-fix behaviour was observed through
qtask, not through these tests.

## Units

### U1. Land the Go fix — S

1. In a worktree: `git fetch && git rebase origin/main` on `fix/scan-cap-before-filter`.
2. Revert only `go/kdb/sql/executor.go`, run `go test ./kdb/sql/ -run 'Bound|PastMaxRows|NamespaceNotAPrefix'`,
   confirm the new tests fail, restore. Record it in the PR.
3. `cd go && go test -race ./...` (check `uptime` first).
4. PR. State the behaviour change plainly in the description and the release notes: **a query whose
   result exceeds `MaxRows` now fails with `RESOURCE_EXHAUSTED` instead of returning the first
   `MaxRows` rows.** Anything that relied on the silent cut breaks loudly; that is intended.
5. Append to the progress log in `docs/kdb-open-issues-plan.md`.

### U2. Make the result bound configurable — S/M

The bound is the literal `10_000` in `server/wire_listen.go` (two `QueryContext`/`ScanEstimateInput`
sites, ~lines 619 and 639) and `server/conflict_procedure.go:286`; `sql/executor.go` has the
default `defaultMaxResultRows`. After U1 a legitimate read of more than 10,000 rows (for example an
application listing a whole collection) can only fail. Add `--max-result-rows` (flag, `KDB_*`
environment variable and config-file key, matching how `--scan-row-budget` is wired through
`config.ServiceFile`), default 10000, plumbed to both wire sites and to
`ScanEstimateInput.MaxRows`, because the admission cost model (`server/costmodel.go`, "streaming
plans retain at most the result bound") sizes its memory grant from it. Test: a result of
`MaxRows+1` rows errors, raising the flag makes it succeed, and the memory estimate scales.
Document the interaction with `--memory-budget-mb` in the flag help.

### U3. Do not present RESOURCE_EXHAUSTED as "busy" — S

The TypeScript client folds `RESOURCE_EXHAUSTED` into `KdbBusyError` (`packages/kdb-client/src/errors.ts`,
~line 195), so the new error reads "kdb: server busy: ... add a LIMIT" and is treated as retryable.
Retrying an over-large query returns the same error forever (the server's own comment in
`error_classification.go` says exactly this for the row budget). Give it its own non-retryable class
(or flag), keep `BUSY` retryable, and check the Go client SDK and `backoffDelay` for the same
conflation. Test with a stubbed reply carrying `RESOURCE_EXHAUSTED`.

### U4. Kotlin parity — M/L

The spec requires both trees to agree (`kdb-spec-layer16-qtask-gaps.md`, "Decisions taken"). The Kotlin
engine applies `take(context.maxRows)` to results and to id lists in
`kdb-sql/src/commonMain/kotlin/dev/kdb/sql/SqlExecutor.kt` (about lines 51, 168, 287 for rows; 382
`Limit`, 394 `ScoredScan`, 407 `inListScan`, 420-422 index range and full-text, 438 `fullScan` for
ids) and has its own `maxRows` default in `SqlModel.kt:276` and
`kdb-hybrid-query/.../HybridQueryTypes.kt:29`. The id-list truncations before the residual filter
are the same defect. Port the Go semantics:

- `fullScan` and `inListScan`/`indexScan` return every id; work bounded the way Go bounds it
  (check whether Kotlin has a row budget; if not, say so rather than inventing one).
- Rows beyond `maxRows` raise a new `SqlResultLimitException` (name to match the existing
  `SqlPlanningException` family) instead of `take`.
- Add a **shared fixture** so both trees assert the same cases: a small `maxRows` over a larger
  namespace, the same queries and expected outcomes as `scan_result_bound_test.go`. Put it under
  `go/testdata/golden/` alongside the search fixtures and have both suites read it.
- `./gradlew build --no-daemon` (U1 of the open-issues plan makes that green).

### U5. Ranked paths: decide and test — S

`rankedIDs(hits, max)` (Go) and the `ScoredScan` `take` (Kotlin) cut a ranking at `MaxRows` before
a residual `WHERE`. A ranking is naturally top-N, so a cut is defensible, but `MATCH(...) AND
status = 'open'` can lose matches that rank below the cut and satisfy the filter. Decide the
semantics (candidate depth bounded by the existing `4*(n+m)` rule when there is a `LIMIT`; an
error, not a silent cut, when there is none and the ranking exceeds the bound), write the test that
shows today's behaviour, then change both trees together.

### U6. Server-level tests for DML past the bound — S

`UPDATE`/`DELETE` resolve their targets through `matchingDocs` (`stopAt = 0`), so they were
affected too, and qtask observed `DELETE ... WHERE kdb_id = ...` silently doing nothing. Add wire-level
tests (a real listener with a small `MaxRows`, as `scan_row_budget_test.go` does for the budget) that
`UPDATE` and `DELETE` act on a document beyond the bound and report the true `rowsAffected`.

### U7. Planner: serve `kdb_id = ?` and `kdb_id IN (...)` by key — M, optional but valuable

With no declared schema every predicate is a full scan, including `WHERE kdb_id = '<uuid>'`, which
is a keyed lookup the storage layer can already answer (`GetDocument`). Add it as an access path in
`chooseAccessPath` / `QueryPlanner` (label `keylookup` in `QueryResult.Plan`). This turns every
application `UPDATE`/`DELETE` by id from O(namespace) to O(log n). Related, larger, and separate:
hash/btree indexes on schemaless namespaces (today they require declared fields, which a
many-collection namespace cannot provide).

### U8. TS client: delete by id — S, optional

`packages/kdb-client` `Transaction` carries only `write` operations although the wire format has
`DeleteOp`. SQL `DELETE` works again after U1, so this is not blocking; it would let a client delete
without a scan. Add `deletes: string[]` (or a `DocDelete`) to `Transaction`, encode `{kind:"delete"}`,
and cover it in the client's interop sweep.

## Contract with qtask (do not break)

qtask pins a KDB version and has tests that run against a real `kdb-service`. After this work lands
and is released, the qtask side bumps the pin and retires its workaround; that is a qtask change,
not a KDB one. What qtask depends on:

- `tests/data-kdb-scan-cap.test.ts` loads 10,050 documents and detects the server's behaviour by
  itself: against a truncating server it checks that qtask refuses to scan and that deletes past
  the cap fail loudly; against a fixed server it checks that selective queries and deletes work
  past 10,000. Point it at a build with `KDB_SERVICE_BIN=/path/to/kdb-service`. It passes today
  against both the v0.6.2 release binary and a build of `27336e1`.
- qtask's store refuses to answer when `COUNT(*)` equals exactly the cap (`KDB_SERVER_ROW_CAP`,
  default 10000), which only a truncating server can report. Do not change `COUNT(*)` so that it
  reports exactly `MaxRows` for larger namespaces.
- qtask needs `RESOURCE_EXHAUSTED` for over-large results to stay `RESOURCE_EXHAUSTED` (U3) and a
  way to raise the bound (U2); without U2 an unfiltered read of a collection above 10,000
  documents cannot work, with or without pushdown.
- The qtask migration plan is `~/devel/qtask/docs/KDB_MIGRATION.md`; blocker B1 there is this
  document's subject. Update its status when a release containing U1 exists.

## Open questions for the owner

1. Is a hard error the right behaviour for an over-large result, or should the server page (a
   continuation token)? The plan assumes an error because it is the smallest honest change.
2. Should `RowBudget` (default 1,000,000 examined rows) also apply to `fullScan`'s id collection
   when `ResidualWhere` is nil? Today the id slice is unbounded in memory below that budget
   (about 16 MB per million ids), which looks acceptable.
3. Release: this changes observable behaviour, so v0.6.3 or v0.7.0?

## Progress log

_(append one entry per unit: date, unit, PR, commit, and anything found along the way)_

- 2026-10-11 — Go fix committed on `fix/scan-cap-before-filter` (`27336e1`), not pushed. Found by
  qtask; reproduced and verified there against the release binary and the fixed build.
