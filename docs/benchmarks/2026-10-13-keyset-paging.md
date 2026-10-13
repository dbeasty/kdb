# Keyset paging for SQL continuations

Date: 2026-10-13. Base: `origin/main` 8251286, with this branch's benchmark file copied in. The branch was then
rebased onto 2733e4c; the paging code is unchanged by the rebase. Machine load average 3.9–6.1. The run was
interleaved base/new 3×2 under the shared benchmark lock.

`BenchmarkPageThroughScan` reads a whole namespace through a paged SELECT, pages of 500, over the wire; one op is
every page, start to end.

| benchmark | time, base | time, keyset | B/op, base | B/op, keyset | allocs/op, base | allocs/op, keyset |
|---|---|---|---|---|---|---|
| `SELECT *`, 5k docs | 20.4 ms | 15.6 ms (~, p=0.065) | 11.2 MiB | **8.4 MiB (−25%)** | 114k | **91.5k (−20%)** |
| `SELECT *`, 20k docs | 149 ms | **61 ms (−59%)** | 81.3 MiB | **29.7 MiB (−63%)** | 757k | **366k (−52%)** |
| `WHERE n >= 0`, 20k docs | 969 ms | **79 ms (−92%)** | 1,069 MiB | **53 MiB (−95%)** | 13.9M | **687k (−95%)** |

Paging by offset re-reads every row before each page, so reading a table in pages costs O(n²/page). With a WHERE
clause, the residual is also evaluated again for every skipped row on every page. With keyset paging, each row is
read once.

The point-read hot path, `BenchmarkWorkloadRead`, was unchanged in a first run (176 B/op, 4 allocs/op, all p ≥ 0.09).
The `WireSelect` rows from that run are not reported: the memory guard of the shared `benchGovernedWireServer`
helper sampled the whole benchmark process, which already held every earlier case's namespace, and refused the
queries. `BenchmarkPageThroughScan` therefore uses its own server without a memory limit.
