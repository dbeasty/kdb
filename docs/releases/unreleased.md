# Unreleased

## Performance

**Indexes no longer slow every write down as they grow.** The Go server wrote every index's
snapshot to disk — all of them, in full, each file fsynced — synchronously under the write gate
on every 64th index-touching commit, and twice for every `CREATE INDEX`. A commit's cost
therefore grew with (number of indexes × their total size): with 27 indexes the 64th commit
stalled every writer for over half a second, and creating 27 indexes on an empty namespace took
15–21 seconds because the *k*-th `CREATE INDEX` rewrote all *k* indexes twice.

Snapshots are now written off the write path. A commit only updates the in-memory indexes; a
background flusher writes the snapshots once the namespace has gone quiet for a second, at most
one flush at a time and never more than a tenth of the time, and only for indexes that changed
since they were last written (an unchanged index has just its manifest moved to the new head).
`CREATE INDEX` and `DROP INDEX` write only the small catalog synchronously. A graceful shutdown,
an idle namespace close and the host's close all write the snapshots a last time, so a clean
restart still opens without rebuilding.

Crash safety is unchanged: an index whose manifest does not name the head it opens at is rebuilt
from the documents before it serves a query, so a `kill -9` at any moment — mid-commit or
mid-flush — costs a rebuild, never a wrong answer. The snapshot's directory is now synced before
its manifest is renamed into place, so after a power loss a manifest can never name a head whose
snapshot did not reach the disk, and re-creating a dropped index at the same head can no longer
pick up the dropped index's snapshot.

Measured in-process (`TestIndexWriteCostOnDisk`, `KDB_INDEX_PERF=1`) on a disk-backed namespace
with `--durability sync`, 27 single-path indexes (HASH, every third BTREE), 2,000 sequential
single-document commits then 150 concurrent ones; Apple M3 Max, macOS, two runs each:

| | v0.7.1 | now | no indexes |
| --- | --- | --- | --- |
| `CREATE INDEX` ×27, empty namespace | 21.2 s, 14.9 s | 0.36 s, 0.31 s | — |
| sequential writes, docs/s | 54, 64 | 97, 111 | 100–133 |
| per-commit p99 | 662 ms, 584 ms | 18 ms, 16 ms | 11–20 ms |
| 150 concurrent writes | 1.26 s, 1.39 s | 0.11 s, 0.06 s | 0.05–0.13 s |

The machine's fsync latency drifts between sessions (a later run of the same test measured 177–182
docs/s with 27 indexes against 187–193 without, `CREATE INDEX` ×27 in 0.14–0.17 s), so compare
columns within a row rather than across runs. With 27 indexes, writes now cost what they cost
without indexes, within that noise; the remaining per-commit time is the fsync of the commit
itself.
