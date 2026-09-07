# What a namespace costs to hold open, and why it used to be its whole history

Date: 2026-09-07. Machine: Apple M3 Max (16 cores), macOS 26.5.2, Go 1.26.3
(`darwin/arm64`), APFS internal SSD.

Reproduce:

```
cd go && go test ./kdb/embed/ -run TestOpenCostOfGrowingDocument -v -count=1
```

The workload is one document rewritten 463 times, gaining an entry each
time — a long-lived append-only aggregate (a game match, an audit trail, an
order with a growing line-item list). Nothing about it is exotic, and
nothing about the *live* data grows beyond a single document.

## The finding

Holding such a namespace open cost memory proportional to the arithmetic
sum of every version it had ever held, not to the data that was live.

| final document | on disk (zstd) | open churn | live heap, before | live heap, after |
|---:|---:|---:|---:|---:|
| 41.7 KB | 0.60 MB | 182 MB | 15.1 MB | 15.3 MB |
| 132 KB | 0.66 MB | 452 MB | 37.0 MB | 32.4 MB |
| 403 KB | 0.70 MB | 1181 MB | 99.9 MB | 35.5 MB |
| 1.40 MB | 0.88 MB | 3890 MB | **382.3 MB** | **25.7 MB** |

A 1.4 MB document cost **382 MB resident** — 284× its live size — and the
curve was quadratic in the rewrite count, so it got worse with every write
rather than settling. On a container sized for the live data (Zolik's KDB
service runs under a 1 GB cap) that is fatal, and it is fatal on a workload
whose live data is under two megabytes.

Held constant-size instead, so only the length of history varies, the
before/after shapes are:

| rewrites of a fixed 200 KB document | live heap |
|---:|---:|
| 50 | 12.2 MB |
| 200 | 27.4 MB |
| 800 | 35.3 MB |

16× the history now costs 2.9× the memory, flattening toward the configured
budget. It was linear before.

## Where the memory was

Two independent structures held the same bytes — the decoded text of every
version — and **neither could be fixed alone**. Removing either one moved
the measured total by 0.01 MB, because the other still pinned every
version. This is worth stating plainly because it makes the obvious
bisection method lie: nil out one retainer, observe no change, conclude it
is not the problem, and both times be wrong.

1. `storage/engine.shardedDocByHashStore` — every document version ever
   committed, keyed by content hash, with a comment stating that entries
   are never deleted so historical `atCommit` reads keep working.
2. `dag.InMemoryCommitDag.commits` — every commit, and a commit's
   operations carry the full text of each document it wrote.

## What changed

Both are now bounded, and both fall back to the delta log — the one place
every version is already durable — when they drop something.

- **Version store**: bounded by `storage.ResolvedDocumentCacheBytes` (half
  the hot-tier budget). Versions reachable from the *current* tree are
  pinned and never evicted, so every read of live data stays in memory;
  only history is evictable. `ServerEngine.ColdDocumentLoads` counts
  load-backs, so a budget too small for the live working set is visible
  rather than silent.
- **Commit operations**: bounded by `storage.ResolvedCommitOpsBytes` (a
  quarter of it), with branch heads, tags and reader pins never evicted.
  `GetCommitOrThrow` loads operations back transparently;
  `WalkWithOperations` is the walk that guarantees them.

Both loaders index content or commit hash to a *frame location* and read
one frame on demand, lazily building the index on first miss. Locations,
not documents — an index holding the text would reintroduce exactly the
retention being removed — and lazily, so a namespace nobody reads history
of pays nothing.

## Second pass: a checkpoint, so open stops reading history at all

Bounding memory fixed what a namespace *held*; it did not change the fact
that open reconstructed current state by decoding every commit ever
written. A checkpoint - the commit graph, the refs, the live tree, and the
branch heads' own operations - is now written at clean close and
immediately after any open that had to replay in full, and read in place of
the log on the next open.

Same workload, measured across the three states:

| final document | churn: original | after bounding | after checkpoint | live heap now |
|---:|---:|---:|---:|---:|
| 41.7 KB | 310 MB | 182 MB | **4.7 MB** | 2.6 MB |
| 132 KB | 838 MB | 451 MB | **7.0 MB** | 3.1 MB |
| 403 KB | 2299 MB | 1181 MB | **13.8 MB** | 4.0 MB |
| 1.40 MB | 7708 MB | 3890 MB | **38.6 MB** | 7.3 MB |

200x less allocation to open than where this started, and 382 MB resident
became 7.3 MB. A direct A/B on the same store with only the checkpoint file
removed (`TestCheckpointMakesOpenSkipTheLog`) reads 3.08 MB against
135.91 MB.

The checkpoint costs disk: it carries the head commits' payloads, so the
data directory for the 1.4 MB case grew from 0.88 MB to 2.31 MB. That is
proportional to live data, not to history, which is the trade being made
everywhere else here too.

### What keeps it honest

- **The log is still the authority.** Anything wrong with a checkpoint -
  absent, corrupt, wrong format version, wrong namespace - falls back to a
  full replay. It is a cache and nothing else.
- **It cannot mask damage.** A checkpoint fingerprints every segment it
  claims to cover (sequence, size, last commit). If the log no longer
  matches, the checkpoint is discarded and the log is replayed, which is
  what surfaces the damage. Without this, truncating a sealed segment
  produced a clean open and silently missing history.
- **A crash still costs nothing.** Writing a checkpoint immediately after
  a full replay means a process that is killed - the ordinary end of a
  container - still leaves one behind, so the expensive open pays for the
  next one rather than repeating forever.
- **The tail is always replayed.** A checkpoint only ever claims segments
  below the one the writer has open, so commits written after it come back
  from the log (`TestCheckpointReplaysTheTail`).

## What this still does not fix

**Historical trees are rebuilt by scanning.** A checkpoint carries the live
tree only. Reading at a historical commit therefore replays the whole log
once, hashing every document, to recover the docID-to-content-hash mapping
each commit had. It is lazy (a namespace only ever read at its head never
pays it) and it happens once per process - but it is a full scan.

Git does not have this problem, because git *stores* trees: a tree object
is content-addressed, on disk, and shared by hash between commits, so
reading at an old commit is commit then tree then blob. KDB's delta log
records commits with full document text but no tree objects, so the mapping
exists only in memory and can only be re-derived by re-hashing everything.
Persisting trees as objects removes the rebuild entirely, and is the next
piece of work.

**`treesByHash` is still unbounded.** Every `DocumentTree` produced in a
session is retained. Trees are small - 3 MB against 329 MB in the heap
profile above, because the persistent trie shares structure between
versions - so this is not the binding constraint, but it has the same shape
and no bound.

## A correction on the storage side

This work started from a report that the delta log was 66.7× larger than
git storing the same 463 versions (22.39 MB against git's 0.34 MB packed).
That does not reproduce against the current default. With zstd on — the
default in `embed.OpenFileRuntime` — 463 versions of a 1.4 MB document
occupy **0.88 MB on disk, less than the live document**, which beats git's
loose objects and is within ~2.6× of git after `gc`.

Compression toggled on the same workload:

| final document | zstd | none |
|---:|---:|---:|
| 41.7 KB | 0.60 MB | 9.52 MB |
| 132 KB | 0.66 MB | 30.01 MB |

22.39 MB sits on the uncompressed curve, so that measurement was almost
certainly taken with compression off. Content-addressed dedup and
delta-encoding between versions remain real gaps — kdb stores each version
whole where git deltas it against its neighbours — but on top of zstd the
incremental win is modest, and it was never the reason a small document
exhausted a 1 GB container. Memory was.
