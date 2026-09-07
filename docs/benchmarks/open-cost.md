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

## Third pass: two history strategies, chosen per namespace

A checkpoint carries the live tree, not the tree every past commit had.
Recovering those is its own problem, and it has two reasonable answers with
opposite cost shapes - so both exist, and a namespace records which one it
was built with (`storage.HistoryStrategy`, `KDB_HISTORY_STRATEGY`).

**`replay`** writes nothing extra. The first read at a historical commit
replays the delta log and re-hashes every document to recover the mapping.
Free on the write path, expensive once on a read that touches history,
nothing at all for a namespace only ever read at its head.

**`objects`** is git's answer: each commit's tree is recorded under that
tree's own hash, and each document version under its content hash, so a
historical read is commit → tree → document, three lookups. The default for
new namespaces.

First read at the oldest of 200 commits, same workload, same machine:

| strategy | allocation for that read |
|---|---:|
| replay | 200.95 MB |
| objects | **0.85 MB** |

The disk it costs, 463 rewrites of one document:

| final document | replay | objects |
|---:|---:|---:|
| 9.9 KB | 0.32 MB | 0.58 MB |
| 1.37 MB | 1.93 MB | 2.50 MB |

Cheaper than it looks because SSTable blocks are compressed and successive
versions of one document compress well against themselves.

### Why a tree object is a delta, not a tree

A KDB document tree is flat - every document hangs off the root - so a
whole-tree object costs O(documents) per commit. Git avoids that by nesting
trees per directory; there are no directories here. Persisting the
in-memory Merkle trie's nodes instead is worse for the common case: that
trie is fixed-depth 32 with no path compression, so touching one document
creates 32 internal nodes of 513 bytes - about 16.5 KB per write whatever
the document's size.

So a tree object records the entries that changed, against the previous
tree, with a full object every 32 commits to bound how far a read walks
back. Resolution is checked against the hash it was looked up under, which
is exact for content-addressed data: a truncated chain or an encoding bug
produces a miss and a fall back to the log, never a wrong answer.

### The two are not interchangeable on one directory

A namespace built under `objects` has tree and document objects nothing
else writes; one built under `replay` has none. Opening either as the other
is refused with `HistoryStrategyMismatchError`, which names the conversion:

```
kdb-inspect migrate-history --data-dir DIR --namespace NS --to objects|replay
```

That runs offline under the data directory's exclusive maintenance lock,
because it rewrites both what the namespace claims about itself and the
objects backing that claim - a writer appending commits through the middle
would leave the two disagreeing. A namespace with no marker at all predates
the setting and is `replay`, whatever the caller asks for, because that is
what its bytes actually support.

## The settings

Everything above is a per-namespace initialization setting rather than a
built-in behaviour, on `embed.FileRuntimeOptions.Storage`:

| setting | env | default | what it trades |
|---|---|---|---|
| `HistoryStrategy` | `KDB_HISTORY_STRATEGY` | `objects` for new namespaces | write cost against historical-read cost |
| `DocumentCacheBytes` | `KDB_DOCUMENT_CACHE_BYTES` | half the hot-tier budget | resident memory against cold reads |
| `CommitOpsBytes` | `KDB_COMMIT_OPS_BYTES` | a quarter of it | the same, for commit operations |
| `TreeChainLimit` | — | 32 | bytes written against how far a historical read walks back |
| `DisableCheckpoints` | `KDB_CHECKPOINTS=off` | off (checkpoints on) | open cost against not writing one |

`DisableCheckpoints` also stops a checkpoint being *read*, not just
written: a namespace with the setting off must actually open from the log,
or turning it off would change nothing until the next write.

## What this still does not fix



**`treesByHash` is still unbounded.** Every `DocumentTree` produced in a
session is retained. Trees are small - 3 MB against 329 MB in the heap
profile above, because the persistent trie shares structure between
versions - so this is not the binding constraint, but it has the same shape
and no bound.

**Under `objects`, a version's text is on disk twice**, in the delta log
and in the object store. Making the log prunable once its objects are
written is the change that would remove that, and is not attempted here.

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
