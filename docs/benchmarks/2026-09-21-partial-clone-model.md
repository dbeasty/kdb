# Partial clone vs filtered projection: a model

**Date:** 2026-09-21. **Question:** should Phase 8 of
[kdb-distributed-implementation-plan.md](../kdb-distributed-implementation-plan.md) be built? Phase 8
is a verifiable partial clone: commit format v2 with operation digests, and bodies fetched on
demand.

**Gate:** build it only if a partial clone costs less than half of what a filtered projection
(Phase 7) costs, resident or on disk, on at least one realistic document size, *and* a workload
needs replica-side writes into the source's history.

**Verdict: the gate does not pass.** On space, a partial clone can never beat a projection:
- It holds a strict superset of what a projection holds.
- The difference is a fixed overhead that grows with the source's size and history, not with what
  the replica needs.

Phase 8 is not built.

## Inputs

| Quantity | Value | Source |
|---|---|---|
| Document-trie bytes per entry | **~165 B** | Measured 2026-09-21 by building a `document.DocumentTree` of 10k and 100k random entries (heap delta after GC). Got 165 and 164 B/doc. Load average was ~9.4 at the time, which does not affect a heap measurement. |
| Commit-graph bytes per commit | ~519 B | [kdb-commit-graph-memory.md](../kdb-commit-graph-memory.md) |
| Selectivity `f` | 1%, 10%, 50% | fraction of the source's documents the replica needs |
| Body size `S` | 1 KB, 64 KB, 1 MB | per document |
| Source | N = 50,000 documents, C = 250,000 commits (5 writes per document) | illustrative |

## Model

**Partial clone** (holds the full graph and the full tree, plus the bodies it fetched):

    N·165 + C·519 + f·N·S

**Projection** (holds only the matching documents, plus a handful of its own commits):

    f·N·(S + 165)

**Difference** (partial clone minus projection):

    N·165·(1 − f) + C·519

This is positive for every `f < 1`, and it doesn't depend on `S`. For the source above it's about
**8 MB + 130 MB ≈ 138 MB**.

| S \ f | 1% | 10% | 50% |
|---|---|---|---|
| **1 KB** | projection 0.6 MB, partial 138.5 MB (**~230×**) | 6 MB vs 144 MB (**~24×**) | 29 MB vs 164 MB (**~5.7×**) |
| **64 KB** | 32 MB vs 170 MB (**~5.3×**) | 320 MB vs 458 MB (**~1.4×**) | 1.6 GB vs 1.74 GB (**~1.1×**) |
| **1 MB** | 500 MB vs 638 MB (**~1.3×**) | 5 GB vs 5.14 GB (**~1.03×**) | 25 GB vs 25.1 GB (**~1.01×**) |

The ratio is the partial clone's cost divided by the projection's. It never falls below 1, let
alone to 0.5.

**Disk goes the same way, only more so.** Under today's commit format, operations carry full
bodies. That's why the partial clone needs commit format v2 in the first place. Without it, a
partial clone's log would hold every body anyway.

## What a partial clone would buy, and where that need is met instead

The partial clone's advantage is not space:
- It can verify the source's commits.
- It can write into the source's history as a full peer of it.

Verification matters when the source isn't trusted. Projections trust their authenticated source
for content, and that's the right trade for an edge replica of a hub it already trusts.

The need to write back belongs to Phase 7.4 (the write-back outbox), which queues writes and
replays them at the source. That costs no format change, no dual-format DAG, and no
cross-language golden fixtures.

## If this is revisited

Re-run the model when any of these hold:
- The trie's per-entry cost or the graph's per-commit cost drops by an order of magnitude. The
  on-disk commit graph ([kdb-commit-graph-on-disk.md](../kdb-commit-graph-on-disk.md)) is the
  candidate that could do this.
- A deployment needs replicas that verify an untrusted source.
