# Compressing the document trie: ~4,900 bytes per document down to ~160

Date: 2026-09-08. Machine: Apple M3 Max (16 cores), Go 1.26.3 (`darwin/arm64`), verified idle
(83.9% CPU idle) before each measurement.

Removes the constraint recorded in
[`2026-09-08-base-tree-pinning.md`](2026-09-08-base-tree-pinning.md) and
[`2026-09-08-workload-matrix-after-pinning.md`](2026-09-08-workload-matrix-after-pinning.md):
the live document tree cost about 4,900 bytes of heap per document, linear and independent of
document size, which bounded how large a namespace could be held open at all.

## The defect

`DocumentTree` hashes a 16-ary trie over a UUID's 32 hex nibbles. Every entry lived at depth 32
with no path compression, so inserting one document allocated a private spine of 32 internal
nodes, each carrying a 16-pointer array plus a 32-byte hash. The nodes were shared between
*versions* of the tree - that is what makes commits O(1) - but not between *entries*, so the
cost was paid once per document and never recovered.

## The fix, and why it changes no hash

A leaf now stands for the whole subtree beneath its position as soon as that subtree holds
exactly one entry, at whatever depth that becomes true, instead of only at depth 32.

The hash is untouched, and that is the point. A subtree holding one entry has a determined
hash: fold `leafHash` up through the single-child internal nodes it would have had. `foldLeaf`
evaluates that fold instead of building it out of nodes, so a compressed leaf carries exactly
the hash its 32-node spine would have produced. **Every tree hash this package has ever emitted
is unchanged**, which is what makes this an internal representation change rather than an
on-disk format break - stored tree objects, delta logs, checkpoints, the golden vectors and the
Kotlin implementation all stay valid untouched.

Two halves are easy to get wrong and both are covered by tests:

- **Splitting.** When a second entry arrives beneath a compressed leaf, only the levels the two
  keys genuinely share become real nodes. For random UUIDs that is a handful of levels at any
  realistic *n*, rather than 32.
- **Re-compressing on delete.** A subtree collapsing back to one entry has to become a leaf
  again. Without it the spines grow back one deletion at a time in a namespace that churns.

CPU is a wash by construction: the fold performs the same SHA-256 rounds the old insert
performed on its way back up. What disappears is the allocation of the nodes, not the hashing
of them.

## Memory

Building a `DocumentTree` entry by entry and reading `HeapAlloc` after two `runtime.GC()` calls:

| documents | before | after | reduction |
|---:|---:|---:|---:|
| 10,000 | 48.5 MB | **1.6 MB** | 30x |
| 50,000 | 237.6 MB | **7.5 MB** | 32x |
| 200,000 | **932.7 MB** | **31.0 MB** | 30x |

About **4,900 -> 162 bytes per document**, still linear but with a constant 30x smaller. A
200,000-document namespace now fits a 1GB container with room to spare, where the tree alone
previously did not fit.

## Throughput

No cost, measured on `BenchmarkWorkloadWriteInsert/heavy-multi-user`, default benchtime,
`-count=5`:

| | ops/sec |
|---|---:|
| `6d2b4bb` baseline | 30,051 |
| before compression | 32,431 (n=3 median) |
| after compression | **29,030** (n=5 median; min 16,969, max 30,890) |

Within the spread of this row, which is wide. Allocation per operation is materially better -
159 allocs/op against 210-280 before, and 14KB/op against 17-24KB.

The configuration that previously could not complete now does:

| `-benchtime` | before | after |
|---|---|---|
| default | 32,431 ops/sec, passes | 29,030 ops/sec, passes |
| 3s | **fails in 96s** | **passes in 86s** (1,292 ops/sec at 108,709 documents) |

## What this does not fix

At `-benchtime 3s` the row now completes but reports 1,292 ops/sec, having grown the namespace
to 108,709 documents. Per-operation cost still rises with namespace size, so **something else is
O(namespace) on the write path** - the tree-object full-object write is the obvious candidate,
being O(documents) by construction. That is a separate investigation and it is no longer a
memory wall; the run finishes.

Two further reductions are available in the trie itself if it becomes the constraint again:

- Internal nodes still carry a fixed `[16]*trieNode` array (128 bytes) however few children they
  have. A bitmap plus a compact slice would cut most of that, at the cost of a more intricate
  `internalHash`.
- Nothing here shares structure *between* sibling entries, only between versions.

Neither is worth doing on the current numbers.

## Both implementations

Kotlin carried the identical defect - `trieInsertAt` recursing to `TRIE_DEPTH` and allocating a
16-element array at every level - and now carries the identical fix. Its `TrieNode` previously
held only a hash and its children, so a compressed leaf needed the entry's uuid and content hash
added to it before splitting was expressible at all.

The two implementations must stay in step or they silently compute different tree hashes for the
same entries, which `DocumentTreeTrieParityTest`'s literal vectors exist to catch. Since neither
side's hashes changed, those vectors are untouched and passing on both.

## Tests

`go/kdb/document/document_tree_compression_test.go`:

- `TestTreeMemoryPerDocumentStaysBounded` - asserts bytes per document against a 600-byte
  ceiling, so it fails on the defect (cost scaling with trie depth) rather than on the machine's
  heap size. The defect measured ~4,900.
- `TestCompressedLeafHashesMatchFullDepthSpine` - reconstructs the 32-level fold independently
  and compares, so it cannot pass by both sides sharing a mistake.
- `TestDeleteRecompressesTheSpine` - a tree emptied down to one entry must hash identically to
  that entry inserted fresh, and must be made of **one node**.

  The node count is the load-bearing half and was missing from the first version of this test.
  A compressed leaf hashes *identically* to the spine it replaces - that equivalence is the
  whole premise - so no assertion about hashes can tell whether compression happened at all.
  Ablating the re-compression branch leaves one entry spread over 4 nodes with every hash still
  correct; only counting nodes fails. Without it, the branch could be deleted as dead code and
  the suite would stay green while memory regressed on any namespace that churns.

`kdb-document/src/commonTest/kotlin/dev/kdb/document/DocumentTreeCompressionTest.kt` mirrors
those and adds two the Go side does not need: that splitting two keys differing only in their
last nibble is order-independent, and that deleting an absent key which shares a prefix with a
present one is a no-op. The second is specific to compression - a leaf above `TRIE_DEPTH` may be
a different key sharing the prefix, so delete now compares the key instead of trusting the path,
which the uncompressed version never had to do.

`go test ./...` and `go test -race ./...` green; Gradle `test allTests` green (11m40s).
