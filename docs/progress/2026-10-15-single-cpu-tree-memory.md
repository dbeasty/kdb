# 2026-10-15 — document trees grew without bound at one CPU

`TestTreeFileResidentMemory` and `TestSlabTreeMemoryPerDocument` failed whenever Go ran with
`GOMAXPROCS=1` (v0.8.1 and v0.8.2), and passed at 2 or 4. **The failures were real, not a test
artifact.** Unlike the snapshot test (`2026-10-15-snapshot-memory-test-threshold.md`), nothing
was being cancelled out: the live slab's chunk memory kept rising for as long as updates ran.
Updating 1,000 documents of a 200,000-document tree took the slab to 168–180 MB after 3M updates
at one CPU, still climbing. At four CPUs it stayed flat at 38.7 MB (the tree itself is 24.9 MB).
Small single-core VMs and containers limited to one CPU run with `GOMAXPROCS=1`.

## Why reuse depended on the CPU count

A dropped tree version's slab records are freed by a runtime cleanup on its handle, so only
after a GC cycle. A writer that only updates allocates almost no heap, so the slab requests the
cycles itself (`counted`: once the records made since the last cycle exceed half the live ones,
it runs `runtime.GC()` on a background goroutine). No `sync.Pool`, finalizer or release goroutine
was involved; the problem was GC timing:

- The writer allocates so little heap that it never assists the collector. The collector gets
  only its background share of the CPU: a millisecond per cycle at 4 CPUs, but **250–630 ms at
  one CPU**. During that time the writer made 200,000–500,000 more records.
- Those records survive the cycle, and when it ended the slab restarted its count from "now". So
  they were counted as live, and each trigger came out larger than the last. That is the
  geometric growth the nudge exists to prevent. The trigger's live estimate was
  ~600,000 records; the true number is 275,000.

## Fix

`go/kdb/document/document_tree_slab.go`, plus `commit` in `document_tree_trie.go`:

- **Backpressure.** The collection a slab requests must be done by the slab's next check (4,096
  records later). If it isn't, the writer waits for it after releasing the slab lock
  (`trieSlab.pace` → `slabPace`), much like a GC assist. A slab that comes due while another
  slab's request is running also waits on that one, rather than running ahead of it. At 4 and 16
  CPUs the writer never waited in the tests (a cycle takes ~400 records' time), so behaviour
  there is unchanged.
- **Accounting.** A cycle the slab requested counts its garbage from the request, not from when
  the cycle is seen to end. With this, the live estimate equals the true count (275,462 at both
  1 and 4 CPUs, traced).

Tried and dropped, because neither changed anything measurable: yielding after the wait (to let
cleanups run first) and yielding right after the request (to start marking sooner).

## Measurements

The tree has 200,000 documents and 3M updates go to 1,000 of them. Two runs each; the machine was
shared (load 9–15) and nothing else heavy ran.

| | chunks, 1 CPU | µs/update, 1 CPU | chunks, 4 CPUs | µs/update, 4 CPUs |
|---|---|---|---|---|
| before | 180.2 / 175.6 MB, growing | 8.44 / 8.68 | 38.7 / 38.7 MB | 9.02 / 9.07 |
| after | 38.7 / 38.7 MB, flat | 8.98 / 9.04 | 37.1 / 37.1 MB | 9.02 / 9.11 |

At one CPU, updates now cost 4–6% more. That is the collection work the writer used to skip, and
the rate now matches four CPUs. At `GOMAXPROCS=1` both original tests now pass:
`TestSlabTreeMemoryPerDocument` gives 206 B/doc (it was 380–398; 2 and 4 CPUs give 207), and
`TestTreeFileResidentMemory` gives 3.9 MB (16.6 MB before; 2.3 MB at 4 CPUs; its limit is 8 MB).

New test: `TestSlabReuseOnOneCPU` sets `runtime.GOMAXPROCS(1)` itself, runs 600,000 updates and
requires the chunks to stay within 1.8× the built tree. Before the fix it failed at 96.6 MB
(3.9×); after, it gives 38.7–40.2 MB (1.55–1.61×).

## Release note

**Fixed: memory growth on single-CPU deployments.** With one CPU (`GOMAXPROCS=1`: a single-core
VM or a container limited to one CPU), a namespace under sustained updates did not reuse the
memory of dropped document-tree versions fast enough, and the live tree's memory grew without
bound. In a benchmark it reached 4–7× the tree's size and was still growing. It now settles at
the same size as on multi-core machines (about 1.5× the tree). Updates on one CPU cost about 5%
more, the garbage-collection work that had been skipped. Multi-core deployments are unaffected.
