# 2026-10-15 — snapshot memory test passed only thanks to the bug #295 fixed

The v0.8.1 release's `artifacts` job (which runs `go test ./...` inside the embeddable bundle) failed
twice on `TestInstallSnapshotMemoryIsBounded`: n=2000 measured 733 B/doc live at Advanced against a
600 B/doc limit, reported as "bodies are being held".

**Not a leak, and not a v0.8.1 regression.** Reproduced only with `GOMAXPROCS=1` (740 B/doc; 120 at 2
or 4 CPUs). Exact heap diffs (`MemProfileRate=1`, base vs Advanced) show the same growth on v0.8.0
and v0.8.1 — document-tree slabs (+800 KB, +336 KB) and the page spool's buffer (+256 KB), no bodies
— but v0.8.0 also *freed* 1,024 KB in `dag.(*nodeStore).put`: the install replaced the node's
original graph, whose genesis held a full 1 MiB chunk. That freed megabyte cancelled the tree's
real cost. #295 made the genesis chunk 4 KiB, so the cost shows.

**Fix (test only):** the limit is half a body per document (`bodyLen/2`), which still fails the
moment bodies are held (2,000+ B/doc) without depending on slab-pool rounding. Passes at
GOMAXPROCS 1, 2 and 4.

## Release note

None (test-only).
