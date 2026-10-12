# 2026-10-12 — the two v0.7.0 nightly failures (branch `fix/nightly-e2e-failures`)

The v0.7.0 release commit (25dae37) ran the e2e nightly (run 38197566122), and two jobs failed.
The first is a real defect in this build's version 2 merge semantics (#134) and is fixed here.
The second is a test that checked more than it meant to.

## 1. `test_load_procedure_per_node_counter[5-0]`, `[5-1]`: merges held forever (product defect)

**Symptom:** a five-node mesh running the documented per-node counter (#133), with its chain at
version 2, did not converge in 180 s. Two head groups ({n0,n1,n4} and {n2,n3}), open conflicts on
every node, and syncs failing with `DEADLINE_EXCEEDED: timed out waiting for an earlier write to
finish` until the end of the run.

**Cause:** the held conflicts' reason, preserved in the evidence, is
`building the merge base from several nearest common ancestors: procedure perNodeCounter: more
than 10000 conflicting documents in one sync`. That is two documents, not 10,000.

- Under version 2, a merge whose heads have several nearest common ancestors folds them into one
  virtual base (`virtualBase`, `go/kdb/peersync/merge_semantics_v2.go`). Each fold step merges
  in the next base, using the merge bases of the two as its own base, recursively.
- Nothing was remembered between steps, so the same inner bases were rebuilt once for every path
  that reached them. The chain's procedure was rerun each time.
- In a dense criss-cross that is exponential. A five-node mesh under concurrent compare-and-set
  writes builds exactly that shape. The test's braid with 14 levels runs the procedure 32,767
  times for one document.
- The server allows 10,000 procedure calls per sync (`maxProcedureCallsPerSync`). Past that, the
  merge is held.
- The history is fixed once writes stop, and every node computes the same thing. So every retry
  on every node held it again, forever. The comment on that budget says a hold is safe because
  "the other node may still merge". That is not true when the limit is hit deterministically.
- Each retry started thousands of script sandboxes while holding the namespace's write gate (peer
  sync adopts inside `Exclusive`). So the node's other syncs waited more than the 5 s write
  timeout and failed with `DEADLINE_EXCEEDED`. The timeouts were a symptom, not the cause.

**It is (a), stuck forever, not (b), slow.**
- The preserved data dirs of both failed runs were restarted on the v0.7.0 binary, peered with
  each other, with no writes, on an idle core budget (`GOMAXPROCS=2` on a 16-core machine).
- They stayed on the same two heads for the full 90 s. Every node held the same reason, and
  `DEADLINE_EXCEEDED` came back. The conflict entries had already been retried 4-18 times each
  in CI.
- The same data dirs on this branch's binary converged in under 3 s, with no conflicts:
  - `[5-0]`: 0+2, then all five, on `9d4168838e5c`.
  - `[5-1]`: all five, on `e11cb1c4b807`.

**Fix:** `virtualBase` remembers each fold it builds, keyed by its depth and its bases in order,
and each base's values.
- Within one merge, a fold depends only on those and on the document ids, which are fixed for the
  merge. So a remembered result is exactly what rebuilding would produce.
- `held` already records the first reason a document was held, and the first visit happens in
  the same order. So the result, its origin and what is held are all unchanged.
- Only the number of times the procedure runs changes. The braid goes from 32,767 runs to 29.
- This applies to every rule a version 2 chain runs inside the fold. A chain without a procedure
  had no budget to hit, but it paid the same exponential CPU inside the write gate.

**Tests** (`go/kdb/peersync/merge_semantics_v2_virtual_base_test.go`):
- `TestV2VirtualBaseOfABraidIsLinear` builds the shape exactly: three strands, each level
  merging the three below in its own pairing order. It fails before the fix (32,767 procedure
  runs, budget 500) and passes after (29).
- `TestVirtualBaseMemoMatchesTheUnmemoizedFold` compares the fold with and without the memo on
  176 multi-base pairs of a random criss-cross. The value, the origin and the held set must all
  be equal.
- `TestV2PerNodeCounterMeshConverges` runs five nodes with the per-node counter, syncing in
  crossing rounds. It checks a bounded procedure count per merge, no holds, and that each key
  converges on its node's increments.

## 2. `test_rollback_to_v062_after_a_clean_stop`: v0.6.2's own merge storm (test design)

After the rollback, all five nodes ran v0.6.2. Every sync succeeded, but the heads kept
differing. That is N2, v0.6.2's five-node merge storm under concurrent writes, which #114 fixed
in this build. It does not depend on the data v0.6.2 opened, so it says nothing about whether a
rollback works.

The test now checks what it is for:
- All five rolled-back nodes are checked for every acknowledged write, as before.
- n3 and n4 then stop, and must stop cleanly on v0.6.2.
- The writes and the strict convergence check (same head twice, identical documents, no
  conflicts) run on three v0.6.2 nodes, with a 150 s budget. The writes of n3 and n4 must still
  be on those three.
- Three v0.6.2 nodes converge; the rolling-upgrade test starts from such a mesh.

`load()` gained a `nodes=` filter for this.

## 3. `test_resolution_chain_version_2_after_a_rolling_upgrade`: a plain write's 409 (test defect)

Another session reported this one, found at load 20-25: a plain control-plane write intermittently
got 409. It reproduced here 1 run in 3 with `GOMAXPROCS=2` at load 15. The 409 came from the
mixed-mesh rounds, where every node writes its own field of the same three documents with no wait
between rounds:

`write {'f0': 2} … failed (409): {'conflicts': [{'kind': 'CONCURRENT_WRITE'}], 'error': {'code':
'conflict', 'message': 'another writer committed first; re-read and retry'}, 'retryAfterMs': 2,
'baseHash': 'b9e03c…', 'targetHash': '4ec8c0…'}`

**Not a product defect.** A control-plane write is optimistic.
- It is built on the head the server resolves when the request arrives.
- `detectConflicts` (`go/kdb/transaction/default_engine.go`) refuses it only if *that same
  document* differs between that base and the head the write lands on.
- Here a peer's merge, carrying another node's write to the same document, landed in between.
- The response is the documented "plain conflict means retry" outcome
  (`writeCommitError`, `go/kdb/control/docwrite.go`), with a retry hint. v0.6.2 answers the same
  way.

None of the suspects applies:
- No conflict is held: the conflict queue is not involved.
- The chain hash is not involved either.
- No conflict is reported for a document nobody else changed.

The test helper `must_write` treated that answer as a failure. It now retries a 409 `conflict`,
honouring `retryAfterMs`, up to 20 times. Any other status, including 409 `precondition_failed`,
still fails at once. It then passed 6 runs in 6 under the same conditions.

## Evidence

Shared machine (16 cores, load 5-10 from other sessions). Binaries were built from this branch,
and v0.6.2 with `scripts/build-prev-service.sh v0.6.2`.

- `go test -race ./...` (module `go/`): all packages pass.
- `go test -race ./kdb/peersync/` and the server's resolution, procedure, chain and conflict
  tests: pass.
- `test_cluster_resolution.py`, default selection: 23 passed, 2 xfailed.
- `test_cluster_resolution.py -m "slow or destructive"`: 3 runs out of 3 gave 16 passed and
  2 xfailed (the documented summing procedure). Two of the runs used `GOMAXPROCS=2`, at load
  9-18. `test_load_procedure_per_node_counter[5-0]` and `[5-1]` passed every time.
- `test_upgrade.py -m upgrade`: 3 runs out of 3 passed, two of them with `GOMAXPROCS=2`. Each
  gave 10 passed and 1 xfailed (`after_a_crash`, recorded). One run instead gave 9 passed and
  1 skipped, which is the deepen test's timing-dependent skip.
- The whole nightly slow job (`-m "(slow or destructive) and not upgrade"`) with `GOMAXPROCS=2`:
  29 passed, 2 xfailed, and 1 skipped (S3, which needs LocalStack).
- `test_rollback_to_v062_after_a_clean_stop`, rewritten, passed on its own with `GOMAXPROCS=2`.
  Its original form passed here too (27 s): the storm needs a slower runner than this one.
- The evidence replays under section 1: the v0.7.0 binary stays stuck, and this branch converges
  in under 3 s.

## Not done (follow-ups)

- **A real budget hit still holds forever.** A divergence with more than 10,000 documents that
  genuinely need the procedure holds on every node, on every retry, for the reason above. The
  "safe" comment on `maxProcedureCallsPerSync` (`go/kdb/server/resolution.go`) is wrong. It needs
  a design decision: raise the budget, merge in pages, or settle past the budget by the chain's
  next rule.
- **Procedures run inside the write gate.** A legitimately large merge with a procedure chain
  still blocks local writes and other syncs for as long as its scripts run.

## Release note

- Fixed: on a mesh under concurrent writes (seen on five nodes), a version 2 resolution chain
  with a stored procedure could hold a merge forever. The error was `procedure …: more than 10000 conflicting documents in
  one sync`, even for a handful of documents. While it lasted, syncs on the affected nodes failed
  with `DEADLINE_EXCEEDED: timed out waiting for an earlier write to finish`. Building the merge
  base from several common ancestors repeated the same work once per path through the history;
  it now does it once. The merge results are unchanged. Data dirs already stuck converge as soon
  as their nodes run the fixed release.
