# 2026-10-11 — F3: what a merge-time procedure can rely on (branch `fix/procedure-merge-semantics`)

F3 from #130's report: a stored procedure used as an in-merge resolver can't merge by summing
deltas, and the user guide suggested exactly that ("add the two counters together"). This
changes docs, tests and the parity corpus only. Merge semantics are untouched: F1/F2/F4 (merge
base, chain format, peersync resolution) are on `fix/resolution-chain-merge-semantics`.

**The contract, as the code stands** (`go/kdb/peersync/merge_resolution.go`):
- A procedure is called only for a document whose bodies differ and that both sides changed.
  The criss-cross case where neither changed but they still differ also counts. Byte-equal
  bodies are skipped (`sameBody`, and `joinSameContent` for equal trees). A document only one side
  changed is taken whole.
- `base` is the value at `dag.CommonAncestor(p0, p1)`: one nearest common ancestor, possibly a
  stale one in a criss-cross.
- `origins` follow a value back through merges that kept a parent's body. A body a merge
  built has the merge commit as its origin, with the merge's timestamp.
- `local`/`remote` are canonical sides (lower head hash first), not "this node".
- The procedure runs on every node, possibly repeatedly, over earlier merge results.

So the result must be a deterministic join of states: commutative, associative and idempotent.

**Is the per-node counter expressible? Yes, fully.** The procedure doesn't need to know which
node runs it or which side is whose. Each *writer* raises its own key, and the procedure takes
the per-key max. The writer gets a stable key from its node id (`kdb node status`, or
`thisNode` in `GET …/resolution`). `origins` aren't needed.

One trap found along the way: the merged body is compared **byte for byte** (`sameBody`, and
`ToJSONString` keeps the procedure's key order). So a procedure must emit keys in sorted order,
or two nodes building the same map in different insertion orders won't converge. The documented
example sorts.

**Changed:**
- `docs/kdb-user-guide.md`: what the procedure is given, made precise. New subsection "What a
  merge-time procedure can rely on": when it is called and not called; the commutative,
  associative and idempotent requirement; the per-node counter example; set union, max/min and a
  monotone status as safe patterns; a don't-list (`local + remote - base`, appending without
  dedupe, wall-clock and origin timestamps).
- `docs/kdb-spec-layer11-component32-stored-procedures.md`: §12.1 example fixed, the `base`
  comment corrected, and a new §12.4.1 with the same contract.
- `go/testdata/golden/conflict_corpus/per-node-counter-max.json` (new parity case, passes on goja
  and GraalJS). `sum-two-counters.json`'s comment now says it is runtime parity only, not a merge
  rule.
- `kdb-integration/e2e/test_cluster_resolution.py`:
  - `test_procedure_per_node_counter_is_the_total_of_increments[3,5]` covers the criss-crossing
    ordered merges that break the sum, plus concurrent increments on the live mesh.
  - `test_load_procedure_per_node_counter[3,5 × kills 0,1]` (slow) runs every node under CAS at
    full speed, optionally with one SIGKILL. It checks each key against its node's last
    acknowledged value. The allowance is one write the kill cut off after it committed.
  - The three sum tests keep their xfails, with reasons citing the contract.
  - New helper `converged_full`: the docs listing returns only a *preview*, which a map keyed by
    node ids outgrows. `converged_docs` would fail on it with a JSON decode error.

**Evidence:** load runs converged exactly, e.g. 916/916 and 914/914 increments on 3 nodes
without kills. With a kill: 831 converged against 830 acknowledged, which is the one cut-off
write. The summing procedure stays xfailed alongside it.

**Not done:** a lint at `PUT …/procedures` for the `- base` pattern. A source-text match would
also flag legitimate uses (comparisons, the parity corpus's own sum case), so it wouldn't be
robust. The docs and tests carry the contract instead.
