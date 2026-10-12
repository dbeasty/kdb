# 2026-10-11: F1, F2, F4, resolution chain merge semantics (version 2)

Branch `fix/resolution-chain-merge-semantics`. Fixes three of the four defects that #130
(`kdb-integration/e2e/test_cluster_resolution.py`) found in resolution chains on a mesh. F3 is a
procedure-semantics and documentation matter and is handled separately.

## What landed

All three fixes change what a merge builds. Every node must build the same merge for the same
heads, or a mesh diverges (N2/#114). So the fixes are **gated** on a chain version:
`ResolutionChain.Version`, the `"version"` field of `PUT /v1/ns/{ns}/resolution`.

- **Version 1 (absent, or `1`, which is normalized to absent)** keeps the original semantics,
  byte for byte. Its JSON has no `version` field, so its hash is the one v0.6.2 computes. Every
  new code path is behind `chain.mergeBases()`.
- **Version 2 (`peersync.ChainVersionMergeBases`)** has the fixed semantics.
- **Any higher version** is refused by `PUT` (400). A replicated copy is still installed, so
  the node does not fall back to the chain it held before, and the error goes to the conflict
  queue (`meta-apply`). The node advertises `unsupported:<hash>` and holds every conflict with the
  reason.

**F1: merge base.** New `dag.MergeBases(as, bs)` returns every nearest common ancestor. It uses
git's paint-down-to-common walk, newest generation first, which visits only the history since the
bases. With `AncestryPruning` off it falls back to closures. `CommonAncestor` is unchanged:
version 1 and other callers still use it. In version 2 the base handed to the rules comes from
`peersync.virtualBase`:
- one base is used as it is;
- several are folded in hash order. Each fold three-way merges against the nearest common
  ancestors of the folded set and the next base, recursively, under the namespace's chain.

What the chain cannot settle while building the base (it queues, or runs out) goes to the later
write. A rule that cannot run (a failing procedure) holds the document. Only the conflicting
documents are built, with `valueIndex`: a first-parent walk of the operations the node holds,
reading a shallow root's tree where the walk ends.

**F2: field-merge per field.** Version 2's field-merge (`fieldMergeByOrigin`) does not use the
base. For each top-level field the two sides differ on, `valueIndex.fieldOrigin` follows the field
to the write that set it:
- through commits that left the field alone, to the first parent;
- through a merge, to every parent holding the same field value, taking the latest answer;
- a merge that built a value neither parent had is its own origin.

A side changed a field when that origin is not in the other head's history, the same rule the
merge already applies per document. One side changed it: that side wins. Both changed it: a
collision. If `last-write` is the next rule, each colliding field goes to the later field origin
(timestamp, then hash). Otherwise the document passes to the next rule, as before.

**F4: validity and the schema.** For a version 2 chain with `validity`, `AdvertisedHash` appends
the node's schema hash (`;schema=<hex>|none`), so nodes judging by different schemas do not merge.
A node holding no schema holds a conflict that reaches `validity` instead of letting the next rule
decide. The schema replicates through `_kdb/meta`, which a sync adopts before it compares hashes,
so the held divergence settles on the first sync with a node that has the schema.

Also:
- `GET …/resolution` reports `version`, `supported` and `advertisedHash`.
- `MetaStore.repinChain` copies the whole chain. It used to rebuild the chain from its rules,
  which dropped `allowUnrelated` and would have dropped the version.
- The user guide's resolution-chain section has a "Chain version 2" subsection. The procedure
  section is untouched.

## Why version 2 is deterministic and order-independent

- **Deterministic.** Every input to a version 2 merge is a function of the history both nodes
  hold: the merge-base set (sorted), the fold order (hash order), the chain, and field origins.
  Merge commits keep the #114 rules: hash-ordered parents, derived tx id, fixed author and message,
  the latest parent's timestamp. `resolveBothWays` checks both nodes build the same commit, and a
  15-seed mesh test (`TestV2MeshConvergesOnTheLastWriterWinsMap`) runs concurrent pairwise syncs.
- **Order-independent (field-merge then last-write).** The DAG stamps every commit after its
  parents (`causalTimestampLocked`). So a field write is earlier than any later write that has it
  in its history.
  - By induction over merges, a field's value at any head is its latest write in that head's
    history.
  - "Only one side changed it" therefore coincides with "that side's write is later".
  - A merge keeps the later of two concurrent writes, so the result is the maximum over the union
    of the histories.
  - Maximum is associative, commutative and idempotent, so the result is a last-writer-wins map
    whatever the merge order.
  - `TestV2FieldMergeLastWriteIsAssociativeAndCommutative` checks this directly: 40 seeds, 3-5
    heads with random overlapping field writes and criss-cross syncs, 8 random merge trees each.
    Every tree must equal the LWW map of all writes. Under version 1 the same test fails at seed 1.
- **Caveat.** A node that holds less history (shallow, below a snapshot root) can find a
  different field origin or merge base than a full node. It is the same limit `origin()` and
  `CommonAncestor` already had, and the reason the e2e graft and deepen tests exist.

## Evidence

- Go unit tests at each layer:
  - `dag/merge_bases_test.go`: criss-cross, an older ancestor that is closer, edge cases, and 30
    random graphs against the definition, pruned and unpruned.
  - `peersync/merge_semantics_v2_test.go`: the F1 shape, the F2 three orders, the property test,
    the criss-cross counter (version 2 gives 11111 and version 1 gives 11211), validity holding
    without a schema, the hash binding, the version gate, and field origins.
  - `peersync/merge_semantics_v2_mesh_test.go`.
  - `server/resolution_version_test.go`: normalization, the `PUT` refusal, and a replicated
    unknown version that is installed, reported and advertised as unsupported.
  - Each F test also asserts that version 1 still gets it wrong, so the fixture stays a real
    reproduction.
- e2e, `test_cluster_resolution.py`: chains are set at version 2. The xfail marks for F1, F2 and
  F4 are removed. The F1 load test and the ordered counter test pass. Two F3 marks stay:
  `..._when_both_sides_reach_the_same_value` (strict) and `test_load_procedure_counter_sum`
  (non-strict), with the F3 reason. `KDB_E2E_CHAIN_VERSION=1` reruns the file under version 1:
  F1 [3,5], F2 [3,5], F4 [3] and the counter test [3,5] fail again, 7 failures.
- e2e, `test_upgrade.py::test_resolution_chain_version_2_after_a_rolling_upgrade` (v0.6.2 binary):
  1. A mixed mesh with a version 1 chain shows the same chain hash on v0.6.2 and on this build, and
     converges.
  2. After all nodes upgrade and the chain switches to version 2, the ordered F2 scenario
     converges on the later write.
  3. A v0.6.2 node under a version 2 chain still reports the version 1 hash. It ignores the
     unknown field, so it does not merge with version 2 nodes, but v0.6.2 nodes still merge with
     each other under version 1. That is why the switch comes after the upgrade.
- Kotlin: `TestKotlinReadsGoConflictResolution` runs under chain versions 1 and 2. The Kotlin CLI
  replays merges Go built under either.

## Gates (shared machine, load average 3-15)

- Targeted `go test -race` for dag, peersync, server, replication, syncnode, control and
  integration (Kotlin interop included) passed. The full `go test -race ./...` passed once,
  after the first merge of main.
- `test_cluster_resolution.py`, default run, before #133 was merged in: 21 passed, 2 xfailed
  (F3), four times, two of them with `GOMAXPROCS=2` in the services' environment. After merging
  #133 (F3 docs and per-node counter tests): 23 passed, 2 xfailed.
- `test_cluster_resolution.py`, slow run: 12 passed, 2 xfailed (load counter sum, F3), once
  normally and once with `GOMAXPROCS=2`.
- `test_cluster.py`, `test_resolver_authority.py`, `test_graft.py`: 15 passed.
- `test_upgrade.py -m upgrade` with v0.6.2 (`scripts/build-prev-service.sh v0.6.2`): 10 passed,
  1 xfailed. The xfail is the pre-existing rollback-after-crash test.

## Merging with #133 (F3)

#133 marked `test_procedure_counter_sum_is_the_total_of_increments` as a strict xfail (the
anti-pattern). Under a version 2 chain that ordered scenario passes, because the procedure gets
the virtual base, so the mark is gone. The sum is still the anti-pattern: the strict
`..._when_both_sides_reach_the_same_value` and the non-strict load test keep #133's reasons. The
user guide's "`base` is one common ancestor" sentence now says what version 2 hands a procedure.
