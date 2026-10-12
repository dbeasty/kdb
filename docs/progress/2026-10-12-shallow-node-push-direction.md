# 2026-10-12 — Shallow node, push direction: a snapshot hub that is only ever pushed to (branch `fix/shallow-node-push-direction`)

The gap #111 recorded by inspection ("Not fixed, found along the way", N1's entry in
`docs/kdb-open-issues-plan.md`): a node that dials a snapshot-bootstrapped node and pushes it a
branch forked behind that node's root gets "missing parent" back, for ever, when the shallow node
never pulls from anyone. #111 fixed the pull direction; #128 the `deepen=true` recovery.

**Reproduced, before any fix.**
- Go, `server/shallow_push_test.go`: #111's `boundaryFork` (file-backed joiner J at root S, A
  holding C on P, P a parent of S), then A dials J and pushes (`Mode: SyncPush`). On main:
  `peer: INTERNAL: peer sync: storing commit <C>: missing parent <P> (0 of 2 commits in this page
  were stored before it)`, both with a memory and a file-backed pusher.
- e2e, `test_cluster.py::test_snapshot_hub_that_is_only_pushed_to_converges`: two writing nodes,
  a hub bootstrapped from n0's snapshot and then restarted with no `--peer`, both writers given
  `--peer name=hub,...,mode=push`. On a main build both pushers failed every cycle
  (`consecutiveFailures` 29 and 51 when the 60 s wait ran out) with
  `namespace e2e/data: peer: INTERNAL: peer sync: storing commit 9da78da1…: missing parent
  37b94c47… (0 of 163 commits in this page were stored before it)`; the hub never moved.
- Why: the pusher sends "reachable from my ref minus the ancestor closure of what the peer's refs
  name" (`MissingCommitsFrom`). The hub's head is its root S, so every ancestor of S counts as
  held; P is one, and the hub holds nothing behind S. The pull fix cannot run: the hub cannot
  fetch from the node that dialled it.
- Making the e2e deterministic took two goes, both worth knowing. First, with timing alone the
  pushes reached below the root only 3 of 5 times: a pusher whose commits all descend from the
  hub's root, or one whose second pull moved the hub past the fork, needs no fork point. Second,
  a pusher that does **not** hold the hub's head sends its entire history (`known` is empty), and
  that heals the hub as a side effect: the page reaches genesis. The bug needs a pusher that
  holds the hub's head. The fixture (`cluster_fixtures.fork_behind_a_pushed_only_hub`) now builds
  the fork with stops: from a common head H, n1 writes C while n0 is down, n0 writes X while n1
  is down, the hub bootstraps at X with one sync (`interval=1h`), and n1 takes X before it is
  wired to the hub. It then fails on main every run and the test asserts the pushers' logs show
  the fork point was sent.

**Design: the receiver asks, on the same session; no message changes shape.**
- Receiver (`peersync/v2_host.go` `refUpdate`, `peersync/boundary.go`): when a pushed page will
  not store and this node has shallow roots, it runs #111's `gapsBelowBoundary` over the page. If
  parents are missing behind its boundary it refuses the page with a typed
  `PushBelowBoundaryError`, sent as PEER_ERROR with a new code, `SHALLOW_BOUNDARY`
  (`wire.ErrorCodeShallowBoundary`). It remembers, for this session only, which parents those
  were (bounded at 1024).
- Pusher (`peersync/v2_client.go` `push`, `pushBoundaryStates`): on `SHALLOW_BOUNDARY` it works
  out which parents of the page lie behind the roots the peer advertised (`NamespaceRefs.Shallow`;
  `peerBoundaryGaps` walks the history behind them, only in this rare path). It sends each one's
  state with `GRAFT_PUSH`, built by `snapshotPage` (and #111's fold when it never built that
  tree), and then sends the same page again, once. What it sent is in
  `NamespaceSyncResult.BoundarySent` and is logged by the replicator. If it cannot supply a
  state, it fails with a typed `PeerBoundaryError`.
- Receiver, `GRAFT_PUSH`: a commit this session refused a push for is admitted through #111's
  `extendBoundary` (Graft's durable path: foreign tree, marker `graftRoots` before the log frame,
  verified against the commit's hash and tree hash). It needs no `allowUnrelated`, because the
  receiver itself found the hash as the parent of a pushed commit, behind its own boundary. Any
  other `GRAFT_PUSH` is a graft as before and still needs `allowUnrelated`, which the tests pin.
  Like a pushed graft, it needs the session's push grant on the namespace. Like a pulled fork
  point, it is not checked document by document (`AuthorizeDocuments`): it is an existing
  commit's state, not a write, and it never reaches main except through a merge whose own
  operations are authorized as usual.
- Rejected: having the pusher send fork-point states up front. It cannot tell exactly what the
  receiver lacks without probing. A `FETCH_REQUEST` probe walks the receiver's whole ancestry of
  the probed commit, and it would cost a round trip on every push to a shallow node. Asking only
  after a refusal costs nothing on the common path. A new field on `REF_UPDATE_ACK` was also
  rejected: an old pusher would ignore it and read the ack as success.

**Compatibility.**
- **Older pusher (v0.7.0 and earlier) → this receiver:** it does not know the code, so it does
  not retry. Its push fails with `peer: SHALLOW_BOUNDARY: peer sync: <ns>: pushed commit <C> has
  parent <P> behind this node's snapshot root ... the pusher did not send its state first - a
  pusher newer than v0.7.0 does when told; otherwise this node takes it when it next pulls from
  any peer, or by deepening ...`. That is what `/v1/peers` `lastError` shows. Main does not
  move, and nothing is admitted. Shown in Go
  (`peersync/boundary_push_test.go::TestOldPusherGetsATypedBoundaryRefusal`, which drives the
  host with raw frames) and in e2e against real v0.6.2 pushers
  (`test_upgrade.py::test_old_pushers_into_a_new_snapshot_hub_get_a_typed_error_then_heal_on_upgrade`):
  the typed error appears, the hub holds its root, and upgrading the pushers heals it. v0.7.0 is
  not tagged yet; main's build stood in for it in the reproduction above.
- **This pusher → an older receiver:** the receiver answers `INTERNAL "missing parent"` as it
  always did. The pusher does not react to it and reports it unchanged.
- The pull path (#111), canonical joins (#114), version 2 merges (#134) and graft are unchanged.
  Only the refused-push branch and the refused-parent `GRAFT_PUSH` branch are new.

**Tests** (each confirmed failing with the fix reverted):
- `server/shallow_push_test.go`. The push fast-forwards a file-backed J across the fork point,
  with a memory and a file-backed pusher. J does not take history below its root, the root stays
  shallow, a clean restart keeps the head, and the next push sends nothing more. A second case:
  J wrote since its bootstrap, so the push is a real merge on J that traces C's value down to the
  fork point. A pulls the merge back, both agree, and the result survives a restart.
- `peersync/boundary_push_test.go`. The fork point is sent and becomes a shallow root on the
  receiver. An old pusher gets `SHALLOW_BOUNDARY` naming both commits. A `GRAFT_PUSH` of a commit
  behind the root that the session was *not* refused for is still refused (`allowUnrelated`).
  After the refused parent's state, the same page stores and main moves.
- Revert evidence:
  - Receiver change reverted: the server and peersync tests fail with the original `INTERNAL ...
    missing parent`.
  - Pusher change reverted: they fail with `SHALLOW_BOUNDARY ...`, which is exactly the
    old-pusher behaviour above.
  - e2e on a main build: fails as quoted above.

**Gates** (shared machine, macOS, load 6-29, binaries built from this branch):
- `go test -race` on peersync, server, syncnode and replication: pass. Full
  `go test -race ./...`: pass (55 packages).
- e2e:
  - `test_cluster.py -k only_pushed_to`: 5 of 5 passes with the fix; fails every run on main.
  - `test_cluster.py` default: 13 passed. With `-m ""`: 16 passed.
  - `test_graft.py`: 1 passed. `test_replicator.py`: 5 passed.
  - `test_upgrade.py -m upgrade` with `KDB_SERVICE_BIN_PREV` from
    `scripts/build-prev-service.sh v0.6.2`: the new
    `test_old_pushers_into_a_new_snapshot_hub_...` passed every run. One full run of the file was
    clean (11 passed, 1 xfailed). Others failed one or both of two tests that also fail on a
    main build of the same file (2 of 3 main runs, same signatures). Both are pre-existing and
    load-dependent, and neither touches this change:
    - `test_resolution_chain_version_2_after_a_rolling_upgrade`: a `must_write` answered 409
      (`baseHash` conflict) in the mixed v0.6.2/new mesh. There is no shallow node, so neither
      new path runs. It passed alone and paired with the new test.
    - `test_rollback_to_v062_after_a_clean_stop`: a five-node mesh running only v0.6.2 did not
      converge in 90 s at load 20-25. None of this branch's code is in those processes.

**Not fixed, found along the way.**
- After a refused page is retried, the pusher's `Pushed` count misses the commits the refused
  attempt stored before it failed (they are already held on the retry). This is cosmetic.
- A push-only peer never takes the hub's merges back, so a hub merging pushes from two writers
  never reports the same head as them, only the same documents. That is how `mode=push` already
  behaves; the e2e compares documents for the hub, not heads.
- A `history=none` receiver still cannot hold a fork point's tree (#111's note,
  `ErrForeignTreeNeedsObjects`). By inspection, not tested: a push into one fails the admission
  with the receiver's `ShallowBoundaryError`, which reaches the pusher as a `PeerBoundaryError`,
  and the receiver has to deepen, as on a pull.
- `test_upgrade.py`'s chain v2 test intermittently gets a 409 on a plain write, on main as well
  (see Gates). It is worth its own look.

## Release note

**A snapshot-bootstrapped node that is only ever pushed to now takes branches forked behind its
snapshot root.** Before, a hub whose peers push to it (`mode=push`), or a node with no `--peer`
of its own, refused such a push with "missing parent" on every sync, for ever. Now the pushing
node sends the fork point's state on the same connection. Pushers from v0.7.0 and earlier can't
do that: their push fails with a `SHALLOW_BOUNDARY` error naming the commit and its missing
parent. Upgrade the pushing nodes, or have the hub pull from a peer once or deepen
(`POST /v1/ns/{ns}/deepen`).
