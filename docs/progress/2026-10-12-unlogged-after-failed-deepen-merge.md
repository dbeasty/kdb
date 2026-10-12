# 2026-10-12: unlogged commits after a failed deepen or merge, and the WebSocket peer drain

Branch `fix/unlogged-after-failed-deepen-or-merge`. These are the three gaps #113's entry in
`docs/kdb-open-issues-plan.md` left open in the "stranded unlogged commit" family. All three were
real on `main` (25dae37).

## Gap 1: a failed `Deepen`

**Real.** `peersync.Deepen` stored the history it fetched with plain `PutCommit` and
`PutShallowCommit`, without marking it unlogged. A retry inside the same process worked (59dae9e
made Deepen re-runnable). But if the node closed cleanly before a retry, the close-time checkpoint
described that history without operations. The reopened node held it, so a retry found every
parent present, fetched nothing, and failed reading it back to log it: `commit ... has no frame
in the delta log`. History reads below the root failed the same way. This failed for both a
refused queue and a failed fsync wait.

**Fix.** Deepen stores with `PutUnloggedCommit` and the new `dag.PutUnloggedShallowCommit` (for
new horizons). `persistAll` clears each mark before queueing, as before. It now also puts the
mark back when the **wait** fails, not only when queueing fails. A failed wait means the log may
not hold the frame. Omitting a commit the log does hold costs a refetch, but describing one it
lacks breaks the namespace. After a failure and a restart, the node reopens with the root still
shallow and without the history below it, as after a crash, and the next Deepen fetches it.

## Gap 2: a merge, join or fast-forward whose log write fails

**Real, and worse than recorded.** `Adopt` moves main (fast-forward `SetHead`, merge
`AppendMergeCommitOnto`, join `AppendCommitOnto`) and applies the new tree to storage *before*
`persistAll`. When the log write failed:
- main was left naming a commit the log never received;
- a merge or join made here was never marked unlogged;
- the peer's commits behind it were marked unlogged and left out of the checkpoint, while main
  was always kept.

So the close-time checkpoint described a head whose history was missing. Results seen, per
case:
- fast-forward: the reopened head was "missing";
- merge (#114 joins included): the next sync failed as unrelated histories, or the merge commit
  had no frame;
- refused queue: a local write after the failure was logged on top of the unlogged head, and
  the next open **refused to start** ("1 commit(s) reference parent commits never found in the
  log").

The #134 version-2 virtual-base merges go through the same `resolveDivergedLocked` step and
persist path, so they are covered by the same fix.

**Fix**, in three parts:
1. **Refused queue (under the serialization): undo the adoption.** `undoAdoption` marks the
   merge or join unlogged, puts main back on the previous head and rewinds live storage to that
   head's tree. It reads each touched document at the old tree, rewrites it, and checks the
   rebuilt tree hash. Frames queued before the refusal stay in the log, and the next open
   replays them as after a crash.
2. **Failed wait (outside the serialization): mark, don't undo.** Main has been released to
   other writers by then, and a log that failed a write refuses every later one (the
   `commitLogWriter` latch). So the commits whose waits failed get their marks back, and so does
   the merge or join made here.
3. **Checkpoint backstop.** `CheckpointSnapshotRetaining` now leaves out every commit built on
   an unlogged one, because replay cannot admit a commit whose parent it never stored. Shallow
   roots stop the walk, so a deepen in progress does not take its root out with it. The new
   `CheckpointSnapshotLogged` (used by `embed.saveCheckpoint`) refuses the checkpoint outright
   with `ErrHeadNotLogged` while main names such a commit. The previous checkpoint plus the log
   is then what the next open rebuilds, as after a crash.

`persistAll` now restores only the marks a commit had: a commit it never marked (a merge, or
history already logged) is the caller's to mark. Before, a refused queue marked the failing
commit unconditionally. For a Deepen re-logging its already-durable snapshot root, that would
have made main look unlogged.

## Gap 3: the HTTP/WebSocket peer endpoint was not drained

**Real.** On shutdown the service closed the TCP peer listener before draining, but the
`--peer-http` server (`/kdb/sync`, `syncnode.Node.Handler`) was only closed by a `defer`, after
storage had closed. Its sessions are hijacked WebSocket connections, which `http.Server.Close`
does not end. `Node.Close` closed them but did not wait for their handlers.

**Fix.** The new `syncnode.Node.CloseInbound` does four things:
- closes every listener `Listen` opened;
- ends every `Handler` session;
- refuses new sessions with 503;
- waits for the handlers to return, using a WaitGroup counted under the same lock as the
  refusal.

The service's shutdown now closes the peer HTTP server and then calls `node.CloseInbound()`
where it used to close only the TCP listener, still before `BeginDraining`. `Node.Close` calls
`CloseInbound` too.

## Tests

Every test fails on `main`. Each part of each fix was reverted once on its own to confirm a test
catches it:
- `server/unlogged_failure_test.go`, file-backed nodes, log failure injected by wrapping
  `PersistAsync` (as the v0.6.2 deepen tests do). Each case fails the write, closes cleanly,
  reopens, and requires a node that opens, whose whole main history has readable operations, and
  that converges on the next sync and survives another restart.
  - `TestFailedDeepenIsNotCheckpointed`: queue and wait. Reverting the Deepen marking or the
    wait re-marking fails it.
  - `TestFailedAdoptionIsNotCheckpointed`: fast-forward, merge and join, each with a refused
    queue and a failed wait. Merge and join also run "made-wait", where the peer's commits reach
    the log and only the merge's own wait fails. Reverting the undo fails all three queue cases
    (the reopen refuses to start). Reverting the backstop fails the three wait cases. Reverting
    the made-commit re-mark fails merge/made-wait.
- `dag/unlogged_test.go` `TestCheckpointLeavesOutWhatIsBuiltOnUnloggedCommits`: the closure,
  `ErrHeadNotLogged`, and the shallow-root exception (which fails when reverted).
- `syncnode/handler_test.go` `TestCloseInboundEndsHandlerSessionsAndWaits`: a live WebSocket
  session is ended, its handler has returned before `CloseInbound` does (fails without the
  wait), and a new session is refused.

## Gates

All run with load average 7-11 on a shared machine:
- `go test -race` for dag, peersync, embed, server, syncnode, replication and service: all ok.
- The full suite (`go test -race ./...`, 55 packages): all ok.
- e2e with binaries built from this branch,
  `test_cluster.py -k "drain or kill9" -m ""`: 4 of 4 runs passed, 4 tests each.
  `test_peer_http.py` also passed.

## Not covered

The failure injection fails the write without latching the real log. In production, a refused
queue or failed wait means the log is latched or closed, so nothing more is logged in that
process anyway. The in-process undo matters for what the process serves and checkpoints until it
stops. A **local** commit whose log write fails (`commitWith`) still leaves main on an unlogged
commit. It is not marked, so the backstop does not see it. That is outside peer sync and was not
changed here. Kotlin peer sync was not checked.

## Release note

A node whose delta log write failed while it was deepening history, merging a peer's changes or
fast-forwarding to them could restart broken. It failed every later sync with "has no frame in
the delta log", lost the merge, or refused to open. Such a failure now leaves the node as a crash
would: the next start is clean and the next sync fetches what was lost. On shutdown, peer-sync
sessions served over WebSocket (`--peer-http`, `/kdb/sync`) are now ended and waited for before
storage closes, as the TCP peer listener's already were.
