# Runtime configurability: change settings without a restart

**Status:** plan, nothing implemented. Written 2026-09-09 against
`feat/scheduled-maintenance-loop` (PR #51). §3 revised the same day after
review: the mode switch is non-destructive and reclamation is a separate
explicit act, which is a better design than the one first written here.

The ask, in the order it was given:

1. The maintenance loop should be configurable **at runtime through the
   control UI**, as well as at startup.
2. Reclamation should offer **immediate cleanup vs lazy cleanup** as a
   choice.
3. It should be possible to switch **`history=full` → `history=none`** (and
   back) at runtime, and certainly across a restart.
4. Generally: **settings should take effect as soon as possible** rather
   than requiring a restart or a rebuild.

Point 4 is the real ask and the other three are instances of it, so this
plan is organised around the existing mutability ladder rather than around
the three features.

---

## What already exists

Worth stating before proposing anything, because most of the machinery is
built and the work is mainly filling gaps in it.

**A mutability classification, per setting** (`config/descriptors.go:44`).
Five classes, each with a documented meaning:

| class | meaning |
|---|---|
| `MutabilityLive` | a setter exists that is documented safe under load |
| `MutabilityNewConnections` | read when a listener/connection is created |
| `MutabilityNamespaceReopen` | read when a namespace is opened |
| `MutabilityRestart` | read once at startup |
| `MutabilityImmutable` | describes what is on disk; disagreeing is refused at open |

**A live-change API** (`control/settings_apply.go`). `PATCH /v1/settings`
with per-change outcomes, `dryRun`, `expectRevision` compare-and-swap, and
opt-in `persist` to the config file (`--control-settings-persist`, off by
default because in a GitOps deployment that file belongs to the deployment
tool). Anything applied and not persisted is **drift**, reported at
`/v1/settings/drift` and shown in the UI.

**A refusal that names the remedy** (`settings_apply.go:387`). A refused
change comes back with its class and what *would* work, rather than a
generic "cannot change that".

**A settings tab in the UI** (`control/ui/index.html:1331`), already
rendering `/v1/settings`, editing, and showing drift.

**Five live settings today**: `memory.budgetMB`, `memory.reserveMB`,
`governance.scanRowBudget`, `log.level`, `cache.commitOpsBytes`.

So: the ladder, the API, the persistence story, the drift story and the UI
all exist. What follows is mostly about moving specific settings *down* the
ladder and closing two structural gaps.

---

## The gaps, precisely

### Gap A — the maintenance loop is startup-only and mostly undescribed

`governance.maintenanceInterval` (PR #51) is `MutabilityRestart` with no
live setter. `Sweep`, `MaxDefer` and `CompactTables` are
`MaintenanceOptions` fields with no descriptor, no flag and no env var at
all — reachable only by an embedded caller writing Go.

### Gap B — history mode and retention are invisible to the control plane

`KDB_HISTORY_MODE`, `KDB_RETAIN_DURATION` and `KDB_RETAIN_COMMITS` are read
by `embed/storage_options.go` and appear in **neither** `serviceSpecs()`
**nor** `envOnlySpecs()`. The control plane cannot report them, let alone
change them. (`history.strategy` *is* described, as `MutabilityImmutable`.)

This is the most surprising finding of the research: the settings that
decide whether a namespace deletes data are the ones an operator currently
cannot see from the UI.

### Gap C — `MutabilityNamespaceReopen` is a class with no implementation

The refusal message says so outright: *"it needs that namespace closed and
reopened, which this control plane cannot yet do"*
(`settings_apply.go:398`). Meanwhile `Host.CloseNamespace` and
`Host.NamespaceWithOptions` both exist (`embed/host.go:294`, `:213`), so
the mechanism is present and only the wiring and the safety are missing.

Closing this gap is worth more than any individual setting: it converts a
whole existing class of settings — `cache.documentBytes`,
`cache.historyTreeBytes`, `storage.checkpoints`, `graph.ancestryPruning`,
`graph.rebuildCommits`, `history.anchorInterval` — from "restart" to "a few
seconds of that one namespace being unavailable".

### Gap D — two sources of truth for the history mode

- `meta.json`'s `historyMode`, read at open, **enforced** by the engine.
- `policy.NamespacePolicy.HistoryMode`, runtime-mutable through
  `policy.Registry.Put`, and used for **exactly one thing**: choosing
  between `HistoryDisabledError` and `HistoryNotRetainedError` when a
  version fails to resolve (`query/hybrid/engine.go:96`). It enforces
  nothing.

A policy that says `none` over a namespace whose marker says `full` changes
error text and nothing else. That is a trap for whoever wires runtime mode
switching, because the policy registry is the mutable-looking one and the
marker is the one that counts.

---

## Design

### 1. An honest ladder, applied to these settings

The target class for each setting, and what it needs:

| setting | today | target | needs |
|---|---|---|---|
| `governance.maintenanceInterval` | restart | **live** | setter on the scheduler |
| `governance.maintenanceSweep` | *(absent)* | **live** | descriptor + setter |
| `governance.maintenanceMaxDefer` | *(absent)* | **live** | descriptor + setter |
| `retain.duration` / `retain.commits` | *(absent)* | **live** | descriptor + setter; see below |
| `governance.reclaim` | *(absent)* | **live** | new; `manual`/`immediate`/`balanced`/`lazy`, see §2-3 |
| `history.mode` | *(absent)* | **live** | §3 — the switch writes a marker and destroys nothing |
| `history.strategy` | immutable | immutable | unchanged — it is a migration |

`retain.*` is the easiest of these, though not free. The window is already
*threaded per pass* — `checkpointAndTruncate(… window …)` takes it as an
argument rather than reading it from anything cached deeper down — but the
value comes from `opts.Storage.Retain` on the `opts` copy the `maintain`
closure captured at open (`embed/file.go:264-273`). So making it live means
giving that closure a mutable holder to read instead of a captured struct.
That is a small, local change: no reopen, no marker rewrite, no migration,
and the plumbing that carries the value to the decision is already correct.

### 2. Immediate vs lazy cleanup

Not a new mechanism: a named preset over knobs PR #51 already has, plus one
new trigger for the genuinely-immediate end.

| preset | `Interval` | `Sweep` | `Busy` respected | `MaxDefer` | extra |
|---|---|---|---|---|---|
| `manual` | — | — | — | — | **never reclaims**; only an explicit `compact history` does |
| `immediate` | 10s | 1m | no | 0 | reclaim on segment seal |
| `balanced` | 5m | 30m | yes | 6 | — |
| `lazy` | 30m | 6h | yes | 24 | — |
| `off` | — | — | — | — | reclaim only at close |

`manual` is the important addition and is covered in §3: it is what makes a
mode switch non-destructive, and it is the state a namespace should land in
when it is switched to `none`.

`immediate` needs one new hook: a callback on segment seal, so reclamation
is triggered by the event that creates the opportunity rather than by a
clock that happens to tick afterwards. The seal point is already a
well-defined moment (`truncate.go` refuses to touch the segment currently
being written, and `file.go`'s `maintain` closure computes `through` from
the newest *sealed* segment).

State the trade honestly in the UI help text: `immediate` costs continuous
CPU and I/O against the smallest possible footprint; `lazy` costs disk
against nearly no background work. The three-probe `needsWork` gate means
even `immediate` does nothing on an idle namespace.

**Keep `MaxDefer` non-zero for every preset that respects `Busy`.** The
reasoning from PR #51 stands: the busiest server is the one whose log grows
fastest, so unbounded deferral hands back exactly the unbounded growth
`history=none` exists to prevent. `immediate` sets `Busy` aside entirely
rather than deferring, which is the coherent reading of "immediate".

### 3. Switching history mode at runtime

**Revised 2026-09-09 after review.** The earlier version of this section
treated `full`→`none` as inherently destructive and `none`→`full` as a
one-way door. That framing was wrong, because it conflated two things that
should be separate: *which mode a namespace is in* and *whether anything
has actually been reclaimed yet*.

#### The mode switch itself destroys nothing

Flipping `full`→`none` writes a marker and nothing else. It does not
delete a segment, does not fold history, does not touch the log. All it
changes is what the namespace is *permitted* to do later. Flipping back to
`full` before anything has been reclaimed is therefore completely
lossless — there is nothing to restore, because nothing was lost.

This makes the mode switch a cheap, reversible, low-stakes operation, which
is what it should have been: an operator evaluating `none` should be able
to turn it on, look at what it would reclaim, and turn it off again.

#### Reclamation is a separate, explicit act

The destructive step is `compact history`, and it is the real one-way door
— correctly located, and now the only place a confirmation is warranted.

This needs a `reclaim` axis orthogonal to the mode, which folds into the
preset ladder from §2:

| reclaim | behaviour |
|---|---|
| `manual` | **nothing is ever reclaimed automatically.** Only an explicit `compact history` deletes anything |
| `immediate` | reclaim on segment seal |
| `balanced` | the PR #51 default: every 5m, when there is something to do and nothing to fight |
| `lazy` | every 30m, deferring hard to load |

**`manual` should be the default a namespace lands in when it is switched
to `none`.** Turning on a mode should not, by itself, start deleting data.
An operator opts into automatic reclamation as a second, separate decision.

This does mean `none` + `manual` gives unbounded disk, which is the thing
`none` exists to prevent — so the UI has to say plainly that the mode is
armed but not reclaiming, and how much is currently eligible. `planTruncation`
computes exactly that without deleting anything, so the number is available.

#### The "special entry that stays" already exists — as the checkpoint

The natural way to express "history collapses to one retained thing" would
be a synthetic baseline commit standing in for everything below the floor,
with the oldest retained commit re-parented onto it. **That is not
available.** `ParentHashes` is inside the hashed commit payload
(`document/kdb_commit.go:59`), so re-parenting the oldest retained commit
changes its hash, which changes every descendant's hash, which rewrites the
entire namespace's identity. A baseline cannot live in the hash chain.

It has to be a side structure the loader knows about — and the checkpoint
is already exactly that. It survives truncation by construction, it holds
the complete live tree as (docID, contentHash), and under `none` it is
already promoted from cache to *authority*. The "special entry that stays"
is built; it just isn't described that way.

What the checkpoint gives back on a restore is therefore **the state at the
floor, not the commits that produced it**. That is a real limit and it is
inherent to collapsing history at all.

#### Full restoration is achievable, and is mostly built

Unless the reclaimed segments are kept somewhere. And they already are:
`s3.ReplicaSink` mirrors every sealed segment (`PutSegment`), and
`GetSegment` and `ListSegments` **already exist** on it
(`storage/io/s3/replica_sink.go:60-66`). A namespace with the S3 tier on
has a complete copy of every segment truncation is about to delete.

**One thing blocks using it, and it is a two-line problem with a real
design decision behind it.** `PrimaryWithReplicas.Delete` fans deletion out
to every replica (`storage/io/primary_replicas.go:58-65`), so compaction
currently destroys the archive it would have restored from.

Splitting "delete locally" from "delete everywhere" turns `compact history`
into **evict from local disk** — bounded local disk *and* full
restorability, including the individual commits, not merely the floor
state. That is the strongest version of what was asked for, and it needs:

- a replica role that deletion does not reach (an *archive* tier, as
  distinct from a *replica* whose job is to mirror the primary exactly —
  these are genuinely different jobs and conflating them is why the fan-out
  exists);
- a cold-read path that falls back to `GetSegment` when a segment is below
  the local floor, which is a natural extension of the existing
  `deltaColdLoader`;
- a restore action that pulls a range back to local disk and moves the
  floor back down.

Without an archive tier configured, `compact history` stays genuinely
destructive and must say so. With one, it is eviction and is safe.

#### What each direction actually means, restated

| action | with no archive | with an archive tier |
|---|---|---|
| `full`→`none`, nothing compacted | reversible, lossless | reversible, lossless |
| `compact history` | destructive; state survives via the checkpoint, commits do not | eviction; everything restorable |
| `none`→`full` after compaction | history resumes from the floor; the collapsed range is gone | history resumes; the collapsed range can be pulled back |


### 4. Namespace reopen (Gap C)

The enabling primitive for much of the above. `CloseNamespace` currently
unroutes *first*, then tears down — so a naive close/open leaves a window
where calls fail with `ErrUnknownNamespace`. A reopen for configuration
should instead:

1. quiesce writes for that namespace (the write gate already supports this
   — `BeginDraining`/`WaitForWritesToDrain` are per-runtime);
2. close and reopen under the new `StorageOptions`;
3. re-register with the adapter and the memory arbiter;
4. report the pause duration in the change outcome.

Bounded and reported, this is a legitimate operation. Unbounded and silent,
it is an outage. The outcome JSON should carry how long the namespace was
unavailable.

---

## Phases

**Phase 1 — make what exists visible and live.** Descriptors for
`history.mode`, `retain.duration`, `retain.commits`,
`governance.maintenanceSweep`, `governance.maintenanceMaxDefer` (Gap B).
Setters on `MaintenanceScheduler` + `liveSettings()` entries for the
maintenance knobs and `retain.*`. History mode reported, and refused for
now with the refusal pointing at Phase 4.
*Exit:* an operator can see every setting that governs reclamation, and
change the cadence ones from the UI with no restart.

**Phase 2 — reclaim modes.** `governance.reclaim` as the preset in §2,
plus the seal-triggered pass for `immediate`. Presets set the underlying
knobs, which stay individually settable; the UI shows the preset and what
it resolved to.
*Exit:* `immediate` demonstrably holds a smaller footprint than `lazy` on
the same workload, measured.

**Phase 3 — namespace reopen.** The drain-swap-report sequence in §4, wired
to `MutabilityNamespaceReopen` so the whole existing class becomes
changeable. Replace that refusal string with an implementation.
*Exit:* `cache.documentBytes` changes on a running server, with the pause
reported and bounded.

**Phase 4 — history mode switching, non-destructively.** The marker flip
plus `reclaim=manual` as the landing state, with Gap D resolved. Because
the switch destroys nothing, this no longer depends on Phase 3's reopen for
safety — only on the engine observing a mode change consistently.
*Exit:* a namespace switches `full`→`none` and back from the UI with no
data change either way, and reports how much would be eligible if compacted.

**Phase 5 — `compact history`, and the archive that makes it reversible.**
The explicit reclaim action, plus splitting local deletion from replica
deletion so an archive tier survives it (§3). Cold-read fallback to
`GetSegment`, and a restore that moves the floor back down.
*Exit:* with an archive tier configured, a compacted range is restorable
commit-for-commit; without one, `compact history` says plainly that it is
not.

Phase 3 is the structurally valuable one and is independent of the rest —
it frees six existing settings on its own. Phases 1, 2 and 4 form the
reclamation story and run in order. Phase 5 is the only one that needs new
storage behaviour rather than new wiring, and is the one to cut if the
archive turns out not to be wanted: without it, everything above still
works, and `compact history` is simply honest about being destructive.

---

## Traps

1. **`retain.*` going live changes what is already deleted, not just what
   will be.** Shortening the window makes previously-retained segments
   eligible on the very next pass. That is correct, and it is also
   irreversible, so the UI should say what a shortening will make eligible
   *before* applying it — a dry run that reports "this would make N
   segments (M bytes) eligible" is the right shape, and `planTruncation` can
   already compute it without deleting anything.
2. **A live `Interval` change must not lose the current sleep.** The loop
   selects on `after(Interval)`; changing the field mid-sleep does nothing
   until the pending timer fires. Either signal the loop or accept and
   document one stale interval.
3. **Persisted vs applied for per-namespace settings.** The config file is
   process-scoped and `meta.json` is per-namespace. A namespace-scoped
   change that "persists" must go to the marker, not the service file, or
   it will read back as drift forever.
4. **Do not let `immediate` mean "ignore the three-probe gate".** Immediate
   should mean "react to the seal event promptly", not "run a full pass
   every ten seconds regardless" — the gate is what keeps an idle namespace
   free, and it is orthogonal to how eager the cadence is.
5. **The confirmation belongs on `compact history`, not on the mode
   switch.** Putting it on the switch trains operators to click through the
   dialog that does not matter, so the one that does gets clicked through
   too.
6. **`none` + `manual` has unbounded disk**, which is the failure `none`
   exists to prevent. That is the correct default because it is *safe*, not
   because it is finished — so the UI has to show that the mode is armed but
   not reclaiming, and how much is eligible. Do not let a namespace sit in
   that state silently.
7. **Replica deletion currently destroys the archive.**
   `PrimaryWithReplicas.Delete` fans out to every replica, so today
   truncation deletes the S3 copy along with the local one. Any restore
   story depends on splitting those two, and the split is a real design
   decision: a *replica* mirrors the primary exactly (deletions included),
   an *archive* deliberately does not. Do not quietly make replicas stop
   honouring deletes — add the second role.
8. **A restore has to move the floor back down.** The checkpoint's
   `floorSequence` is what the open guard uses to tell "truncated as
   designed" from "damaged" - pulling segments back below the floor without
   lowering it leaves them ignored, and lowering it without the segments
   actually present turns a clean open into a hard failure.
