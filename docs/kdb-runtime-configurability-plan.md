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

**Phase 1 — make what exists visible and live. LANDED 2026-09-09.**

| what | where |
|---|---|
| `SetInterval` / `SetSweep` / `SetMaxDefer`, plus accessors | `embed/maintenance_loop.go` |
| `reconfigured` channel so a shortened interval does not serve out the pending sleep | same |
| `SetRetentionWindow` / `RawRetentionWindow` on the engine; `retainLive` override | `storage/engine/tree_objects.go`, `server_engine.go` |
| `EmbeddedKdbRuntime.SetRetentionWindow`, refusing `full` and read-only | `embed/history_features.go` |
| `liveRetention`, so the pass and the close-time checkpoint read the live value | `embed/file.go` |
| descriptors for `history.mode`, `retain.duration`, `retain.commits` | `config/descriptors.go` |
| `governance.maintenanceSweep` / `MaxDefer` through the full flag/env/file chain; `maintenanceInterval` reclassified `restart` → `live` | `config/service.go`, `descriptors.go`, `service/service.go` |
| `liveSettings()` entries for all five, `MaintenanceSource`, `maintenanceRegistry` | `control/settings_apply.go`, `control/server.go`, `service/service.go` |

Three decisions worth keeping:

- **Zero is refused, not interpreted.** A `0` interval reads as "stop
  maintaining" from a UI and as "use the default" in a struct literal;
  `maxDefer: 0` reads as "never defer" and means "default 6". Both are
  refused with a message naming the two readings, because a settings patch
  is not where a loop's lifecycle gets decided.
- **`retain.*` is read-modify-write per namespace**, against each
  namespace's *current* window rather than one rebuilt from the
  descriptors — `duration` and `commits` are halves of one value and a
  patch to one must not revert a live change to the other.
- **A change that reaches no scheduler and no namespace is an error**, not
  a success. "Applied" against nothing would report a setting as in force
  while changing nothing.

`history.mode` is described as `immutable` and refused, which is the honest
class *today* — the marker is checked at open and disagreeing is refused.
Phase 4 reclassifies it.

*Exit met:* every setting that governs reclamation is visible from
`/v1/settings`, and the cadence and window settings change on a running
server.

**Phase 2 — reclaim modes. LANDED.** `storage.ReclaimMode`
(`manual`/`immediate`/`balanced`/`lazy`) as a preset over the scheduler's
knobs, `reclaim.mode` live from the control plane, `ReclaimHeld` +
`EligibleSegments`/`EligibleBytes` on a held pass, and `CompactHistory` as
the explicit ask. `immediate` is driven by a seal notification from the
commit log rather than a shorter timer, and stops deferring to load.
*Exit:* met for behaviour (tests assert manual holds, balanced reclaims,
presets reach the scheduler). The footprint *measurement* comparing
`immediate` against `lazy` on one workload was not run.

**Phase 3 — namespace reopen. LANDED.** `Host.ReopenNamespace`,
`KdbServerRuntime.ReopenWith`, and the service's `namespaceReopener` tying
them together; five settings in the class are mapped to storage options and
apply through it. Draining is bounded and giving up is the safe outcome;
`ResumeAfterDrain` exists because `BeginDraining` is deliberately one-way at
shutdown. The Host now records each namespace's options so a reopen cannot
silently revert what it does not name.
*Exit met:* `cache.documentBytes` changes on a running server, with the
pause reported in the outcome.

**Phase 4 — history mode switching, non-destructively. LANDED.**
`EmbeddedKdbRuntime.SetHistoryMode` writes the marker, tells the engine, and
lands on `ReclaimManual` — an unspecified reclaim mode means manual here
rather than the global default, which is the line that makes the switch
reversible. `HistoryLost` is read from the segments on disk rather than
inferred from the mode. Gap D resolved: `historyModeOf` asks the engine
first and falls back to the policy only for a store that cannot answer.
*Exit met* at the runtime level; a control-plane endpoint for it is not
wired, so today the switch is an API call rather than a UI button.

**Phase 5 — `compact history`, and the archive. PARTLY LANDED.**

Done: `CompactHistory` (Phase 2). The `SinkRole` split —
`storio.SinkReplica` follows deletions, `storio.SinkArchive` does not — so a
compaction no longer destroys the copy that could restore it, which was the
blocker named in §3. `RestoreSegment` fetches a segment back from an archive
and writes it to the primary, refusing a write-only archive by name rather
than failing obscurely. `HasArchive` is plumbed up to `TruncationResult.
Reversible`, so a compaction can say which of the two operations it is.

Not done, and each weakens the claim: **nothing constructs an archive from
configuration** — the role exists and is tested, but no flag, env var or
config field puts a sink in it, so a deployment cannot turn one on yet.
**The cold-read path does not fall back to the archive**, so a read below
the floor still fails rather than fetching. **No restore moves the floor
back down**, which is the operation an operator would actually run; trap 8
below still applies to whoever writes it.

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
