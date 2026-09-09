# Runtime configurability: change settings without a restart

**Status:** plan, nothing implemented. Written 2026-09-09 against
`feat/scheduled-maintenance-loop` (PR #51).

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
| `governance.reclaimMode` | *(absent)* | **live** | new; see §2 |
| `retain.duration` / `retain.commits` | *(absent)* | **live** | descriptor + setter; read per pass already |
| `history.mode` | *(absent)* | **namespace-reopen**, then live for `full`→`none` | §3 |
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
| `immediate` | 10s | 1m | no | 0 | reclaim on segment seal |
| `balanced` (default) | 5m | 30m | yes | 6 | — |
| `lazy` | 30m | 6h | yes | 24 | — |
| `off` | — | — | — | — | reclaim only at close |

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

The two directions are not symmetric and should not be presented as if they
were.

**`full` → `none` is tractable, and mostly already written.** What
`MigrateHistoryMode` does offline is: take the lock, run
`PrepareForTruncation`, set `historyStrategy=replay`, write the marker
(`embed/migrate_history.go:116`). The expensive step — making every live
body durable outside the log — is a method the running maintenance loop
*already calls on a live engine every pass*. The remaining pieces are:

- a setter for the engine's mode, with every reader observing the change
  consistently (today it is read from config at open);
- stopping tree-object writes mid-flight, which the offline path already
  declares safe: existing objects become "dead weight, exactly as
  `MigrateHistoryStrategy` leaves them";
- marker ordering: bodies durable **then** marker written, so a crash in
  between leaves `full`, which never deletes anything. Same ordering as the
  offline path, and safe in the same way.

**`none` → `full` is a one-way door in the other direction and must be
labelled as one.** Segments are already deleted; nothing can resurrect
them. The offline path just flips the marker. So the honest description is
*"stop reclaiming from here on"*, **not** *"restore history"*, and the UI
must say so before it accepts the change. It also leaves the namespace on
`replay` — valid for `full`, just slower for historical reads, and
recovering `objects` is a real migration.

**Recommended staging:** `full`→`none` first, as `MutabilityNamespaceReopen`
(Gap C), before attempting it live. A reopen gets the correct result with
the existing, tested offline code path plus a drain, and it is a much
smaller correctness surface than mutating an open engine's mode. Promote it
to live only if the reopen pause proves unacceptable.

**Resolve Gap D as part of this.** Either the policy's `HistoryMode`
becomes a projection of the marker (read-only, reported), or writing it
becomes the thing that triggers the migration. Two independently-writable
fields that disagree is the worst of the three options, and is what exists
today.

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
maintenance knobs and `retain.*`. History mode reported but refused, with
the refusal naming the migration.
*Exit:* an operator can see every setting that governs reclamation, and
change the cadence ones from the UI with no restart.

**Phase 2 — reclaim modes.** `governance.reclaimMode` as the preset in §2,
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

**Phase 4 — history mode switching.** `full`→`none` via Phase 3's reopen,
with Gap D resolved. `none`→`full` with the one-way-door warning. Live
(no-reopen) switching only if Phase 3's pause proves unacceptable in
practice.
*Exit:* a namespace converts `full`→`none` from the UI and its next
maintenance pass reclaims correctly.

Phases 1 and 2 are independent of 3 and 4 and deliver most of the day-to-day
value; 3 is the structural one; 4 depends on 3.

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
5. **`none`→`full` in the UI needs a confirmation that names the loss.** An
   operator who reads "full history" as "history is back" has been
   misinformed by the label, not by the code.
