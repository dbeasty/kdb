# KDB Component Spec — Layer 15
## Corruption Detection, Repair, Backup, and Hybrid Restore
### Components 58–62

**Depends on:** Layer 4a (WAL, SSTable, delta writer, platform IO), Layer 6 (compaction engine),
Layer 8 (peer sync), Layer 10 (inspect tooling), Layer 12 (peersync conflict detection, Go-native
server), **Layer 13 Component 47 (durable restart contract — hard prerequisite, see §2.4)**

-----

## 1. Purpose

kdb has no way today to answer "is this database intact?" short of trying to open it and seeing
whether it throws, and no way to recover from "no, it isn't" short of hand-editing files. Every
commit is already content-addressed (SHA-256, chained through parents) and every delta frame
already carries a CRC32 — the primitives for integrity exist, but nothing walks them end to end,
nothing repairs what it finds, and there is no backup format at all, only a replication sink that
happens to copy bytes to S3.

This layer builds four things on top of what already exists, in strict dependency order:

1. **Verification** (Component 58) — walk the primitives that already exist (frame CRCs, commit
   hashes, parent closure, content hashes) and produce a precise report of what is wrong and
   where. This requires no format changes and is safe to ship first.
2. **Repair** (Component 59) — turn that report into action: truncate torn tails, quarantine bad
   bytes, migrate legacy layouts, and give every sealed segment a durable identity via a
   manifest. This finally implements `kdb-inspect repair-segments`, which two error messages in
   the current codebase already promise and which does not exist.
3. **Backup** (Component 60) — turn the existing S3 replication sink into something with a
   verifiable, restorable shape: a manifest that names a consistent set of objects, incremental
   uploads, and a `backup verify` that never requires a restore.
4. **Restore, including hybrid restore** (Component 61) — rebuild a data directory from any
   combination of a damaged local log, a backup, and one or more peers, unioned by commit hash.
   "Hybrid" is not a special mode here; every restore is a union of whatever verified sources are
   available, and a plain restore from backup is just the case where the local-log source is
   empty.
5. **Peer backfill and recovery pinning** (Component 62) — fix peer sync so it can actually be
   used as a recovery source for a node that is far behind or empty, and make sure a recovering
   node's needed history is not compacted away while it catches up.

The throughline: **detect before repairing, repair before restoring, and restoring is just
verified union**. Nothing in this layer trusts an unverified byte, and nothing destroys evidence
— every repair and every restore is reversible until the operator confirms it.

-----

## 2. Findings

### 2.1 GAP: `kdb-inspect repair-segments` is promised twice and exists nowhere

`go/kdb/embed/delta_replay.go:60` and `go/kdb/storage/delta/writer.go:266` both construct error
messages telling the operator to run `kdb-inspect repair-segments` — the first when replay hits a
non-monotonic or legacy segment layout, the second when `openWriter`/`listSegments` refuse a
UUID-named (pre-Layer-13) segment directory. `go/cmd/kdb-inspect/main.go:21` has exactly one case,
`dump-wire`. Any directory that trips either check today is permanently stuck: the tool the error
tells you to run does not exist. Layer 13 §4.1 already specifies this command's job (migrate
UUID-named segments to sequence order by walking parent links) but no implementation shipped with
it.

### 2.2 GAP: torn tails are detected but never repaired

Layer 13 Component 47 (§4.3, uncommitted at the time of writing — see `DeltaSegmentScanner.kt:47-80`
/ `go/kdb/storage/delta/scanner.go:49-96`) correctly *stops* scanning at a CRC mismatch in the
final frame of the final segment and treats it as a torn write. But nothing then truncates the
segment: the bad bytes stay on disk forever, every future open re-tolerates the same tail, and a
second real write after the torn one would be appended past garbage rather than over it. There is
also no quarantine — if the "torn tail" classification is ever wrong (see §2.4's ordering
dependency), the evidence needed to tell is not preserved anywhere separate from the live segment.

### 2.3 GAP: sealing is a no-op; there is no segment manifest

`go/kdb/storage/io/os_store.go:161-164`:

```go
func (s *OSByteStore) MarkSealed(segmentName string) error {
	// v1: sealing is advisory; persisted segments are discovered by listing + scanning.
	return nil
}
```

Nothing on disk distinguishes a segment that was cleanly sealed from one a crashed process left
mid-write. There is no per-namespace catalog of segments, sizes, or content hashes — every open
re-derives everything by listing the directory and scanning every frame of every file. This means:
(a) verification has no cheap path — it must always re-read full segment bytes; (b) a backup has
no natural unit to catalog — "back up the segments not yet backed up" requires a stored identity,
and there is none; (c) after the Layer 13 diff, `SegmentID` is no longer even stable across opens
(regenerated per listing at `go/kdb/storage/delta/writer.go:236,244` and the corresponding scan in
`DefaultDeltaSegmentWriter.kt`) — the only stable identity a segment has today is its sequence
number, which says nothing about its content.

### 2.4 Prerequisite: Layer 13 Component 47

Every component in this layer assumes segments are monotonically sequenced, replay is
topological, and frame CRCs are verified — i.e. Layer 13 §4.1–§4.3. Those changes are present in
the working tree as an uncommitted diff at the time of writing. Component 59's repair logic (§4)
in particular special-cases "torn tail in the last segment" versus "corruption elsewhere" using
exactly Layer 13 §4.3's classification; if that classification is wrong, everything built on top
of it repairs the wrong thing. **Component 47 must be committed and its test suite green before
any part of this layer ships.**

### 2.5 GAP: the S3 replica sink is replication, not backup

`go/kdb/storage/io/s3/replica_sink.go` (via `ReplicaSink` / `PrimaryWithReplicas`,
`go/kdb/storage/io/replica.go`, `primary_replicas.go`) mirrors sealed segments and byte-store
snapshots to `s3://{bucket}/{prefix}/ns/...` when `KDB_S3_BUCKET` is set
(`go/kdb/embed/file.go:17,33-43`). It has no manifest, so there is no way to ask "does this bucket
currently hold a restorable, consistent copy of namespace X?" without listing every object and
guessing. It has no verification — an object could be truncated by a failed multipart upload and
the sink would not know. It has no retention policy — objects accumulate forever. And it has no
restore path at all: nothing in the repo reads these objects back. This is upload-only plumbing,
not a backup system, and it is Go-only — there is no Kotlin equivalent.

### 2.6 GAP: peer sync cannot rebuild a node that is more than trivially behind

`PullMissing` (Go `go/kdb/peersync/client.go:209`, Kotlin `PeerSyncClient.kt:163`) issues exactly
one `CommitFetch` with `maxCommits` defaulting to 100 (`PeerSyncClient.kt:121`,
`PeerSyncTypes.kt:75`, Go `client.go:218`) and there is no continuation loop. The host walks
backward from its head and stops at 100 commits or at the requester's stated head, whichever comes
first (`PeerSyncFrameHandler.kt:189-198`, `go/kdb/peersync/host.go:212-228`). If the requester is
more than 100 commits behind, the batch it receives has an oldest member whose parent is not
locally present, and `dag.putCommit(commit, requireParents=true)` throws
(`InMemoryCommitDag.kt:134-144`, `go/kdb/dag/in_memory_commit_dag.go:165-173`). A node rebuilding
from empty — the exact scenario a corruption-recovery tool needs — cannot use peer sync as-is
once the peer is more than 100 commits ahead. Layer 13 §9.3 already specifies the intended fix
("log-offset replication": a peer tracks a `(segmentSeq, frameOffset)` cursor and "send me
everything after X" becomes a resumable sequential log read) but it is unimplemented.

### 2.7 GAP: peer heads are never persisted, so recovery pins nothing

`CommitDag.compactableBefore(boundary, peerHeads)` (`kdb-dag/.../CommitDag.kt:134-137`,
`InMemoryCommitDag.kt:525-555`) already refuses to compact history at or above any hash in
`peerHeads`, and `InProcessCompactionCoordinator.updatePeerHeads` (`CompactionCoordinator.kt:41-49`)
is the intended way to feed it. But a repo-wide search shows `updatePeerHeads` /
`updatePeerHead` / `peerHeads()` have no callers anywhere outside the compaction module itself —
peer sync never reports what it learns about a peer's head, and nothing persists it across
restarts. Go's compaction engine does not even implement the check (`go/kdb/compaction/engine.go:52`
is a stub). Practically: a node that is offline or mid-recovery pins nothing, so the exact history
it needs to catch up on can be compacted away while it is down.

### 2.8 GAP: blobs and attachments are outside every durability story in scope here

Peer sync transfers commit records only — `KdbOp.FileWrite` carries a `blobHashHex`, never the
bytes (`kdb-wire/.../WireOpMapper.kt:28,52`, `go/kdb/wire/op_mapper.go:19-57`) — and the S3 replica
sink covers segments and snapshots, not the blob chunk store (`kdb-storage-chunking`). A restore
that only replays the commit log will produce documents whose file attachments 404. This layer's
backup and restore components must therefore explicitly include the blob chunk store as a backup
unit, not assume it comes along for free.

### 2.9 Related, already documented: SSTable CRC/footer never verified

Layer 14 §2.7 already documents that SSTable block CRC32 and the footer's file-level SHA-256 are
written but never read back (`SsTableCodec.kt:24-29` / `go/kdb/storage/sstable/codec.go:36-44`),
and notes Layer 14's AEAD work fixes it as a side effect for encrypted stores. This layer's
verification component (§4) closes the same gap for **plaintext** stores, since Layer 15 does not
assume Layer 14 has shipped.

### 2.10 Out of scope, noted for a future format revision: authorship is not persisted

`DeltaRecord.authorship` and `documentPatches` are modeled (`DeltaTypes.kt:17-48`) but never
written to a frame — only `commitPayload` is (`DefaultDeltaSegmentWriter.kt:84-97`,
`go/kdb/storage/delta/page_codec.go:11-35`). On read they are synthesized as
`principal="unknown"` with empty patches. This has no bearing on corruption detection, repair, or
restore — a restored log is exactly as authorship-blind as the log it was restored from — so it is
listed here for completeness and left to a future frame-format revision (§8 Non-goals).

-----

## 3. Design principles

- **P1 — The log is the database; one format, many views.** Verification, repair, backup, and
  restore all operate on the same sequenced, CRC-framed, content-addressed delta log described in
  Layer 4a and hardened by Layer 13. None of them introduces a second source of truth.
- **P2 — Detect, then repair, then restore — never skip a step.** Repair only ever acts on what
  verification proved wrong. Restore only ever replaces what repair could not fix locally. A tool
  that "restores" without first verifying what's salvageable throws away good data.
- **P3 — Never guess; never destroy evidence.** Anything ambiguous refuses the operation and names
  the exact command to run next (mirrors Layer 13 P3). Repair quarantines removed bytes — renames
  them aside — rather than deleting. Restore renames the pre-restore directory aside rather than
  overwriting it. Every tool in this layer is safe to run twice.
- **P4 — Auto-heal only what is provably safe; everything else is operator-driven.** A torn tail
  in the last segment is, by construction under Layer 13's durable-before-visible ordering, a
  commit that was never acknowledged — truncating it destroys nothing the client believes exists.
  Refetching a frame whose exact bytes are attested by a verified backup or peer is provably safe
  in the same sense. Anything where "safe" depends on an assumption that might be wrong — mid-log
  corruption, divergent heads, a manifest that disagrees with the bytes on disk — stops and asks.
- **P5 — Recompute, don't trust.** Every verification step recomputes a hash or checksum from
  bytes and compares; it never takes a stored value's word for it. A manifest is a cache of what
  verification already proved, not a new source of trust — a missing or stale manifest degrades to
  a full re-scan, never to a refused-but-otherwise-fine open.
- **P6 — Restore is verified union.** Every restore, hybrid or plain, is the same algorithm: take
  every available source (damaged local log, backup, peers), verify each frame independently, and
  apply the union topologically by commit hash. A plain restore from backup and a hybrid restore
  that also salvages the local log are the same code path with a different set of inputs — not two
  designs.
- **P7 — Consistent with Layer 13.** This layer inherits P1–P6 of Layer 13 (crash-only durability,
  no repair-on-open by default, sealing as optimization not correctness) and must not reintroduce
  a "silent auto-fix" path that those principles ruled out.

-----

## 4. Component 58 — Integrity Verification

**Goal:** answer "is this database intact?" precisely — which segment, which offset, which commit,
what kind of mismatch — without guessing, using only primitives that already exist on disk.

### 4.1 Three levels

- **L1 — Physical.** For every delta segment: re-derive frame boundaries from the length fields,
  verify the `KDBP` magic and CRC32 of every frame (the check Layer 13 §4.3 already performs
  during replay — L1 verification is that same check exposed as a standalone, read-only pass over
  every segment, sealed or not, not just the ones a given open happens to touch). For every
  SSTable: verify each block's CRC32 and the footer's whole-file SHA-256 (closing §2.9). No
  decompression or semantic interpretation happens at this level — a plaintext-mode L1 pass can
  run on a store it does not otherwise understand.
- **L2 — Logical.** Recompute every commit's SHA-256 from its payload bytes and confirm it matches
  what the payload claims (mirrors the check `putCommit` already performs at the DAG boundary,
  `in_memory_commit_dag.go:158-164`, but run standalone over the whole log rather than only on
  newly-arriving commits). Confirm parent closure: every parent hash referenced by any commit is
  present somewhere in the namespace's log. Confirm every branch/tag head is reachable. Confirm
  segment sequence continuity (no gaps, no duplicate sequence numbers).
- **L3 — Semantic.** Re-materialize each namespace's document tree using the existing
  `SnapshotMaterializer` (`kdb-compaction/.../SnapshotMaterializer.kt:14`) and confirm the
  resulting tree's content hash matches what replay produces through the normal path. Verify every
  blob referenced by a `FileWrite` op resolves to a chunk manifest whose chunks hash-check
  (`kdb-storage-chunking/.../BlobManifest.kt`). L3 is the expensive level — full document
  reconstruction — and is opt-in.

Levels are strictly additive and independently selectable: L2 assumes L1 passed for the bytes it
reads; L3 assumes L2. A verification run reports the highest level it completed and every finding
at or below that level.

### 4.2 Output contract

A structured report (JSON), one object per finding:

```
{
  "namespace": "...",
  "level": "L1" | "L2" | "L3",
  "segment": "00000000000000000003.seg",
  "offset": 40960,
  "classification": "torn_tail" | "mid_log_corruption" | "missing_parent" |
                     "hash_mismatch" | "unreachable_head" | "sequence_gap" | "blob_missing",
  "expected": "<hash or size, if applicable>",
  "actual": "<hash or size, if applicable>",
  "commit_hash": "<if resolvable>"
}
```

plus a top-level summary (segments/commits/blobs scanned, findings by classification, overall
verdict: `clean` / `repairable` / `needs_restore`). This report is the sole input contract for
Component 59 — repair never re-derives its own opinion of what is wrong, it acts on a verification
report (possibly one it generates itself as a first step).

The `torn_tail` vs. `mid_log_corruption` classification is exactly Layer 13 §4.3's rule (final
frame of the final segment vs. anywhere else), computed once here so Component 59 does not
duplicate it.

### 4.3 Where it lives

New package `go/kdb/integrity` (engine) with a corresponding `dev.kdb.integrity` Kotlin module for
parity in a later phase (§6). Exposed two ways:

- `go/cmd/kdb-inspect`: new `verify` case taking `--data-dir --namespace [--level L1|L2|L3]
  [--json]`. This is where the tool naturally lives per the existing `dump-delta`/`dump-wire`
  precedent (`kdb-inspect/.../InspectMain.kt:20`, `go/cmd/kdb-inspect/main.go:21`).
- `go/cmd/kdb`: `kdb verify [--namespace ...]` as a convenience alias defaulting to L1+L2 against
  the current data directory — the common "did my restart just work" check, analogous to `kdb
  status`.

### 4.4 Continuous scrub (specified, deferred to execution)

An optional background L1 pass in `kdb-service`, rate-limited (bytes/sec budget, reusing Layer
13's CPU-governance token bucket once it ships) and resumable (persists a scrub cursor so a
restart continues rather than restarting). Emits the same metrics discipline Layer 13 established
(§13 there): scrub bytes/sec, findings by classification, cursor position. This is fully specified
here but implemented in a later execution phase (§6) since it depends on nothing this layer
introduces except the L1 engine itself.

### 4.5 Tests

| # | Name | Expected |
|---|---|---|
| 1 | `verifyCleanLogPassesAllLevels` | A freshly written, cleanly closed log passes L1+L2+L3 with zero findings |
| 2 | `verifyDetectsSingleFlippedBit` | One flipped byte in a mid-log frame is reported at the exact segment+offset, classified `mid_log_corruption` |
| 3 | `verifyDistinguishesTornTailFromCorruption` | A truncated last frame classifies as `torn_tail`; the same truncation replayed into a non-final segment classifies as `mid_log_corruption` |
| 4 | `verifyDetectsMissingParent` | A commit referencing a parent hash absent from the log is reported at L2 without touching L1 findings |
| 5 | `verifySstableBlockAndFooterCrc` | A flipped byte in an SSTable block, and a flipped byte in the footer hash region, are both detected (closes §2.9) |
| 6 | `verifyL3DetectsBlobMismatch` | A blob chunk whose bytes no longer hash to its manifest entry is reported only at L3 |
| 7 | `verifyReportIsRepairInput` | Running repair (Component 59) against a verify report produces the same fix as running repair with `--verify-first` |
| 8 | `verifyScrubResumesAfterRestart` | A scrub cursor persists across a service restart and does not re-scan already-clean bytes |
| 9 | `verifyNeverMutatesOnDisk` | A verify run's file mtimes/hashes are unchanged before and after (read-only guarantee) |

-----

## 5. Component 59 — Repair, Quarantine, and the Segment Manifest

**Goal:** implement the command two error messages already promise, acting only on what
verification (§4) proved, and never discarding evidence.

### 5.1 The segment manifest

Per-namespace `delta/MANIFEST` (JSON, versioned by a top-level `formatVersion`), one entry per
sealed segment:

```
{ "sequence": 3, "sizeBytes": 40960, "frameCount": 512,
  "firstCommitHash": "...", "lastCommitHash": "...",
  "fileSha256": "...", "sealed": true }
```

Written at seal time (finally giving `MarkSealed` a body — replaces the §2.3 no-op). **Advisory
only**, per P5: a missing, stale, or corrupt manifest never blocks an open or a verify — it
degrades to a full re-scan and the manifest is regenerated. What it buys: O(1) "has this segment
been backed up" checks (a stable identity keyed on sequence + `fileSha256`, closing the identity
gap in §2.3), and an O(hash-per-sealed-file) fast path for repeat verification instead of
O(bytes-per-frame).

### 5.2 `kdb-inspect repair-segments`

`--data-dir --namespace [--dry-run] [--verify-report <path>]`. Three cases, matching the
classifications Component 58 already produces:

- **Legacy UUID-named directory** (Layer 13 §4.1's migration case): derive true commit order by
  walking parent links from every branch/tag head backward, rewrite as sequentially-named segments
  in that order, write the manifest, leave the original files renamed `*.legacy` rather than
  deleted. This is the direct fix for the "existing multi-segment directories may already be
  unopenable" problem Layer 13 §4.1 flags.
- **Torn tail** (`classification: torn_tail`): copy the bytes from the last good frame boundary
  onward into `<segment>.quarantine` (append-only, never overwritten, so repeated repair attempts
  accumulate distinct quarantine files rather than clobbering one), then truncate the live segment
  file to the last good frame boundary. This is the fix §2.2 identifies as missing from Layer 13's
  detection-only behavior.
- **Mid-log corruption** (`classification: mid_log_corruption`): quarantine the entire segment
  file (copy aside, do not touch the original until the next step succeeds), then re-run L2
  verification against the log *with the bad frame excluded*. If parent closure and head
  reachability still hold — i.e. the bad frame's commit(s) are not the only path to some
  reachable state — rewrite the segment without that frame and update the manifest. If closure
  does not hold, **do not repair**: report the exact missing commit hashes and instruct the
  operator to run `kdb restore` (Component 61), which is the only thing in this layer allowed to
  pull replacement bytes from elsewhere.

Repair is idempotent (P3): re-running it against an already-repaired directory finds nothing left
to do and exits clean. `--dry-run` produces the same report `verify` would, annotated with what
repair *would* do, without touching any file.

### 5.3 Auto-heal at open (the operator-approved policy)

Per the design principle in §3 P4, wired into the existing open path
(`DeltaNamespaceReplayer.kt` / `go/kdb/embed/delta_replay.go`):

- **Torn tail on the last segment** → automatically apply exactly the torn-tail repair above
  (truncate + quarantine), log at WARN with segment and offset, continue opening. This upgrades
  Layer 13's current "tolerate and keep the garbage" behavior to "tolerate and clean up," with the
  removed bytes preserved in quarantine per P3.
- **Everything else** (mid-log corruption, legacy layout, manifest/file mismatch on a sealed
  segment) → refuse to open, and the error names the exact `kdb-inspect repair-segments` (or `kdb
  restore`) invocation to run, finally making good on the promise in §2.1. This is unchanged from
  Layer 13's existing refusal behavior for legacy segments — this component only adds the missing
  tool the refusal points to, and extends the same refuse-and-name-the-tool discipline to mid-log
  corruption.

A `--no-auto-heal` flag disables even the torn-tail case for operators who want every repair to be
explicit; default is on, matching the approved policy.

### 5.4 Tests

| # | Name | Expected |
|---|---|---|
| 1 | `repairMigratesLegacyUuidDirectory` | A UUID-named multi-segment directory (Layer 13 §2.1's failure case) opens successfully after repair, in correct commit order |
| 2 | `repairTornTailTruncatesAndQuarantines` | Half-written final frame: repair truncates the segment, and the removed bytes are recoverable from the quarantine file |
| 3 | `repairMidLogRemovesOnlyProvenSafeFrame` | A corrupt frame whose commit has no other path to a reachable head is removed; the segment reopens clean |
| 4 | `repairRefusesWhenClosureBreaks` | A corrupt frame whose commit is the only path to a reachable head: repair makes no change and names the missing commit hashes |
| 5 | `repairIsIdempotent` | Running repair twice in a row produces no second change and no error |
| 6 | `repairNeverDeletes` | Every byte repair removes from a live segment exists afterward in some quarantine file |
| 7 | `autoHealTornTailOnOpen` | Opening a directory with a torn last-segment tail auto-repairs and opens, logging a WARN |
| 8 | `autoHealRefusesMidLogCorruption` | Opening a directory with mid-log corruption refuses and names `repair-segments` |
| 9 | `manifestSurvivesDeletion` | Deleting `MANIFEST` and reopening regenerates it with identical content (advisory-only, per P5) |
| 10 | `manifestDetectsSealedFileMismatch` | A sealed segment whose bytes no longer match its manifest `fileSha256` is reported as mid-log corruption, not silently trusted |

-----

## 6. Component 60 — Backup to Object Storage

**Goal:** turn the existing S3 replication sink into something with a verifiable, restorable
shape.

### 6.1 Backup manifest

A backup is one JSON object, uploaded to
`s3://{bucket}/{prefix}/backups/{namespace}/{backupId}/manifest.json`, written **last** so that a
backup's existence is defined by its manifest's presence (P3: nothing reads a backup as valid
until its manifest says so):

```
{ "formatVersion": 1, "namespaceId": "...", "backupId": "...",
  "baseBackupId": null | "<previous backupId, for incrementals>",
  "createdAt": "...", "headHashes": {"main": "..."},
  "segments": [{"sequence": 3, "fileSha256": "...", "sizeBytes": 40960, "key": "ns/.../delta/..."}],
  "activeSegmentPrefix": {"sequence": 4, "frameCountVerified": 210, "key": "..."},
  "blobs": [{"hash": "...", "sizeBytes": ..., "key": "..."}] }
```

`segments` covers sealed segments only (immutable once sealed, per Layer 13 P4). The active
unsealed segment is backed up separately as a verified prefix — every frame up to the last
CRC-checked offset (never the tail past it, since that portion may still be a torn write in
progress) — recorded in `activeSegmentPrefix`.

### 6.2 Full and incremental

Segments are immutable and sequenced, so incremental backup is exactly: upload sealed segments and
blob chunks not already present in `baseBackupId`'s manifest, re-upload the active segment prefix
(cheap — it's bounded by `MaxSegmentBytes`), write a new manifest referencing the base. This reuses
the existing S3 layout (`s3://{bucket}/{prefix}/ns/...`) as the payload location — the manifest is
new, the upload path is not.

### 6.3 Verify without restoring

`kdb backup verify --backup-id <id>`: re-download the manifest, confirm every listed object exists
at its recorded size, and (configurable depth) either spot-check a sample or fully re-hash every
object against `fileSha256`/blob hash. This is the S3-side analog of Component 58's L1 — same
principle (recompute, don't trust), applied to backup objects instead of live segments.

### 6.4 Retention

Keep-last-N manifests (configurable) per namespace. Because everything is content-addressed,
garbage collection is mark-and-sweep: union the object references of every retained manifest,
delete anything in the bucket prefix not referenced. GC only ever runs after verify confirms the
retained manifests are themselves valid — never blind deletion by age.

### 6.5 Tests

| # | Name | Expected |
|---|---|---|
| 1 | `backupManifestNamesConsistentSet` | Every object a manifest references exists and matches its recorded hash immediately after backup completes |
| 2 | `incrementalOmitsUnchangedSegments` | A second backup after no new writes uploads zero new segment objects, only a new manifest |
| 3 | `incrementalIncludesOnlyNewSealedSegments` | After N new commits seal one segment, the incremental backup uploads exactly that segment plus the new active prefix |
| 4 | `backupExcludesUnflushedTail` | Bytes past the last CRC-verified offset of the active segment are never uploaded |
| 5 | `backupVerifyDetectsTruncatedUpload` | An object truncated mid-upload (simulated) is caught by `backup verify` without needing a restore |
| 6 | `backupIncludesBlobChunks` | A document with a file attachment backs up and verifies the referenced blob chunks (closes §2.8) |
| 7 | `retentionGcOnlyRemovesUnreferenced` | After GC, every object referenced by a retained manifest still exists; nothing else does |
| 8 | `gcRefusesIfAnyRetainedManifestFailsVerify` | GC aborts rather than deletes if a manifest it would keep does not verify |

-----

## 7. Component 61 — Restore and Hybrid Restore

**Goal:** rebuild a data directory from the union of whatever verified sources are available —
damaged local log, backup, peers — applied topologically by commit hash. Per P6, there is one
algorithm, not two.

### 7.1 Sources

A `RestoreSource` supplies verified commit frames plus blob chunks:

- **Local** — the current (possibly damaged) data directory, filtered through Component 58's L1
  verification: only frames that pass CRC are contributed; everything else is left in the
  preserved-aside original (§7.4), never silently dropped, never silently trusted.
- **Backup** — an S3 backup manifest (Component 60), downloaded and verified before any of its
  content is used.
- **Peer** — a live node, via Component 62's backfill protocol.

### 7.2 The union algorithm

1. Collect every available source's contribution: for Local, every CRC-verified frame's commit;
   for Backup, everything named by its manifest; for Peer, everything the backfill protocol
   supplies.
2. Deduplicate by commit hash (commits are content-addressed, so the same commit from two sources
   is trivially recognized as identical — no merge logic needed at this step).
3. Apply the union to a fresh, empty DAG using the **same round-based topological applier** Layer
   13 introduced for replay (`applyCommitsTopologically`,
   `go/kdb/embed/delta_replay.go:88-124`, `DeltaNamespaceReplayer.kt:29,86,115`): repeat "apply
   every commit whose parents are now present" until a round makes no progress. What remains
   unapplied after that is a precise list of missing commit hashes — the same shape of answer
   Component 59's mid-log-corruption case produces, and the trigger for widening the source set
   (add a peer, or fail with that exact list if none is available).
4. Write the result as a fresh, sequenced, manifested log in a new directory. The original damaged
   directory is renamed `<dir>.pre-restore-<timestamp>`, never overwritten (P3) — an operator can
   always compare or recover something restore missed.

"Plain restore from backup" is this same algorithm with an empty Local source. "Hybrid restore" —
the case the user specifically asked for, local-log-as-base plus backup plus peers — is the same
algorithm with all three populated; the local log usually contributes everything *after* the last
backup (since backups lag live writes), and the backup contributes the durable base the local log
may be missing due to the corruption that prompted the restore in the first place.

### 7.3 Divergent heads

If, after union, two contributing sources have heads that are genuinely divergent (neither is an
ancestor of the other — not just "one is behind"), restore does not silently pick one. It lands
the local side on a side branch (reusing the per-host-branch pattern Layer 12 §9 already suggests
for exactly this situation) and reports the same `ConflictReport` shape peer sync already produces
for divergence (`buildConflictReport`, `PeerSyncConflictDetection.kt:245-278`), so an operator
resolves it the same way they would resolve a peer-sync conflict. Restore never moves a head
without either fast-forward being provably safe or an explicit operator merge.

### 7.4 Point-in-time restore

`--at <commitHash|timestamp>` bounds the applied set to ancestors of the target, reusing
`SnapshotMaterializer.materializeAt` (`kdb-compaction/.../SnapshotMaterializer.kt:14`) for the
materialization step once the log itself is rebuilt.

### 7.5 CLI

`kdb restore --data-dir <target> [--backup-id <id>] [--from-local <damaged-dir>] [--peer
<uri>]... [--at <commit|timestamp>] [--dry-run]`. `--dry-run` runs the full union and reports what
would be applied and what would remain missing, without writing anything — the restore-side
analog of repair's `--dry-run`.

### 7.6 Tests

| # | Name | Expected |
|---|---|---|
| 1 | `restoreFromBackupOnlyRebuildsCleanly` | Empty local dir + backup → identical head hashes to the source at backup time |
| 2 | `hybridRestoreLocalAheadOfBackup` | Damaged log with a good prefix, stale backup: restore reconstructs up through the local log's last good commit, not just the backup's |
| 3 | `hybridRestoreFillsGapFromPeer` | Local log missing a middle segment, backup also missing it, a peer has it: union completes only when the peer source is included |
| 4 | `restoreFailsPreciselyWhenNoSourceHasIt` | No source can supply a required commit: restore fails naming exactly that hash, applies nothing |
| 5 | `restoreNeverTrustsUnverifiedLocalFrames` | A corrupt frame in the local log is excluded from the union even though it would parse under CompressionNone |
| 6 | `restorePreservesOriginalDirectory` | After restore, the pre-restore directory exists unmodified under its renamed path |
| 7 | `hybridRestoreDivergedLandsSideBranch` | Local and backup heads are true divergences: restore creates a side branch and a conflict report, does not overwrite either head |
| 8 | `restorePointInTimeMatchesLiveMaterialization` | `--at <hash>` produces a document tree identical to materializing that commit on an undamaged copy |
| 9 | `restoreDryRunMakesNoChange` | `--dry-run` output matches the real restore's plan; no file is written |
| 10 | `restoreIsIdempotentOnRetry` | Running restore again after a partial failure (simulated) does not duplicate or corrupt the target directory |

-----

## 8. Component 62 — Peer Backfill and Recovery Pinning

**Goal:** make peer sync usable as a real recovery source (fixing §2.6), and stop a recovering
node's needed history from being compacted away while it catches up (fixing §2.7).

### 8.1 Log-offset backfill

Implements Layer 13 §9.3 as specified: `LogFetch(namespace, afterSegmentSeq, afterOffset,
maxBytes)` served directly from the sender's sequenced, manifested log — a resumable sequential
read, not a DAG walk. The receiver tracks a cursor `(segmentSeq, frameOffset)` and resumes from it
across restarts (Layer 13 §9.5), so backfill of a large history is a bounded loop of bounded
requests rather than the single capped `CommitFetch` that limits `PullMissing` today. Received
frames are CRC-verified (Component 58 L1) before being handed to the topological applier, and
applied via the same round-based applier Component 61 uses, so a receiver can start applying
before backfill completes (unlike today's `requireParents=true` all-or-nothing apply).

### 8.2 Continuation for the existing DAG-diff path

Independently of log-offset backfill (which requires the sender to also support it), fix
`PullMissing`'s single-shot limitation directly: loop `CommitFetch` with `sinceHash` advancing to
the oldest commit received each round, until the receiver's head matches the sender's or no
progress is made. This closes §2.6 for peers that only implement the existing v1 protocol,
independent of whether §8.1 has shipped on the sender.

### 8.3 Backup as a recovery source, uniformly

`RestoreSource` (§7.1) and the backfill transport share one interface boundary: a `RecoverySource`
that answers "give me everything after cursor X" whether X is a `(segmentSeq, frameOffset)` pair
served by a live peer, or a backup manifest's segment list served by S3, or a local quarantined
segment. This is what makes "restore from a backup, then fill the rest from a peer" a matter of
supplying two sources to the same union algorithm rather than two different code paths.

### 8.4 Recovery pinning

A node's own last-known-durable head (from its manifest) is what it advertises to peers on
reconnect; a peer that receives it persists it (fixing §2.7 — `updatePeerHeads` finally gets a
real caller) and feeds it to `compactableBefore` so that a peer's needed history is not compacted
away while that peer is offline or catching up. Pins expire after a configurable age (a peer gone
long enough is presumed decommissioned, not indefinitely protecting history) and are visible via
`kdb status` so an operator can see what's pinning retention.

### 8.5 Tests

| # | Name | Expected |
|---|---|---|
| 1 | `backfillResumesFromCursor` | Interrupt backfill mid-stream; resuming continues from the last applied `(segmentSeq, frameOffset)`, no re-fetch, no gap |
| 2 | `backfillAppliesBeforeCompletion` | Commits already covered by an early round-based apply are queryable before the full backfill finishes |
| 3 | `pullMissingContinuationCatchesUp10000Commits` | A node >100 commits behind fully catches up via the DAG-diff path with the new continuation loop |
| 4 | `backfillFromBackupEqualsBackfillFromPeer` | The same target history rebuilt via an S3 backup source and via a live-peer source produce byte-identical resulting logs |
| 5 | `pinBlocksCompactionDuringRecovery` | A peer's pinned head prevents `compactableBefore` from squashing history that peer still needs |
| 6 | `pinExpiresAfterMaxAge` | A pin older than `--recovery-pin-max-age` no longer blocks compaction |
| 7 | `pinVisibleInStatus` | `kdb status` lists active recovery pins and the peer/age they belong to |
| 8 | `backfillVerifiesEveryFrame` | A corrupted frame injected into a backfill stream is rejected by the receiver's L1 check, not silently applied |

-----

## 9. Beyond this layer: what stays out of scope

- **Encryption at rest** is Layer 14's job; this layer's manifests and backups operate on
  whatever bytes Layer 14 produces (plaintext or ciphertext) without needing to know which.
- **Continuous scrub's production wiring** (§4.4) is specified but its execution phase is last —
  it depends on Layer 13's CPU governance (Component 49) for its rate limit, which may not have
  shipped yet.
- **Cross-region replication topology** (which region backs up to which) is an operational
  /deployment decision, not a kdb component.
- **Authorship persistence** (§2.10) is unrelated to corruption/backup/restore and is left to a
  future frame-format revision.

-----

## 10. Execution plan

Ordered by dependency; each phase's tests must pass before the next begins.

| Phase | Component | Scope | Gate |
|---|---|---|---|
| **0** | 47 (Layer 13) | Durable restart contract must be committed first | Layer 13 §4.8 tests green |
| **1** | 58 | L1+L2 verification engine, `kdb-inspect verify`, `kdb verify` | Findings match hand-corrupted fixtures exactly (segment+offset+classification) |
| **2** | 59 | Segment manifest, `repair-segments` (legacy migration, torn-tail truncate+quarantine, mid-log quarantine), auto-heal on open | §2.1's promised command exists and does what its error messages say; 100 corrupt-then-repair-then-reopen cycles, zero data loss beyond the provably-unreachable case |
| **3** | 58 (L3) | Semantic verification (document tree + blob hash checks) | L3 findings match hand-corrupted blob/tree fixtures |
| **4** | 60 | Backup manifest, incremental upload, `backup verify`, retention/GC | Full and incremental backups verify; GC never removes a referenced object |
| **5** | 61 | Restore (plain + point-in-time), hybrid restore, divergent-head side-branch handling | 10 test-plan scenarios in §7.6 pass, including the diverged-heads case |
| **6** | 62 | Log-offset backfill, `PullMissing` continuation, recovery pinning | A node >10k commits behind an active peer fully catches up; pinning survives a restart |
| **7** | — | Kotlin parity: manifest format, verification engine, backup/restore CLI | Golden fixtures (`go/testdata/golden/` convention) shared across Go and Kotlin for manifest and backup-manifest formats |

Phase 0 is a hard gate, not a suggestion (§2.4). Phases 1–2 are the minimum useful slice — they
alone turn the two dangling error messages in the current codebase into a real, working command.
Phase 3 can run in parallel with 4 (different code paths). Phase 5 depends on 4's manifest format
being final. Phase 6 is independent of 4–5 and could run in parallel with them if resourced
separately, but is ordered last here to match the "Go first, then Kotlin parity" delivery the
project has chosen for this layer, keeping one clear line of work at a time.

-----

## 11. Test plan

Beyond each component's own tests (§4.5, §5.4, §6.5, §7.6, §8.5), the layer needs end-to-end
scenarios that only make sense across components:

| # | Name | Expected |
|---|---|---|
| 1 | `corruptDetectRepairReopenRoundTrip` | Corrupt a live directory, `verify` finds it, `repair-segments` fixes what's provably safe, reopen succeeds, `verify` afterward reports clean |
| 2 | `killDashNineDuringBackup` | `kill -9` mid-backup-upload; the manifest for that backup either doesn't exist or verifies fully — no half-valid backup |
| 3 | `endToEndDisasterRecovery` | Delete a data directory entirely; `kdb restore` from the last backup plus a live peer reconstructs it to the peer's current head, verified clean at every level |
| 4 | `hybridBeatsBackupOnlyOnDataFreshness` | A hybrid restore (damaged local + backup) recovers strictly more recent data than a backup-only restore of the same directory |
| 5 | `recoveryDoesNotRaceCompaction` | A node backfilling from a peer is never starved by that peer's compaction removing needed history mid-backfill (exercises pinning under load) |
| 6 | `toolsAgreeWithEachOtherAcrossLanguages` | Go-produced manifests and backups are readable and restorable by the Kotlin implementation and vice versa (post Phase 7) |

-----

## 12. Configuration surface

```
--verify-level L1|L2|L3        Depth for `kdb verify` / `kdb-inspect verify`. Default L2.
--scrub-interval DUR           Continuous-scrub pacing. Default disabled (0).
--auto-heal BOOL               Auto-truncate torn tails at open. Default true.
--backup-bucket NAME           Reuses the KDB_S3_BUCKET convention already used by the replica sink.
--backup-prefix PATH           Default "" (bucket root), matching the existing sink's --prefix.
--backup-retention N           Manifests kept per namespace before GC eligibility. Default 30.
--recovery-pin-max-age DUR     How long an offline peer's head still blocks compaction. Default 24h.
```

Every verify/repair/backup/restore run emits a structured summary (counts by classification,
bytes scanned, objects transferred) to the same metrics discipline Layer 13 established — a
corruption event that isn't observable is as bad as one that isn't detected.

-----

## 13. Non-goals

- Encryption of backup payloads — Layer 14's job; this layer's manifests describe whatever bytes
  Layer 14 hands it, encrypted or not.
- Cross-region or multi-cloud backup topology decisions — deployment concern, not a kdb component.
- Fixing authorship persistence (§2.10) — deferred to a future frame-format revision.
- Distributed, cluster-wide backup coordination or scheduling — each node backs up its own data
  directory; orchestrating *when* is an operational script, not a kdb component.
- Automatic, unattended restore triggered by verification findings — restore is always an
  explicit operator action per P4; verification and repair may auto-heal, restore never runs
  itself.
- Signature or attestation of commits beyond the existing content hash — out of scope for this
  layer, as it is for the codebase generally today.

-----

## 14. NBNC Estimate

| Component | Production | Tests | Total |
|---|---:|---:|---:|
| 58 Integrity verification (L1/L2/L3) | ~900 | ~1,100 | ~2,000 |
| 59 Repair, quarantine, manifest | ~700 | ~800 | ~1,500 |
| 60 Backup to object storage | ~800 | ~700 | ~1,500 |
| 61 Restore and hybrid restore | ~900 | ~900 | ~1,800 |
| 62 Peer backfill and recovery pinning | ~700 | ~600 | ~1,300 |
| **Total** | **~4,000** | **~4,100** | **~8,100** |
