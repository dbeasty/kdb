# 2026-10-12 — Backup objects are content-addressed (branch `fix/backup-changed-segment-new-key`)

Closes the item #131 found and did not fix: a sealed segment whose bytes change for any reason
other than damage was re-uploaded under the shared key a base backup's manifest names.

**The defect.** `backup.Create` stored every sealed segment at its data-directory name
(`ns/<ns>/delta/<seq>.seg`), one key per sequence whatever the bytes. An incremental
(`--base-backup-id`) referenced the base's entry only while the hash matched. Otherwise it
re-uploaded to the same key, and the base's object changed under it. A sealed segment's bytes
change legitimately: `kdb-inspect repair-segments` quarantines frames and rewrites the segment,
a `kdb:repair/1` record is appended, a history=none truncation rewrites it. A non-incremental
backup did the same, since it wrote the same key. So an older backup silently changed content.
`backup-verify` of the base caught it only as an unexplained size or hash mismatch, and the
base's original bytes were gone.

**Found along the way: the S3 replica mirror writes the same key.** `kdb-inspect backup --to s3`
and the S3 replica sink read the same `KDB_S3_*` configuration. The sink mirrors each live segment
to `<prefix><segment name>`, which was exactly the legacy backup key. With both on one bucket and
prefix, every mirrored rewrite or segment deletion replaced or removed a backup's object. Probe on
main (in-memory S3): after a mirror put and delete, the backup reported "size 19, manifest says
507" and "size 0, manifest says 180".

**The fix** (`go/kdb/backup/backup.go`):
- Sealed segments go to `backup-segments/<ns>/<seq, 20 digits>-<sha256 of the stored bytes>.seg`
  (`sealedSegmentKey`). Two uploads share a key only when they share content, so an object is
  never changed once written. The key is outside `backups/` (manifests, active prefixes) and
  outside the `ns/` tree the replica sink mirrors to. Identical bytes give an identical key, so a
  repeated full backup rewrites the same bytes in place and changes nothing.
- An incremental still references the base's entry when the hash matches, so unchanged segments
  stay shared and are not re-uploaded. A changed segment gets its own key, and the base's object
  is never touched.
- Older manifests are read unchanged. Each entry records its own `key`, so verify, fetch and
  restore never derive it. An incremental over a legacy base references a legacy object only after
  re-reading it and checking the recorded hash (`baseObjectStillHolds`). A legacy object an older
  kdb, or the mirror, already overwrote is not named again: the incremental uploads its own copy.
  Content-addressed objects are not re-read.
- **`backup-verify` explains the overwrite.** It already caught a mismatching legacy object. It now
  also reads the namespace's other manifests and names the backup whose recorded hash equals the
  bytes now at the key: "the shared object was overwritten by backup <id>, which a kdb older than
  content-addressed backup objects uploaded to the same key…". Without such a manifest (the mirror
  case) it still says that the shared key no longer holds this backup's bytes, and what writes it.
  `backup-fetch` and `restore` refuse such an object, as before, with the same explanation. The
  base's original bytes cannot be recovered from the store. Only the source data directory or a
  peer still has them.
- The active segment's prefix keeps its backup-scoped key. The manifest format is unchanged
  (`formatVersion` 1). Only key values differ.
- Kotlin has no backup creation or manifest reading, so nothing changes there. No other code
  derives backup keys (the e2e tests and `kdb-inspect` use the manifest).

**Before/after**, main → this branch (`go/kdb/backup/changed_segment_test.go`; dir and S3 stores
each):
- Base backup, then segment 0 is repaired (a frame quarantined by `integrity.Repair`), extended
  (a record appended) or rewritten shorter, then an incremental. Main: the incremental writes to
  `ns/bk/ns/delta/00000000000000000000.seg` and the base fails verify ("size 327, manifest says
  507"). Branch: both verify and each restores its own segment 0 byte for byte (base 5 commits;
  incremental 4, 6 and 4). Segment 1 is shared, and only the changed segment is uploaded.
- Mirror put and delete over a backed-up segment: main loses the backup → branch still verifies.
- A legacy base overwritten the old way: main's verify says only "size 327, manifest says 507" →
  branch names the overwriting backup. An incremental over it no longer names the broken object.

**Tests** (`go/kdb/backup/changed_segment_test.go`). Each was confirmed failing with the fix
reverted. The legacy test needed a two-function shim to compile against main.
- `TestIncrementalOverAChangedSealedSegmentKeepsTheBase`: 3 kinds of change × 2 stores; both
  backups verify and restore to their own bytes and commit counts; the unchanged segment stays
  shared, with one sealed upload.
- `TestSealedSegmentObjectsAreContentAddressed`.
- `TestReplicaMirrorDoesNotTouchBackupObjects`.
- `TestLegacyManifestsStillVerifyRestoreAndServeAsBases`: a hand-built old-format backup verifies
  and restores. An incremental over it shares unchanged legacy objects with no upload, and puts a
  changed segment under a new key, leaving the legacy base intact.
- `TestVerifyNamesTheBackupThatOverwroteALegacyObject`.

**Gates:** `go test -race` for backup, integrity, control, cmd/kdb-inspect and storage/io/s3, then
the full suite. The e2e `test_backup_restore.py test_scrub_repair.py` ran with this worktree's
binaries.

**Not done.** No garbage collection: objects no manifest names any more stay in the store, as
before. Existing stores are not migrated. Legacy objects stay where they are and are left alone.

## Release note

A later backup can no longer change an earlier one: sealed delta segments are now stored under
a key that includes a hash of their contents, so a segment changed by `kdb-inspect
repair-segments`, a repair record or a truncation gets a new object and the base backup keeps
the exact bytes it recorded. This also stops an S3 replica mirror sharing the backup's bucket and
prefix from overwriting backup objects. Backups made by earlier versions still verify, restore
and serve as incremental bases, and `backup-verify` now names the later backup that overwrote an
older backup's shared object.
