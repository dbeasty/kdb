# KDB Attachments End-to-End Tests

Status: built, 2026-10-14. The fixes for the defects it found are merged: #228, #230, #231
and #233. Tests the design in [kdb-attachments-plan.md](kdb-attachments-plan.md):
named attachments per record, Merkle chunk hashes, a side attachment table whose root is in every
commit, git-like deltas, streamed pinned reads. This replaces the earlier draft of this plan,
which targeted a path-addressed design that was not chosen.

## What the suite checks

Each test checks one or more of these, using real processes, the Go client and SQL over the network:

- **I1 Bytes.** What is read back is bit-identical (SHA-256) to what was attached.
- **I2 Media.** The downloaded file still parses as the same media: every Y4M frame re-parsed,
  WAV chunks re-read, PNG CRCs and pixels checked, MP4 decoded frame by frame (ffmpeg
  `framemd5`). All compared with the truth recorded when the file was generated.
- **I3 One commit.** A record and its attachment commit together. As of the commit before,
  neither exists; as of it, both do; the JSON never mentions the attachment.
- **I4 Versions.** As of each commit, the bytes are that commit's version.
- **I5 Storage.** Identical bytes are stored once. A small edit leaves about one copy plus a
  delta, once maintenance has run.
- **I6 Search.** SQL over the metadata finds exactly the right records, and their attachments
  are the right files.
- **I7 Replicas.** A node never lists an attachment before it can serve every byte, and nodes
  converge to identical bytes.
- **I8 Crashes.** A kill -9 loses no acknowledged attachment, and partial uploads never become
  visible.
- **I9 Pinned reads.** A read finishes on the version it started on while newer versions commit
  and maintenance runs, and it reports `superseded`.
- **I10 Memory.** Moving a file far larger than any buffer leaves server memory flat.

## Test data: `kdb-integration/e2e/media_fixtures.py`

Generated from a seed with the standard library, so the bytes are identical on every platform and
run. Edits land at known offsets.

| Generator | Purpose |
|---|---|
| `make_y4m` / `replace_frames` | Raw 4:2:0 video, frame-addressable edits (replace, append, truncate) |
| `make_wav` | PCM sweep with a LIST/INFO title, so a retitle changes header bytes only |
| `make_png` | Small image, CRC- and pixel-checked |
| `make_bin` / `stream_bin` / `insert_bytes` | Seeded random bytes of any size, streamed; an insertion shifts every later byte |
| `make_mp4` / `reencode_mp4` | ffmpeg H.264 (a required test dependency); re-encode is the negative control for deltas |

`test_media_fixtures.py` checks the generator itself: determinism, edits confined to their
frames, and truth that detects damage.

## Helper commands: `go/cmd/kdb-e2e-helper/attachments.go`

- `attach`: optional `--drop-at` fails the upload part way and resumes it.
- `tx-attach`: record JSON and attachment in one transaction.
- `sql-attach`: uploads, then binds the blob to `STREAM ?`.
- `read-attachment`: streams, hashes, optionally writes to `--out`. `--pause-after` stops part
  way until the test resumes it. `--offset/--length` makes it a verified ranged read.
  `--resume-at` drops the connection part way and resumes by range at the same commit.
- `list-attachments`.

Expected failures print `{"error", "kind"}`, with kind one of `not_found`, `corrupt`,
`conflict` or `range`.

Python wrappers and on-disk measurement (`<data-dir>/ns/<ns>/attachment/`) are in
`attachment_fixtures.py`.

## Tests

| File | Test | Checks |
|---|---|---|
| `test_attachments_media.py` | round trip of 0 B…3 MiB across the inline (256 KiB) and chunk (1 MiB) boundaries, and every media format, then again after a restart | I1 I2 |
| | record + attachment in one commit | I3 |
| | identical bytes stored once | I5 |
| | a byte flipped on disk is refused, never served | I1 |
| | dropped upload resumes | I1 |
| | several named attachments per record; replace, SQL RENAME/DROP, record delete | I3 |
| | six versions read back as of their commits | I4 |
| | small edit leaves only a delta (Y4M frames, WAV retitle, inserted bytes) | I5 |
| | re-encoded MP4 stays whole (negative control) | I5 |
| | a JSON-only update leaves the attachment alone | I3 |
| | 30-record catalog: SQL filter vs Python filter, then every hit's bytes | I6 |
| | SQL `STREAM ?` with 18 MiB media (above the 16 MB frame and `ATTACHMENT()` cap); `UPDATE … SET ATTACHMENT` | I1 I2 |
| `test_attachments_consistency.py` | paused read of v1 while v2 commits and maintenance runs (history=none) | I9 |
| | three concurrent paused readers | I9 |
| | v1 freed after the last reader closes | I9 |
| | a finished upload survives maintenance until it is committed | I3 |
| | kill -9 mid-upload of 96 MiB: verify clean, nothing visible, partial upload collected | I8 |
| | kill -9 during sustained attachment writes | I8 |
| `test_attachments_cluster.py` | replica polled during a 48 MiB transfer: never listed before it can be read | I7 |
| | writers on both nodes, one record with an attachment from each side | I7 I2 |
| | replica killed mid-transfer of 96 MiB recovers | I7 I8 |
| `test_attachments_ranges.py` | Y4M frames read by byte range (including frames straddling a chunk boundary) match their recorded hashes | I1 I2 |
| | MP4 keyframe packets (located by ffprobe) read by byte range | I1 I2 |
| | arbitrary ranges, the end of the file, and past the end (typed range error) | I1 |
| | a download drops its connection, a new version commits, and the resume by range at the first commit completes the original version | I4 I9 |
| | a corrupted chunk inside a range is refused; other ranges still read | I1 |
| `test_attachments_large.py` | 1 GiB (`slow`) and 5 GiB (`xl`, opt-in) round trip, RSS sampled | I1 I10 |
| `test_attachments.py` (existing) | two-node replication, restart | I7 |
| `test_upgrade.py` (existing) | mixed-version mesh; rollback refused | — |

Measured on 2026-10-14 (macOS): server RSS peaked at 199 MiB moving 1 GiB and at 153 MiB moving
5 GiB, from a 74 MiB baseline.

## Defects and gaps found (as of 2026-10-14)

These are encoded as `xfail(strict=True)`, so fixing one turns its test into a failure until
the marker is removed.

1. **No deltas on a server.** `CompactAttachments` delta-encodes only the replaced versions that
   embedded `PutAttachment` records (`embed/attachments.go`, `putAttachmentRef`). Commits that
   arrive as transactions never record them, and that covers the Go client, SQL
   `SET ATTACHMENT` and peer sync. The list is also in memory only. So a server keeps every
   version whole.
   - The encoder itself meets the budgets. Measured directly against `attachstore`: +66 KB for
     two replaced Y4M frames, +0.5 KB for a WAV retitle, +0.9 KB for 450 bytes inserted into
     8 MiB.
   - Fixed by PR #228 (merged), which derives replacements from the DAG. The tests now pass
     without a marker.
2. **Corruption is not typed on the wire.** The server refused corrupt bytes, but sent them as
   an untyped message. Fixed by PR #230 (merged): wire code `INTEGRITY` maps to `ErrAttachmentCorrupt`.
   The tests now require kind `corrupt`.
3. **GC of replaced versions is unreachable in a short test.**
   - `AttachmentHistoryLatest` and `ReadLease` are embed-only settings, with no service flag or
     env.
   - history=none truncates whole delta segments, which a few commits never fill.
   - So "freed after the last reader closes", and lease expiry, cannot be driven end to end.
     Marker: `COLLECT_NOT_REACHABLE`.
4. **Maintenance deletes a finished upload before its commit.** `attachstore.Collect` keeps only
   blobs a commit references, plus uploads still streaming. A finished upload that is not yet
   committed is deleted by the next maintenance pass. The client API, SQL `STREAM ?` and replica
   pushes all upload first and commit after, so a pass landing in that gap makes the commit fail
   with "attachment blob not stored". Reproduced deterministically by a 2 s gap between upload and
   commit. Fixed by PR #231 (merged, `UploadGrace`). The test is strict, and a negative control with
   `--attachment-upload-grace 300ms` shows that the gap returns when the grace is shorter.
5. **No ranged read in the Go client.** Fixed by PR #233 (merged), which adds
   `OpenAttachmentRange` and `ReadAttachmentAt` with per-chunk RFC 6962 proofs.
   `test_attachments_ranges.py` and the helper's `--offset/--length/--resume-at` use that API.

## Not covered yet

- Concurrent changes to the same attachment from two clients (conflict) end to end. This is
  covered by Go unit tests in `transaction/attachments.go`.
- Kotlin engine and JDBC attachments against a Go server in the e2e harness. The Gradle
  integration job has a JDBC-against-Go test.
- S3-backed attachment store; backup and restore of attachments through the e2e harness.
- `history=none` replicas and snapshot joiners holding attachments.

## Running

```text
cd go && go build -o bin/kdb-service ./cmd/kdb-service && go build -o bin/kdb-e2e-helper ./cmd/kdb-e2e-helper \
      && go build -o bin/kdb-inspect ./cmd/kdb-inspect
cd kdb-integration/e2e
pytest -q test_media_fixtures.py test_attachments*.py           # default: about 8 minutes
pytest -q -m slow test_attachments_large.py                     # 1 GiB
pytest -q -m xl test_attachments_large.py                       # 5 GiB, opt-in
```

ffmpeg must be on the PATH.
