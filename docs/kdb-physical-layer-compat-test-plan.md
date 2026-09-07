# Physical-layer Kotlin/Go compatibility: test plan

## 1. What "physical layer" means here

Everything that determines the **bytes on disk** and the **paths those bytes live at**, for a
KDB data directory that either runtime may open:

| Area | Kotlin | Go |
|---|---|---|
| WAL record frame | `kdb-storage-wal/.../WalCodec.kt` | `go/kdb/storage/wal/codec.go` |
| WAL segment chain naming | `DefaultWriteAheadLog.kt` | `go/kdb/storage/wal/default_wal.go` |
| SSTable block frame | `SsTableCodec.encodeBlock` | `sstable/codec.go encodeBlock` |
| SSTable footer + index | `SsTableCodec.buildFooter` | `sstable/codec.go buildFooter` |
| Delta KDBP page frame | `DeltaPageCodec` | `storage/delta/page_codec.go` |
| Delta segment scan/torn tail | `DeltaSegmentScanner` | `storage/delta/scanner.go` |
| Commit payload (Layer 0) | `KdbCommit.toPayloadBytes` | `document.Commit.ToPayloadBytes` |
| Segment path naming | `SegmentNameBuilder.kt` | `storage/io/segment_name.go` |
| Directory layout / snapshots | `JvmSegmentByteStore` | `storage/io/os_store.go` |
| CRC-32 | `Crc32.kt` | `compression/crc32.go` |
| ZSTD | `ZstdCompression` (zstd-jni) | `compression/zstd.go` (klauspost) |

Explicitly **out of scope**: the wire protocol (`kdb-wire` / `go/kdb/wire`, already covered by
`go/kdb/interop/wire_interop_test.go`), query planning, and anything that never reaches a file.

## 2. The compatibility contract

Three distinct guarantees, tested separately because they are not the same claim:

- **C1 — Byte identity.** For every format field that is not a compressor payload, both
  runtimes encoding the same logical input MUST produce identical bytes. Applies to: all frame
  headers, magics, lengths, CRCs, the SSTable footer and its index lines, the commit payload,
  and every segment path string.
- **C2 — Cross-decode.** Each runtime MUST decode any segment the other wrote, including
  compressor payloads. This is the weaker guarantee that covers ZSTD.
- **C3 — Behavioral parity on damaged input.** Given identical corrupt bytes, both runtimes
  MUST make the same recovery decision (skip vs. resync vs. fail) and report the same counts.

### What ZSTD drags out of C1

Kotlin/JVM compresses through zstd-jni (upstream libzstd); Go compresses through
`klauspost/compress`. Both emit valid, spec-conformant zstd frames, but not the *same* frames.
That is not confined to the block bodies: **the SSTable footer index carries each block's offset
and compressed size**, so those two numeric fields are downstream of the compressor and cannot be
compared across languages either. The same is true of any recorded segment size.

So the footer splits across the two guarantees:

| Footer field | Guarantee | Why |
|---|---|---|
| magic, `indexLen`, trailer | C1 | fixed-width, compressor-independent |
| index line **order** | C1 | insertion order, and the whole point of the determinism fix |
| key hex, tombstone flag | C1 | plaintext-derived |
| offset, compressed size | **C2 only** | a function of the compressor's output length |
| `fileHash` | C1 | SHA-256 over key‖plaintext, never over compressed bytes |

`fileHash` being C1 is the load-bearing one: it is the segment's content address, so a Go-written
SSTable keeps its identity when the JVM reads it. A test that compared whole footer bytes would
fail on the offsets and tell you nothing about that.

### Why ZSTD payloads are C2 and not C1

Kotlin/JVM compresses through zstd-jni (upstream libzstd); Go compresses through
`klauspost/compress`. Both emit valid, spec-conformant zstd frames, but not the *same* frames —
different match finders, different block splits. There is no configuration of either that makes
them agree byte-for-byte. So no test may compare a compressed body across languages. Tests
compare the 16/20-byte header around it, and separately assert that each side decompresses the
other's body to the identical plaintext.

`Decompress` must also tolerate frames that omit the content-size field. klauspost's `EncodeAll`
emits such frames; `ZstdCompression.jvm.kt` already handles this and the reason is recorded in
its comment. A regression here makes a Go-written directory open as *silently empty* on the JVM,
so it gets its own test rather than being assumed.

## 3. Harness

Bidirectional, extending the Kotlin→Go golden mechanism already in
`ExportGoldenTest.kt` / `go/kdb/interop/codec_golden_test.go`.

```
go/testdata/golden/physical/
  kotlin/     written by :kdb-integration ExportPhysicalGoldenTest, read by Go
  go/         written by `go test ./kdb/interop -run ExportPhysicalGolden -tags export`,
              read by Kotlin
```

Fixtures are `.hex` (one lowercase hex string, trailing newline) so they diff readably and
survive git without binary handling — matching the existing `codec/*.hex` convention.

Both exporters are ordinary tests that are *idempotent*: re-running them must leave the tree
clean. A dirty tree after an export run is itself a failure signal (it means one side's encoder
changed), and CI asserts it.

### Regeneration

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew :kdb-integration:test --tests "dev.kdb.integration.ExportPhysicalGoldenTest"
cd go && go test ./kdb/interop/ -run ExportPhysicalGolden
git diff --exit-code go/testdata/golden/   # must be clean
```

## 4. Test matrix

### 4.1 WAL record frame — C1

| # | Case | Assert |
|---|---|---|
| W1 | Magic constant | Go `Magic` == Kotlin `MAGIC` == `0x4B444358`; `BatchMagic` == `0x4B444242` |
| W2 | Header length | Both encode a 21-byte header: `sequence(8) epochMicros(8) kind(1) payloadCrc(4)` |
| W3 | Golden round trip | Go decodes Kotlin's encoded record; re-encode is byte-identical, and vice versa |
| W4 | Timestamp survives | `timestamp` decodes to the encoded value, not to "now" |
| W5 | Kind ordinals | `PutBlob=0 DeleteBlob=1 FlushCheckpoint=2 Marker=3` on both sides |
| W6 | Empty payload | A zero-length payload round-trips (total frame = 33 bytes) |
| W7 | `PutBlob` payload | `hash(32) || bytes` identical across languages |

### 4.2 WAL damaged input — C3

| # | Case | Assert |
|---|---|---|
| W8 | Junk prefix | Both resync forward to the next valid frame and recover the record after it |
| W9 | Short `recordLen` | `recordLen < 21` is rejected, no panic, no negative slice |
| W10 | Payload CRC flipped | Both skip exactly one record and count it as skipped |
| W11 | Header CRC flipped | Both skip exactly one record and count it as skipped |
| W12 | Torn tail | A truncated final frame ends the scan cleanly, is not counted as corrupt |
| W13 | Skip counts equal | `WalRecoverySummary.recordsSkippedCorrupt` == Go `RecoverySummary.RecordsSkippedCorrupt` |
| W14 | `skipCorrupt=false` | Both raise a corruption error carrying the same byte offset |

### 4.3 WAL segment chain — C1

| # | Case | Assert |
|---|---|---|
| W15 | Active name | Both: `ns/{partition}/wal/{walId}` |
| W16 | Rotated name | Both: `ns/{partition}/wal/{walId}.{20-digit zero-padded firstSequence}` |
| W17 | Cross-open | Kotlin opens a Go-written rotated chain without throwing, and vice versa |
| W18 | Chain replay | Both replay **all** segments of the chain in sequence order, not just the newest |
| W19 | Foreign walId | A directory holding two walIds resolves to the same chain on both sides |

### 4.4 SSTable — C1 + C2

| # | Case | Assert |
|---|---|---|
| S1 | Block header | 16 bytes, `v2`, codec id, comp/uncomp lengths, CRC — fixed-width fields identical |
| S2 | Compressed block | Bodies cross-decode to the same plaintext (C2) |
| S3 | Footer determinism | Repeated writes of the same table produce identical footer bytes (per language) |
| S4 | Footer index order | Go's line order == Kotlin's insertion order for the same put sequence |
| S5 | Tombstone line | `<hex>:<off>:<size>:1` written by both, parsed by both |
| S6 | Trailer | Last 4 bytes duplicate `indexLen`; footer magic is `0x4B444253` on both |
| S7 | Duplicate key | `put(k,v1); put(k,v2)` yields one entry, value `v2`, on both sides |
| S8 | `fileHash` | **Byte-identical** across languages for the same put/delete sequence, tombstone `0xFF` marker included |
| S9 | Cross read | Go reads a Kotlin-written segment and vice versa, values and tombstones intact |
| S10 | Bad block CRC | Both fail loudly rather than returning garbage |
| S11 | Empty value | A zero-length value survives both compressors and cross-decodes — klauspost emits a zero-byte body for it where libzstd emits a 9-byte frame |

### 4.5 Delta segment — C1 + C2

| # | Case | Assert |
|---|---|---|
| D1 | KDBP header | 20 bytes, magic `KDBP`, `v2`, codec id, lengths, CRC — identical |
| D2 | `CODEC_NONE` frame | Byte-identical end to end |
| D3 | `CODEC_ZSTD` frame | Header identical; bodies cross-decode (C2) |
| D4 | Unknown codec id | Both reject with an error, neither guesses |
| D5 | Sequenced name | `ns/{ns}/delta/{20-digit}.seg` on both; parse rejects legacy UUID names |
| D6 | Torn tail | Truncated trailing frame stops the scan cleanly on both |
| D7 | CRC mismatch | Both raise a corrupt-frame error at the same offset, both expose partial commits |
| D8 | Commit payload | `toPayloadBytes` identical across languages, including omitted null `schemaHash` |
| D9 | Commit hash | SHA-256 of those bytes identical — a Go-written commit keeps its hash on the JVM |

### 4.6 Layout and paths — C1

| # | Case | Assert |
|---|---|---|
| L1 | Segment paths | `delta`/`wal`/`sstable`/`namespacePrefix` builders agree string-for-string |
| L2 | Snapshot dir | Both store enlistment snapshots under the same directory name |
| L3 | Snapshot key | Both apply the same key sanitization (`:` is not a portable filename char) |
| L4 | `listSegments` order | Both return a globally lexicographic sort, not directory-walk order |
| L5 | Name validation | Both reject empty, `..`, and non-`ns/` names |

### 4.7 Primitives — C1

| # | Case | Assert |
|---|---|---|
| P1 | CRC-32 | Identical over the empty slice, `"123456789"` (== `0xCBF43926`), and 1 KiB of random bytes |
| P2 | Big-endian ints | `writeInt`/`readInt` agree at `Int.MIN_VALUE`, `-1`, `0`, `Int.MAX_VALUE` |
| P3 | Timestamp | `toEpochMicros`/`fromEpochMicros` round-trip identically at the epoch and at negatives |
| P4 | ZSTD no content size | JVM decompresses a klauspost frame that omits the content-size field |

## 5. What this plan found

Everything below was confirmed by a failing test before it was fixed, on the branch that carries
this document.

**Data-format incompatibilities** — a store written by one runtime was unreadable or misread by
the other:

1. **WAL frame v1 vs v2.** Kotlin wrote magic `0x4B444358` with a 21-byte header carrying
   `epochMicros`; Go wrote `0x4B444257` with a 13-byte header and no timestamp, fabricating a
   fresh one for every record on replay. Neither side could read a byte of the other's log.
   *Fixed:* Go adopts Kotlin's v2 frame, timestamps included (W1–W7).
2. **WAL segment chain naming.** Go rotates into `{walId}.{20-digit firstSeq}` and walks the
   whole chain; Kotlin had no rotation, read only the lexicographically-largest segment, and fed
   the whole file name to `KdbUuid.fromString` — which *threw* on any rotated name. A Go data
   directory became unopenable on the JVM the moment its WAL rotated once, and a Kotlin WAL grew
   without limit (`walMaxSegmentBytes` was accepted and never read). *Fixed:* Kotlin gained the
   same chain parsing, rotation, and whole-chain replay (W15–W19).
3. **Snapshot directory and key encoding.** Kotlin used `<root>/snap/` with `:` → `_`; Go used
   `<root>/snapshots/` with the raw key — so snapshots written by one were invisible to the
   other, and Go's file names contained colons. *Fixed:* Go aligned to Kotlin (L2, L3).
4. **Empty values crashed the JVM.** klauspost's `EncodeAll` returns a *zero-byte* body for a
   zero-length input where libzstd emits a 9-byte empty frame. `Zstd.decompressedSize` throws
   `ArrayIndexOutOfBoundsException` on an empty slice, so reading any Go-written SSTable block or
   delta page holding an empty value crashed. *Fixed:* an empty body decodes to empty output
   (S11, P4).

**Nondeterminism and hash divergence:**

5. **SSTable footer order was randomized.** Go built its index by ranging a Go map, so the same
   table produced different footer bytes on every run — never Kotlin's insertion order, and never
   twice the same within Go. *Fixed:* the writer's ordered entry list drives the footer (S3, S4).
6. **Duplicate keys forked the content address.** Kotlin's `linkedMapOf` collapsed a re-written
   key to one entry; Go appended a second, writing an unreachable block and feeding both values
   into the `fileHash` preimage. The same put sequence produced different content hashes per
   language. *Fixed:* Go matches Kotlin's last-value-wins, first-position semantics (S7, S8).

**Robustness on the same bytes:**

7. **Go panicked on a short `recordLen`.** A frame declaring `recordLen < 21` made the payload
   length negative and sliced out of range — reachable from any truncated or hostile segment.
   Kotlin had the bound; Go did not. *Fixed* (W9).
8. **Go abandoned a WAL segment on bad magic** where Kotlin resynced forward, so one damaged byte
   discarded every intact record after it. *Fixed* (W8).
9. **Go never reported skipped-corrupt counts.** `RecoverySummary.RecordsSkippedCorrupt` was
   declared and never assigned, reading zero however damaged the segment was, because
   `DecodeRecords` returned no count to assign. *Fixed* (W13).
10. **`Truncate` diverged.** Go's was segment-granular only; Kotlin's rewrote the active segment
    for a cut falling inside it. Each side was missing the other's half. *Fixed:* both now
    implement all three cases.

## 6. Known gaps not fixed by this plan

- **Kotlin JS/native ZSTD is an identity wrapper** (`ZstdCompression.js.kt`,
  `.native.kt`) while frames are still tagged `CODEC_ZSTD`. A segment written by a JS or native
  Kotlin build is therefore unreadable by Go *and* by the JVM. Out of scope while the deploy
  target is Go + JVM, but it is a latent format bug, not a missing feature — a JS build must
  either link a real zstd or write `CODEC_NONE`.
- **Encryption at rest** (`docs/kdb-spec-layer14-encryption-at-rest.md`) sits below these
  formats and has no cross-language fixtures yet.

## 7. CI

`make test-go` and `./gradlew test allTests` each run their half of the suite. The golden files
are committed, so neither side needs the other's toolchain to run its own tests.

`make test-physical-golden` regenerates every fixture from both encoders and then fails if any
moved — that clean-tree assertion is what makes an *accidental* format change fail the build, and
it is also how you regenerate the fixtures after a deliberate one. CI runs the same thing in the
`golden-freshness` job, which needs both toolchains now that fixtures flow in both directions.
