# KDB Layer 7 — Implementation Execution Plan

**Status:** Implemented (first Kotlin cut — May 2026)  
**Master spec:** `docs/kdb-spec.md` §16.1 (Layer 7), §0 (session state)  
**Depends on:** Layer 6 complete (Components 17–19 implemented; interfaces in §17)

-----

## Scope

Layer 7 delivers **network foundation** (physical tiers + wire + stream clients):

| Component | Module | Spec file |
|---|---|---|
| 20 Storage Tier Manager | `:kdb-storage-tier` | `kdb-spec-layer7-component20-storage-tier-manager.md` |
| 21 Wire Protocol + Framing | `:kdb-wire` | `kdb-spec-layer7-component21-wire-protocol-framing.md` |
| 22 Stream Mode (Mode 1 + 2) | `:kdb-stream` | `kdb-spec-layer7-component22-stream-mode.md` |

**Not in Layer 7:** Mode 3 peer sync (Component 23, Layer 8), JDBC (24), transport adapters WebSocket/TCP (25–26, Layer 9), CLI (29).

-----

## Normative implementation order

Product priority (Phase 2 browser + stream) differs from component numbering. Implement in this order:

### Phase 1 — Wire Protocol (21) first

Framing is a hard dependency for stream mode and future peer sync.

1. Create `:kdb-wire` with `WireCodec`, `WireHeader`, `WireMessageType`.
2. Implement frame length guard + LE layout (master §8.4).
3. Implement binary payloads: Handshake, DeltaCommit, PositionAck, CompactionNotice, IceArchiveNotice, SnapshotRequest/Response.
4. Implement `HandshakeNegotiator` + encoding selection.
5. Stub encode/decode for CommitFetch/Push, DagDiff, SchemaPush (Layer 8 consumers).
6. **Tests:** 12 cases from component spec §7.
7. Paste public interface into master spec §17 → Layer 7 (Component 21).

**Exit criteria:** Round-trip all v1-critical message types in memory; oversize frames rejected.

### Phase 2 — Stream Mode (22)

Requires wire codec stable.

1. Create `:kdb-stream`; depend on `:kdb-wire`, `:kdb-transaction`, `:kdb-index`.
2. Implement `InMemoryWireTransport` for tests.
3. Implement `StreamCoordinator` + `publish` fan-out.
4. Implement `StreamSubscriber` Mode 1: handshake, delta receive, `IndexHintApplier`, `PositionAck`.
5. Implement Mode 2: `TransactionReplay` / `ConflictReport` correlation.
6. Handle `CompactionNotice` → `CompactionBoundaryException` + event.
7. Optional: `WireCompactionCoordinator` adapter for `:kdb-compaction`.
8. Wire 11d background catch-up hook (`resumeFrom = anchorHash`).
9. **Tests:** 12 cases from spec §7 + end-to-end in-memory coordinator/subscriber.
10. Paste interface into §17 (Component 22).

**Exit criteria:** Two in-memory nodes exchange deltas; Mode 2 replay returns Applied or Conflict; position ack tracked.

### Phase 3 — Storage Tier Manager (20)

Can overlap Phase 1 after 11e hooks are understood; no wire dependency for local archive/restore.

1. Create `:kdb-storage-tier`; depend on `:kdb-storage-manager`, `:kdb-namespace-policy`, `:kdb-dag`.
2. Implement `IceBundleWriter` + `IceArchiveBundleWireType` (master §12.3).
3. Implement `inMemoryTierBackendRegistry` + `localFsTierBackendRegistry`.
4. Implement WARM→COLD segment mover subscribed to `TierSignalHooks.onTierTransition`.
5. Implement `archiveCommit` + `dag.stubCommit`.
6. Implement `restoreArchive` into isolated namespace.
7. Produce `IceArchiveNotice` payload objects (encode via `:kdb-wire` in integration tests).
8. **Tests:** 12 cases from spec §7.
9. Paste interface into §17 (Component 20).

**Exit criteria:** Archive + stub + restore roundtrip in JVM test; cold move updates registry tier.

### Phase 4 — Integration + master spec

1. Node bootstrap: register tier manager worker on JVM; stream coordinator on backend nodes.
2. Browser path: enlistment snapshot → `StreamSubscriber.connect(resumeFrom)`.
3. Update master §0 checklist: Layer 7 specs `[x]`, implementation `[ ]` per component.
4. Update §14 stats table with Layer 7 subtotal (~9,000 NBNC).
5. Cross-test: archived commit over stream emits `IceArchived` event.

-----

## Parallelism

| Can parallelize | Cannot start until |
|---|---|
| Wire schema types (21a) | — |
| Ice bundle writer (20a) | Layer 0 codec stable |
| In-memory transport (22a) | 21 frame header frozen |
| Stream subscriber Mode 1 (22b) | 21 DeltaCommit encode |
| Tier cold mover (20b) | 11e `TierSignalHooks` API stable |
| Stream Mode 2 replay (22c) | 22b + `TransactionEngine` |
| Tier archive/stub (20c) | 20a bundle format |

**Rule:** Do not implement Component 22 before Component 21 handshake + `DeltaCommit` round-trip tests pass.

-----

## Gradle modules to add

```kotlin
// settings.gradle.kts
include(":kdb-wire")
include(":kdb-stream")
include(":kdb-storage-tier")
```

Dependency edges:

```
:kdb-wire           → kdb-codec, kdb-error, kdb-document, kdb-index, kdb-compaction
:kdb-stream         → kdb-wire, kdb-transaction, kdb-index, kdb-dag, kdb-storage-manager
:kdb-storage-tier   → kdb-wire (optional, notices only), kdb-storage-manager, kdb-namespace-policy, kdb-dag, kdb-compression
```

-----

## Estimated NBNC (Layer 7)

| Component | Lines |
|---|---|
| 20 Storage Tier Manager | ~3,500 |
| 21 Wire Protocol | ~3,000 |
| 22 Stream Mode | ~2,500 |
| **Layer 7 subtotal** | **~9,000** |

Cumulative engine estimate after Layer 7 specs: **~112,850** (see master §14).

-----

## Session prompts (copy-paste)

**Spec session (done):**
```
Generate implementation-ready component specs for Layer 7: Storage Tier Manager,
Wire Protocol + Framing, Stream Mode (Mode 1 + Mode 2). Follow Section 16.2 structure.
```

**Implement Component 21:**
```
Implement Component 21 per kdb-spec-layer7-component21-wire-protocol-framing.md.
Section 17 Layers 0–6 are fixed contracts. commonMain only.
```

**Implement Component 22:**
```
Implement Component 22 per kdb-spec-layer7-component22-stream-mode.md.
Depends on :kdb-wire. Use InMemoryWireTransport for tests.
```

**Implement Component 20:**
```
Implement Component 20 per kdb-spec-layer7-component20-storage-tier-manager.md.
Subscribe to TierSignalHooks from :kdb-storage-manager. Do not implement S3 SDK in commonMain.
```

-----

## Verification checklist

- [x] `./gradlew :kdb-wire:jvmTest`
- [x] `./gradlew :kdb-stream:jvmTest`
- [x] `./gradlew :kdb-storage-tier:jvmTest`
- [x] Layer 6 tests still pass (`:kdb-compaction:jvmTest`, `:kdb-hybrid-query:jvmTest`)
- [x] Master spec §0 Layer 7 checklist updated
- [x] Master spec §17 Layer 7 interfaces populated
- [x] Master spec §14 includes Layer 7 subtotal row

### Post–Layer 7 — Debug JSON (Component 31)

Implement `:kdb-inspect` per `kdb-spec-layer10-component31-inspect-tooling.md`: JSONL sidecars for delta/wire, offline `kdb inspect dump-*`, and wire JSON payload fixes. Does not change normative storage or hash bytes.
