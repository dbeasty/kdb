# KDB Component Spec — Layer 10
## Component 31: Inspect / Debug Tooling
### `dev.kdb.inspect`

**File:** `kdb-spec-layer10-component31-inspect-tooling.md`  
**Layer:** 10 — Tooling (early delivery; does not block Layer 8–9)  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-inspect`  
**Depends on:** Layer 0 (codec), Layer 1 (document/commit), Layer 3 (`DeltaRecord`), Layer 4a (`DeltaPageCodec`, segment I/O), Layer 7 (`WireCodec`, `WireMessage`)

-----

## 1. Purpose

Provides **non-authoritative JSON views** of engine binary data: optional JSONL sidecars for delta and wire traffic, and offline `kdb inspect dump-*` tools. Production hashing, sync, and tiering remain Layer 0 binary only (master §12.1, §12.4).

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbHash`, `KdbUuid`, `KdbTimestamp` |
| `dev.kdb.document` | `KdbCommit`, `KdbOp`, `KdbDocument` |
| `dev.kdb.storage` | `DeltaRecord`, `DebugSidecarConfig`, `DeltaDebugHook` |
| `dev.kdb.storage.delta` | `DeltaPageCodec`, `DeltaSegmentScanner` |
| `dev.kdb.wire` | `WireCodec`, `WireMessage` |
| `dev.kdb.compression` | zstd decompress (via delta codec) |

-----

## 3. Public Interface

```kotlin
package dev.kdb.inspect

import dev.kdb.document.KdbCommit
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.DeltaDebugHook
import dev.kdb.storage.DebugSidecarConfig
import dev.kdb.storage.CompressionCodec
import dev.kdb.wire.WireCodec
import dev.kdb.wire.WireMessage

object InspectJson {
    fun commitToJsonLine(commit: KdbCommit): String
    fun deltaRecordToJsonLine(record: DeltaRecord, segmentId: KdbUuid, offset: Long): String
    fun wireMessageToJsonLine(message: WireMessage, direction: String): String
}

object DeltaSegmentScanner {
    fun scanSegmentBytes(bytes: ByteArray, compression: CompressionCodec): List<ScannedCommit>
    data class ScannedCommit(val commitHash: KdbHash, val commit: KdbCommit, val payloadOffset: Int)
}

class WireFrameInspector(private val codec: WireCodec) {
    fun dumpFrame(frame: ByteArray, pretty: Boolean = true): String
}

fun deltaDebugHook(config: DebugSidecarConfig): DeltaDebugHook
fun wireDebugHook(config: DebugSidecarConfig): WireDebugHook

fun interface WireDebugHook {
    suspend fun onWire(message: WireMessage, direction: String)
}
```

JVM CLI entry: `dev.kdb.inspect.cli.InspectMain` — subcommands `dump-delta`, `dump-wire`, `dump-commit`, `dump-blob`.

-----

## 4. Contracts

- Sidecar JSONL: one UTF-8 JSON object per line; fields include `type`, `timestamp`, `namespaceId`, and decoded payload.
- `DeltaSegmentScanner` matches v1 on-disk format: sequential KDBP-framed `commitPayload` bytes (Component 10d implementation note).
- Dump tools never mutate source segments or hashes.
- Disabled sidecar (`DebugSidecarConfig.enabled = false`) is a no-op.

-----

## 5. Test Cases

| # | Name | Expected |
|---|---|---|
| 1 | `scan_threeCommits` | Scanner returns 3 commits from in-memory segment |
| 2 | `commitJson_roundtrip` | JSON line contains commit hash and op count |
| 3 | `wireDump_handshake` | Frame decodes to handshake kind |
| 4 | `deltaHook_writesJsonl` | Append triggers one line in sidecar file (JVM) |
| 5 | `wireRoundtrip_ops` | DeltaCommit with Write op survives wire codec |

-----

## 6. Estimated Lines

| Sub-component | Est. NBNC |
|---|---|
| InspectJson + scanner | 350 |
| Sidecar hooks + JVM file writer | 200 |
| WireFrameInspector + CLI | 250 |
| Tests | 200 |
| **Total** | **~1,000** |
