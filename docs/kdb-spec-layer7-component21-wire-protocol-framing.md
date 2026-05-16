# KDB Component Spec — Layer 7
## Component 21: Wire Protocol + Framing
### `dev.kdb.wire`

**File:** `kdb-spec-layer7-component21-wire-protocol-framing.md`  
**Layer:** 7 — Network Foundation  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-wire`  
**Depends on:** Layer 0 (`KdbValue`, codec), Layer 1 (document/commit wire types), Layer 2 (`CommitRef`), Layer 3 (capability bits — read-only), Layer 6 (`CompactionIntent` shapes — encode only)

-----

## 1. Purpose

Defines the **KDB peer wire contract** in `commonMain`: frame envelope (master §8.4), protocol version negotiation, payload encoding (Layer 0 typed binary default, JSON optional), and typed codecs for all message kinds in master §8.5 (`0x01`–`0x0D`).

This module is **transport-agnostic** — it reads/writes `ByteArray` frames. TCP/WebSocket adapters (Layer 9) call `WireCodec.encode` / `decode` on byte streams. Higher layers (Stream Mode 22, Peer Sync 23) send/receive `WireMessage` values without duplicating framing logic.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.codec` | `KdbValue`, `encodeToBytes`, `decodeFromBytes`, `KdbHash` |
| `dev.kdb.error` | `UnsupportedProtocolVersionException`, `EncodingNegotiationFailure`, `KdbException` |
| `dev.kdb.document` | `KdbCommit`, `KdbOp`, `DeltaCommitPayload` wire helpers |
| `dev.kdb.schema` | `SchemaDelta` wire (if present on commit) |
| `dev.kdb.index` | `IndexHint`, `IndexHintWire` |
| `dev.kdb.compaction` | `CompactionIntent` — compaction notice payloads |

-----

## 3. Public Interface

```kotlin
package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.compaction.CompactionIntent
import dev.kdb.document.KdbCommit
import dev.kdb.index.IndexHint

/** Protocol version supported by this build. */
const val KDB_WIRE_PROTOCOL_VERSION: Int = 1

interface WireCodec {
    fun encode(message: WireMessage): ByteArray
    fun decode(frame: ByteArray): WireMessage
    fun encodeFrameOnly(header: WireHeader, payload: ByteArray): ByteArray
    fun decodeHeader(frame: ByteArray): WireHeader
}

fun defaultWireCodec(
    encoding: PayloadEncoding = PayloadEncoding.KDB_BINARY,
): WireCodec

data class WireHeader(
    val messageType: WireMessageType,
    val protocolVersion: Int = KDB_WIRE_PROTOCOL_VERSION,
    val correlationId: Int,
    val payloadLength: Int,
)

enum class WireMessageType(val code: Short) {
    HANDSHAKE(0x01),
    DELTA_COMMIT(0x02),
    COMMIT_FETCH(0x03),
    COMMIT_PUSH(0x04),
    DAG_DIFF(0x05),
    TRANSACTION_REPLAY(0x06),
    CONFLICT_REPORT(0x07),
    COMPACTION_NOTICE(0x08),
    ICE_ARCHIVE_NOTICE(0x09),
    SNAPSHOT_REQUEST(0x0A),
    SNAPSHOT_RESPONSE(0x0B),
    POSITION_ACK(0x0C),
    SCHEMA_PUSH(0x0D),
    ;

    companion object {
        fun fromCode(code: Short): WireMessageType?
    }
}

enum class PayloadEncoding {
    KDB_BINARY,
    JSON,
}

sealed class WireMessage {
    abstract val header: WireHeader

    data class Handshake(
        override val header: WireHeader,
        val request: HandshakePayload,
    ) : WireMessage()

    data class HandshakeAck(
        override val header: WireHeader,
        val response: HandshakeAckPayload,
    ) : WireMessage()

    data class DeltaCommit(
        override val header: WireHeader,
        val payload: DeltaCommitPayload,
    ) : WireMessage()

    data class CommitFetch(
        override val header: WireHeader,
        val namespace: String,
        val sinceHash: KdbHash?,
        val maxCommits: Int = 100,
    ) : WireMessage()

    data class CommitPush(
        override val header: WireHeader,
        val namespace: String,
        val commits: List<KdbCommit>,
    ) : WireMessage()

    data class DagDiff(
        override val header: WireHeader,
        val namespace: String,
        val localHead: KdbHash,
        val remoteHead: KdbHash,
    ) : WireMessage()

    data class TransactionReplay(
        override val header: WireHeader,
        val namespace: String,
        val baseVersion: KdbHash,
        val transactionBytes: ByteArray,
    ) : WireMessage()

    data class ConflictReport(
        override val header: WireHeader,
        val namespace: String,
        val reportBytes: ByteArray,
    ) : WireMessage()

    data class CompactionNotice(
        override val header: WireHeader,
        val intent: CompactionIntent,
    ) : WireMessage()

    data class IceArchiveNotice(
        override val header: WireHeader,
        val namespace: String,
        val originalHash: KdbHash,
        val archiveLocation: String,
        val bundleHash: KdbHash,
    ) : WireMessage()

    data class SnapshotRequest(
        override val header: WireHeader,
        val namespace: String,
        val anchorHash: KdbHash?,
    ) : WireMessage()

    data class SnapshotResponse(
        override val header: WireHeader,
        val namespace: String,
        val anchorHash: KdbHash,
        val snapshotBytes: ByteArray,
        val compressed: Boolean,
    ) : WireMessage()

    data class PositionAck(
        override val header: WireHeader,
        val namespace: String,
        val commitHash: KdbHash,
    ) : WireMessage()

    data class SchemaPush(
        override val header: WireHeader,
        val namespace: String,
        val schemaBytes: ByteArray,
        val revision: Long,
    ) : WireMessage()
}

data class HandshakePayload(
    val nodeId: String,
    val namespaces: List<String>,
    val localHeads: Map<String, KdbHash>,
    val capabilities: WireCapabilitySet,
    val preferredEncodings: List<PayloadEncoding>,
    val clientMode: WireClientMode,
)

data class HandshakeAckPayload(
    val accepted: Boolean,
    val negotiatedEncoding: PayloadEncoding,
    val protocolVersion: Int,
    val remoteHeads: Map<String, KdbHash>,
    val rejectionReason: String? = null,
)

enum class WireClientMode {
    STREAM_READ_ONLY,       // Mode 1
    STREAM_WRITE_BACK,      // Mode 2
    FULL_PEER,              // Mode 3 — decode only in Layer 7; handled Layer 8
}

data class WireCapabilitySet(
    val supportsZstd: Boolean = true,
    val supportsIndexHints: Boolean = true,
    val supportsDirectDeltaIngest: Boolean = false,
    val maxFrameBytes: Int = 16 * 1024 * 1024,
)

data class DeltaCommitPayload(
    val namespace: String,
    val commitHash: KdbHash,
    val parentHash: KdbHash,
    val timestampMicros: Long,
    val operations: List<dev.kdb.document.KdbOp>,
    val indexHints: List<IndexHint> = emptyList(),
    val schemaDeltaBytes: ByteArray? = null,
)

interface HandshakeNegotiator {
    fun negotiate(
        local: HandshakePayload,
        remote: HandshakePayload,
    ): HandshakeAckPayload
}

fun defaultHandshakeNegotiator(): HandshakeNegotiator

/** Validates frame size before full decode (DoS guard). */
fun validateFrameLength(length: Int, maxFrameBytes: Int = 16 * 1024 * 1024)
```

-----

## 4. Data Structures

### Frame layout (normative, master §8.4)
```
Offset  Size     Field
0       4        frameLength   (int32 LE, includes header+payload, excludes self)
4       2        messageType   (int16 LE)
6       2        protocolVersion (int16 LE)
8       4        correlationId (int32 LE)
12      N        payload
```

`frameLength` = 8 + payload.size. Decoder rejects `frameLength < 8` or `> maxFrameBytes`.

### Payload envelope
First byte of payload: `encodingTag` (`0 = KDB_BINARY`, `1 = JSON`). Remaining bytes: message body.

For `KDB_BINARY`, body is Layer 0 record per message type schema (`HandshakeWireType`, `DeltaCommitWireType`, …).

For `JSON`, body is UTF-8 JSON object (interop/debug); v1 production uses binary.

### Correlation model
Request/response pairs share `correlationId`. Server-initiated pushes (e.g. `DeltaCommit`) use server-generated ids; client `PositionAck` echoes the delta's correlation id when acking.

### `CompactionNotice` mapping
Wraps `CompactionIntent` — same fields as `:kdb-compaction` data class.

### Version negotiation
- Client sends `protocolVersion` in handshake.
- Server accepts if `remoteVersion <= KDB_WIRE_PROTOCOL_VERSION` and `remoteVersion >= MIN_SUPPORTED` (1).
- Mismatch → `HandshakeAck.accepted = false`, `UnsupportedProtocolVersionException` on client.

### Encoding negotiation
Intersect `preferredEncodings`; prefer `KDB_BINARY`; else `JSON`; else `EncodingNegotiationFailure`.

-----

## 5. Contracts

### `WireCodec.encode` / `decode`
- **Preconditions:** `message.header.protocolVersion` ≤ `KDB_WIRE_PROTOCOL_VERSION`.
- **Postconditions:** Round-trip equality for all fields on supported types. Unknown `messageType` on decode → `WireDecodeException`.
- **Guarantee:** Deterministic binary encoding for same logical message (Layer 0 deterministic encoding).

### `validateFrameLength`
- **Throws:** `FrameTooLargeException` when over cap.

### `HandshakeNegotiator.negotiate`
- **Postconditions:** `negotiatedEncoding` in intersection. `remoteHeads` copied into ack for stream clients to detect lag.

### Message completeness (Layer 7 v1)
| Type | Encode | Decode | Used by |
|---|---|---|---|
| Handshake / Ack | yes | yes | 22, 8 |
| DeltaCommit | yes | yes | 22 |
| PositionAck | yes | yes | 22 |
| TransactionReplay | yes | yes | 22 |
| ConflictReport | yes | yes | 22 |
| CompactionNotice | yes | yes | 19 adapter |
| IceArchiveNotice | yes | yes | 20 |
| SnapshotRequest/Response | yes | yes | 11d |
| CommitFetch/Push, DagDiff, SchemaPush | yes | yes | Layer 8 (decode now) |

### Zstd
Snapshot and bulk commit push payloads **may** set `compressed=true` in `SnapshotResponse` with zstd-wrapped bytes inside payload record — not applied to every frame in v1.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `UnsupportedProtocolVersionException` | Handshake version unsupported |
| `EncodingNegotiationFailure` | No common encoding |
| `WireDecodeException` | Truncated frame, bad type tag, schema mismatch |
| `FrameTooLargeException` | Length check failed |
| `InvalidCorrelationException` | Response with unknown id (stream layer) |

```kotlin
class WireDecodeException(message: String, cause: Throwable? = null) : KdbException(message, cause)
class FrameTooLargeException(val length: Int, val max: Int) : KdbException("frame length $length exceeds max $max")
class InvalidCorrelationException(val correlationId: Int) : KdbException("unknown correlation $correlationId")
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `frameRoundtrip_handshake` | Encode/decode Handshake | Equal payload |
| 2 | `frameRoundtrip_deltaCommit` | Delta with 2 ops + hints | Equal hash + hints |
| 3 | `rejectOversizedFrame` | length > max | `FrameTooLargeException` |
| 4 | `rejectTruncatedFrame` | short byte array | `WireDecodeException` |
| 5 | `negotiate_prefersBinary` | client [BINARY, JSON] | BINARY |
| 6 | `negotiate_failsNoCommon` | client [JSON], server [BINARY] only | `EncodingNegotiationFailure` |
| 7 | `versionReject_oldFuture` | version 99 | Handshake rejected |
| 8 | `compactionNotice_roundtrip` | CompactionIntent | Equal boundary hash |
| 9 | `iceNotice_roundtrip` | IceArchiveNotice | Equal location |
| 10 | `positionAck_roundtrip` | ack at commit | Equal hash |
| 11 | `jsonEncoding_optional` | JSON handshake | Decodes when negotiator picks JSON |
| 12 | `unknownMessageType` | type code 0xFF | `WireDecodeException` |

-----

## 8. Non-Goals

- **Socket I/O, WebSocket, TCP** — Layer 9 transport adapters.
- **Peer sync state machine (Mode 3)** — Layer 8 Component 23.
- **Stream subscription scheduler** — Component 22.
- **TLS / authentication** — application wraps transport.
- **Multiplexing many namespaces on one socket** — v1 one active namespace per connection (handshake lists all; stream picks first or explicit bind in 22).
- **gRPC / HTTP/2** — out of scope.

-----

## 9. Implementation Notes

### Frame codec implementation
`DefaultWireCodec` writes length prefix last or uses `ByteArrayOutputStream` with precomputed size. Use LE consistently.

### Wire schemas
Define `*WireType` objects in `dev.kdb.wire.schema` mirroring Layer 1/2 patterns. Reuse `IndexHint.toKdbValue()` from `:kdb-index`.

### Compaction adapter
`WireCompactionCoordinator` in `:kdb-stream` or `:kdb-compaction` implements `CompactionCoordinator` by sending `CompactionNotice` — not in this module to avoid cycle; wire module only encodes.

### Module layout
```
dev.kdb.wire
  WireCodec.kt
  WireMessage.kt
  WireMessageType.kt
  HandshakeNegotiator.kt
  schema/
    HandshakeWire.kt
    DeltaCommitWire.kt
    ...
```

### KMP
`commonMain` only.

### Security
Enforce `maxFrameBytes` before allocating payload buffer. Reject negative lengths.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| Frame codec + header | 250 |
| Handshake + negotiator | 300 |
| DeltaCommit + index hints wire | 450 |
| Sync message types (fetch/push/diff) | 500 |
| Snapshot + schema + notices | 400 |
| JSON encoding fallback | 200 |
| Exceptions + tests | 900 |
| **Total** | **~3,000** |
