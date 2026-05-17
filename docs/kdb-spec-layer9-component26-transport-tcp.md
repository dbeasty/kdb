# KDB Component Spec — Layer 9
## Component 26: Transport Adapter — TCP
### `dev.kdb.transport.tcp`

**File:** `kdb-spec-layer9-component26-transport-tcp.md`  
**Layer:** 9 — Platform Adapters  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-transport-tcp`  
**Source sets:** `commonMain`, `jvmMain`, `nativeMain`  
**Depends on:** Layer 7 Component 21 (frame layout), Layer 7 Component 22 (`WireTransport`), Layer 9 `:kdb-transport-core`

-----

## 1. Purpose

Implements **`WireTransport` over a byte stream (TCP)** for JVM backends and Kotlin/Native peers (master §8.7). Reads and writes **length-delimited KDB frames** using the same `ByteArray` layout as `WireCodec.encode` — the stream is a sequence of self-contained frames, each starting with a 4-byte little-endian `frameLength` (Component 21 §4).

Primary backend transport for peer sync, stream coordinators, and future `jdbc:kdb://host:port` network mode.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.transport.core` | `FrameFramer`, `FrameStreamReader`, `FrameStreamWriter`, `TransportUri`, `TransportConnectOptions` |
| `dev.kdb.stream` | `WireTransport`, `WireConnection` |
| `dev.kdb.wire` | `validateFrameLength`, `DEFAULT_MAX_FRAME_BYTES` |
| `dev.kdb.error` | `TransportException`, `ConnectionClosedException` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.transport.tcp

import dev.kdb.stream.WireConnection
import dev.kdb.stream.WireTransport
import dev.kdb.transport.core.TransportConnectOptions

interface TcpWireTransport : WireTransport {
    /** Bind listen socket; invoke [handler] per accepted connection (server role). */
    suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit)
}

fun defaultTcpWireTransport(): TcpWireTransport

data class TcpTransportUri(
    val host: String,
    val port: Int,
    val bind: Boolean = false,  // true when uri is listen form
) {
    companion object {
        fun parse(uri: String): TcpTransportUri
        fun accepts(uri: String): Boolean
    }
}

/** Schemes: `kdb-tcp`, `tcp` (alias). Examples: `kdb-tcp://127.0.0.1:7443`, `kdb-tcp://0.0.0.0:7443?bind=true` */
```

`WireTransport.connect` uses client form; `listen` uses bind form.

-----

## 4. Data Structures

### Stream framing algorithm (`FrameFramer` in `:kdb-transport-core`)
```
READ loop:
  read exactly 4 bytes → frameLength (LE int32)
  validateFrameLength(frameLength)
  read exactly frameLength bytes into buf[0..frameLength-1]
      // buf[0..3] is the length field; total frame size == frameLength
  emit buf.copyOf(frameLength) to WireCodec.decode
```

**Important:** `frameLength` is the **total size of the frame** including the 4-byte length field (matches `DefaultWireCodec.encodeFrameOnly`).

### Write path
```
send(frame: ByteArray):
  require(frame.size >= 4)
  validateFrameLength(readInt32Le(frame, 0))
  writeAll(frame)  // entire frame including length prefix
```

### Buffering
`FrameStreamReader` maintains an internal `ByteBuffer` for partial reads across `read()` calls — required for TCP streaming.

### Server accept loop
```kotlin
data class TcpServerConfig(
    val bindHost: String = "0.0.0.0",
    val port: Int,
    val backlog: Int = 128,
    val maxConnections: Int = 1024,
    val maxFrameBytes: Int = DEFAULT_MAX_FRAME_BYTES,
)
```

### Platform sockets
| Source set | API |
|---|---|
| jvmMain | `java.nio.channels.SocketChannel` + non-blocking + selector, or Ktor TCP |
| nativeMain | POSIX `socket` / `send` / `recv` via `platform.posix` |

commonMain holds framing only; no sockets in commonMain.

-----

## 5. Contracts

### Framing
- **Guarantee:** Never delivers partial frame to `incoming()`.
- **Guarantee:** Preserves frame byte order on a single connection.

### `connect`
- **Postconditions:** TCP established; first `send` may proceed immediately.
- **On failure:** `TransportException` with cause; no leaked half-open reader task.

### `listen`
- **Postconditions:** Binds port; each accept spawns independent `FrameStreamReader` loop.
- **Backpressure:** If handler suspends, reader pauses (bounded channel between reader and handler).

### Half-close
v1: treat peer FIN as `ConnectionClosedException`; full-duplex half-close not exposed.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `TransportException` | Connect refused, reset, malformed length, read timeout |
| `ConnectionClosedException` | EOF before full frame |
| `FrameTooLargeException` | Length prefix exceeds cap |
| `WireDecodeException` | Not thrown here — consumer (`WireCodec`) |

```kotlin
class TransportTimeoutException(val timeoutMs: Long) : TransportException("read timeout after ${timeoutMs}ms")
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `framer_singleFrame` | one frame bytes | one emission |
| 2 | `framer_splitHeader` | 4-byte length sent in 2 reads | one frame |
| 3 | `framer_backToBack` | 2 frames in one read | 2 emissions FIFO |
| 4 | `rejectZeroLength` | length=0 | `WireDecodeException` or `TransportException` |
| 5 | `rejectOversizedLength` | length > max | `FrameTooLargeException` |
| 6 | `eofMidFrame` | disconnect after 2 bytes | `ConnectionClosedException` |
| 7 | `tcpRoundtrip_loopback` | server + client | bytes equal |
| 8 | `listenAccept_twoClients` | 2 parallel connects | isolated streams |
| 9 | `parse_kdbTcpUri` | `kdb-tcp://host:9` | port 9 |
| 10 | `peerSyncOverTcp` | PeerSyncClient + host | heads converge (integration) |
| 11 | `streamCoordinatorOverTcp` | publish delta | subscriber receives |
| 12 | `nativeSmoke` | nativeTest loopback | same as 7 on nativeMain |

-----

## 8. Non-Goals

- **WebSocket** — Component 25.
- **TLS** — use stunnel/nginx or JVM SSL socket wrapper outside v1 module.
- **UDP / QUIC** — out of scope.
- **HTTP upgrade** — not HTTP.
- **Connection pooling** — one `WireConnection` per TCP socket; pool at application layer.

-----

## 9. Implementation Notes

### Shared module `:kdb-transport-core`
Both 25 and 26 depend on:
```kotlin
package dev.kdb.transport.core

class FrameStreamReader(private val maxFrameBytes: Int)
class FrameStreamWriter

data class TransportConnectOptions(
    val connectTimeoutMs: Long = 10_000,
    val readTimeoutMs: Long = 0,
    val maxFrameBytes: Int = DEFAULT_MAX_FRAME_BYTES,
)

object FrameFramer {
    fun readFrame(source: suspend () -> ByteArray?): ByteArray
}
```

### Gradle
```kotlin
include(":kdb-transport-core")
include(":kdb-transport-tcp")
```

### Native
Use non-blocking sockets with coroutine dispatcher; avoid blocking `recv` on Native main thread.

### Integration priority
Implement TCP **before** WebSocket in Layer 9 execution plan — matches Phase 1 JDBC/network backend path in master §14 Build Phases.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| transport-core framer | 350 |
| jvmMain client + server | 900 |
| nativeMain client + server | 600 |
| Tests | 150 |
| **Total (26 + core share)** | **~2,000** (core counted once in Layer 9 subtotal) |
