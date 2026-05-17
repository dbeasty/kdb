# KDB Component Spec — Layer 9
## Component 25: Transport Adapter — WebSocket
### `dev.kdb.transport.ws`

**File:** `kdb-spec-layer9-component25-transport-websocket.md`  
**Layer:** 9 — Platform Adapters  
**Status:** Implementation-ready  
**Gradle module:** `:kdb-transport-ws`  
**Source sets:** `commonMain` (URI + config + framing helpers), `jsMain`, `jvmMain`  
**Depends on:** Layer 7 (`WireCodec`, `validateFrameLength`), Layer 7 Component 22 (`WireTransport`, `WireConnection`), Layer 9 shared framing (`:kdb-transport-core`)

-----

## 1. Purpose

Implements **`WireTransport` over WebSocket** for browser clients (jsMain) and JVM WebSocket clients/servers (jvmMain). Each **binary WebSocket message** carries exactly one complete KDB wire frame (master §8.4 / Component 21) — no additional envelope beyond the frame length prefix already inside the frame bytes.

Enables `StreamSubscriber.connect("wss://host:port/ns")`, `PeerSyncClient.connect`, and future network JDBC without changing `:kdb-stream` or `:kdb-wire`.

-----

## 2. Dependencies

| Module | Interfaces used |
|---|---|
| `dev.kdb.transport.core` (shared) | `FrameFramer`, `TransportUri`, `TransportConnectOptions` |
| `dev.kdb.stream` (22) | `WireTransport`, `WireConnection` |
| `dev.kdb.wire` (21) | `validateFrameLength`, `DEFAULT_MAX_FRAME_BYTES` |
| `dev.kdb.error` | `TransportException`, `ConnectionClosedException` |

-----

## 3. Public Interface

```kotlin
package dev.kdb.transport.ws

import dev.kdb.stream.WireConnection
import dev.kdb.stream.WireTransport
import dev.kdb.transport.core.TransportConnectOptions

/** Factory — select via `defaultWebSocketWireTransport()` on each platform. */
interface WebSocketWireTransport : WireTransport {
    /** Optional JVM-only: bind coordinator before clients connect. */
    suspend fun listen(uri: String, handler: suspend (WireConnection) -> Unit)
}

fun defaultWebSocketWireTransport(): WebSocketWireTransport

/** Parsed URI — schemes `ws`, `wss`, `kdb-ws`, `kdb-wss`. */
data class WebSocketTransportUri(
    val secure: Boolean,
    val host: String,
    val port: Int,
    val path: String,
    val query: Map<String, String>,
) {
    fun toWireUri(): String  // canonical `wss://host:port/path?...`
}

object WebSocketTransportUriParser {
    fun parse(uri: String): WebSocketTransportUri
    fun accepts(uri: String): Boolean
}

/** jvmMain — minimal server for integration tests and embedded coordinators. */
interface WebSocketServer {
    suspend fun start(bindUri: String)
    suspend fun stop()
    val activeConnections: Int
}

fun inProcessWebSocketServer(): WebSocketServer  // jvmTest: pairs with jsMain/client via loopback
```

`WebSocketWireTransport.connect` accepts:

| Scheme | Example | Use |
|---|---|---|
| `ws` / `wss` | `wss://db.example.com:7443/stream` | Standard |
| `kdb-ws` / `kdb-wss` | `kdb-wss://localhost:7443` | KDB alias (same semantics) |

Query parameters (optional, v1):

| Param | Meaning |
|---|---|
| `namespace` | Default namespace id for handshake |
| `maxFrameBytes` | Override frame cap (default 16 MiB) |

-----

## 4. Data Structures

### WebSocket message mapping
```
Client ──binary WS message──► Server
         body = WireCodec.encode(message)   // full frame incl. 4-byte LE length
```

Text WebSocket frames are **rejected** with `TransportException("text frames not supported")`.

### Connection state
```kotlin
internal data class WsConnectionState(
    val uri: WebSocketTransportUri,
    val options: TransportConnectOptions,
    val open: Boolean,
    val maxFrameBytes: Int,
)
```

### jsMain
Uses browser `WebSocket` with `binaryType = "arraybuffer"`. `send` copies `ByteArray` to `ArrayBuffer`.

### jvmMain client
Uses Java 11+ `java.net.http.WebSocket` or OkHttp — implementation choice; spec requires non-blocking `suspend send` and `Flow` receive.

### jvmMain server
`WebSocketServer` uses embedded JDK HTTP server WebSocket or Ktor CIO — **test-focused v1**; production deployments may front with nginx.

### Ping / keepalive
v1: rely on platform WebSocket ping; optional `TransportConnectOptions.pingIntervalMs` on jvmMain only.

-----

## 5. Contracts

### `connect(uri, options)`
- **Preconditions:** URI parseable; host reachable (best-effort).
- **Postconditions:** Returns open `WireConnection`. First `incoming()` may emit server-initiated frames after handshake (Component 22/23 responsibility).
- **Guarantee:** `send(frame)` delivers one binary message whose payload equals `frame` byte-for-byte.

### `send` / `incoming`
- **Preconditions:** `frame.size` ≤ `maxFrameBytes`; frame passes `validateFrameLength` on first 4 bytes.
- **Postconditions:** Framing errors surface as `TransportException`; peer close → `ConnectionClosedException` on next `send`/`incoming`.

### `listen` (jvmMain)
- **Postconditions:** Each accepted socket wrapped as `WireConnection`; handler invoked per connection; handler completion or error closes socket.

### Threading / coroutines
All public APIs are `suspend`-safe; `incoming()` is cold `Flow` collected on caller context.

-----

## 6. Error Cases

| Exception | When |
|---|---|
| `TransportException` | DNS failure, HTTP upgrade failed, text frame received, frame too large |
| `ConnectionClosedException` | Socket closed while sending or in `incoming` |
| `IllegalArgumentException` | Unrecognized URI scheme |
| `FrameTooLargeException` | Delegated from `validateFrameLength` before send |

```kotlin
class TransportException(message: String, cause: Throwable? = null) : KdbException(message, cause)
class ConnectionClosedException(message: String = "connection closed") : KdbException(message)
```

-----

## 7. Test Cases

| # | Name | Input | Expected |
|---|---|---|---|
| 1 | `parse_wss_defaultPort` | `wss://host/path` | port 443, secure=true |
| 2 | `parse_kdbWs_alias` | `kdb-ws://127.0.0.1:7443` | maps to ws://127.0.0.1:7443 |
| 3 | `roundtrip_singleFrame` | in-process server + client | same bytes received |
| 4 | `rejectTextFrame` | server sends text | `TransportException` |
| 5 | `rejectOversizedSend` | frame > max | `FrameTooLargeException` |
| 6 | `closeEmitsException` | close socket, then send | `ConnectionClosedException` |
| 7 | `multiFrameOrder` | 3 sequential sends | FIFO on `incoming` |
| 8 | `handshakeOverWs` | StreamSubscriber + coordinator | handshake completes (integration) |
| 9 | `namespaceQueryParam` | `?namespace=demo` | parsed into uri.query |
| 10 | `serverFanout_twoClients` | listen + 2 connects | both receive broadcast |
| 11 | `wssRequiresTls` | jvmTest with self-signed | connect with trustAll test flag |
| 12 | `jsConnect_loopback` | jsTest in-memory server shim | connect succeeds |

-----

## 8. Non-Goals

- **TCP transport** — Component 26.
- **TLS termination / cert pinning policy** — application or reverse proxy; test-only `trustAll` flag.
- **WebRTC datachannel** — future (master §15).
- **Multiplexing namespaces on one socket** — one logical session per connection (handshake lists namespaces).
- **Compression inside WS** — per-message compression disabled v1; snapshot zstd stays in payload (Component 21).

-----

## 9. Implementation Notes

### Module layout
```
kdb-transport-ws/
  commonMain/  WebSocketTransportUri.kt, WebSocketWireTransport.kt (expect)
  jsMain/      JsWebSocketWireTransport.kt
  jvmMain/     JvmWebSocketWireTransport.kt, JvmWebSocketServer.kt
  jvmTest/     InProcessWebSocketServerTest.kt
```

### Dependency
```kotlin
// build.gradle.kts
commonMain.dependencies {
    api(project(":kdb-stream"))
    implementation(project(":kdb-transport-core"))
}
```

### jsMain constraints
No `java.net` — use `org.w3c.dom.WebSocket` via Kotlin/JS interop or Ktor JS client.

### Security
Validate frame length before allocating receive buffer. Cap pending incomplete frame buffer per connection.

-----

## 10. Estimated Lines

| Sub-component | Est. NBNC lines |
|---|---|
| URI parser + options | 150 |
| commonMain API | 100 |
| jsMain client | 400 |
| jvmMain client + test server | 600 |
| Tests (jvm + js where CI allows) | 250 |
| **Total** | **~1,500** |
