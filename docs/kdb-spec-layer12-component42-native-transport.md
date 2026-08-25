# Component 42 — Native Transport (TCP, Kotlin/Native)

Layer 12 (renumbered from Revision 1's Component 32 — that number collided with this repo's own,
already-shipped "Component 32: Stored Procedure Engine"; see the gap analysis §7).

**Deprioritized in Revision 2.** The maturity audit confirmed a full Go engine port already exists
in this repo (`go/kdb/...`), including a working embed runtime (`go/kdb/embed`) — ordinary Go, not
Kotlin/Native. If that can be linked into Zolik's `gomobile`-built mobile binary directly (open
question, not yet answered — see the gap analysis §1.1), Zolik's mobile-embedding goal may not need
Kotlin/Native on iOS/Android at all, which is exactly what this component and Component 43 exist to
enable. **Don't invest here until that question is answered** — this spec is preserved as-is
(still accurate; nothing about the stub itself changed) in case the Go-embed path doesn't pan out
and Kotlin/Native mobile targets turn out to be needed after all.

Layer 12. Depends on Layer 9 (Component 26, TCP transport).

## 1. Purpose

`kdb-transport-tcp`'s `nativeMain` implementation throws on both `connect()` and `listen()` — it
is a stub, not an adapter. This component fills it in with a real POSIX-socket-backed
implementation of the existing `WireTransport` contract, so that any Kotlin/Native target
(`linuxX64`, `macosArm64`, and — once Component 43 adds the targets — `iosArm64`,
`iosSimulatorArm64`, `androidNativeArm64`) can act as a KDB server or peer-sync client, not only
JVM/JS. This is what lets a KDB instance embedded inside a mobile app, or a KDB-backed LAN host
binary, participate in networked sync at all.

## 2. Dependencies

- `kdb-transport-core` — `WireTransport`, `WireConnection` interfaces (Layer 9, Component 25/26's
  shared contract).
- `kdb-wire` — frame codec (`kdb-spec-layer7-component21-wire-protocol-framing.md`); this
  component only supplies the socket, framing is unchanged.
- Kotlin/Native POSIX interop (`platform.posix.*`) — no third-party sockets library; the existing
  `jvmMain` adapter uses raw `java.net.ServerSocket`/`Socket`, and this component is the
  Kotlin/Native equivalent, not a rewrite of the framing or handshake layers above it.

## 3. Public Interface

```kotlin
// kdb-transport-tcp/src/nativeMain/kotlin/dev/kdb/transport/tcp/NativeTcpWireTransport.kt
package dev.kdb.transport.tcp

actual class NativeTcpWireTransport : WireTransport {
    actual override suspend fun connect(uri: String): WireConnection
    actual override suspend fun listen(
        uri: String,
        onConnection: suspend (WireConnection) -> Unit,
    ): WireListener
}

// A listening socket handle, mirroring the shape JvmTcpWireTransport already returns
// implicitly via its listen loop — made explicit here so callers can close a native
// listener deterministically (Kotlin/Native has no GC-driven socket cleanup to rely on).
interface WireListener {
    val boundPort: Int
    suspend fun close()
}

// Internal, not part of the public surface, but load-bearing enough to name here:
// a native `WireConnection` implementation backed by a raw POSIX fd.
internal class PosixSocketWireConnection(
    private val fd: Int,
) : WireConnection {
    override suspend fun send(frame: ByteArray)
    override suspend fun receive(): ByteArray
    override suspend fun close()
}
```

`WireTransport`/`WireConnection` themselves are unchanged — this component is purely filling in
the `actual` side of an `expect`/`actual` pair that already exists (per Layer 9's dependency
rules: "Transport implements `WireTransport` from Component 22").

## 4. Data Structures

```kotlin
// Internal socket-address parsing, mirroring what JvmTcpWireTransport already does for
// "host:port" URIs — kept private, not a new public wire concept.
private data class TcpEndpoint(val host: String, val port: Int)
```

No new wire-visible data structures. Frames on the wire are byte-identical to what
`JvmTcpWireTransport` produces — that's the point: a native and a JVM node must be able to talk to
each other over this transport without either side knowing which runtime the other is.

## 5. Contracts

- `connect(uri)`: given `"tcp://host:port"`, opens a POSIX socket, performs the standard blocking
  `connect(2)` off the calling coroutine's thread (via `withContext(Dispatchers.IO)` or the
  Kotlin/Native equivalent worker dispatch — native coroutines have no shared-memory `IO`
  dispatcher by default, so this must be built explicitly, not assumed), and returns a
  `WireConnection` once the TCP handshake completes. Throws `TransportException` on any socket
  error (matches `JvmTcpWireTransport`'s existing exception contract) — never a raw `errno`.
- `listen(uri, onConnection)`: binds and listens on the given host:port, accepting connections in
  a loop and invoking `onConnection` once per accepted connection, each on its own coroutine/worker
  — mirrors `JvmTcpWireTransport`'s accept-loop shape exactly, so `kdb-server`'s
  `runSqlWireListen` requires *no changes* to run under this transport; it only depends on
  `WireTransport`, never on `JvmTcpWireTransport` concretely.
- `WireListener.close()`: closes the listening socket; any in-flight `onConnection` callbacks are
  allowed to finish, no new ones start. Idempotent — calling twice is not an error.
- Backpressure/framing: unchanged from Component 21 — this component only ever hands raw bytes
  across the POSIX read/write boundary; frame length prefixing, message types, and correlation ids
  are entirely `kdb-wire`'s concern and this component must not duplicate or reinterpret them.
- Thread/worker safety: Kotlin/Native has no shared mutable state across workers without explicit
  transfer. `PosixSocketWireConnection` must not be shared across native workers without a documented
  ownership transfer — the accept loop hands each connection to exactly one worker/coroutine and
  that ownership does not move again during the connection's lifetime.

## 6. Error Cases

- `TransportException("connect failed: <reason>")` — DNS resolution failure, connection refused,
  timeout.
- `TransportException("bind failed: <reason>")` — port already in use, permission denied
  (privileged port), address family mismatch.
- `TransportException("connection closed")` — raised from `send`/`receive` on a connection whose
  peer has closed the socket; callers (`kdb-peer-sync`, `kdb-server`) already handle this
  exception type from the JVM adapter, so no new catch sites are needed upstream.
- Malformed URI (`connect("not-a-uri")`) — `IllegalArgumentException`, thrown before any socket
  syscall, matching `JvmTcpWireTransport`'s existing parse-then-connect ordering.

## 7. Test Cases

1. **Loopback connect/listen round trip** — `listen("tcp://127.0.0.1:0", ...)` (port 0 = OS
   picks), read back `boundPort`, `connect` to it, send a frame, assert the `onConnection` side
   receives byte-identical bytes.
2. **Multiple sequential connections to one listener** — three `connect` calls in turn, each
   independent, listener stays open throughout.
3. **Concurrent connections** — N connects launched concurrently, assert `onConnection` fires N
   times with no dropped or duplicated frames.
4. **Cross-runtime interop** — a JVM `JvmTcpWireTransport` listener, a native `connect()`, full
   handshake (Component 1, `0x01`) succeeds — this is the test that actually matters for Zolik's
   use case and should be in `kdb-integration`, not just this module.
5. **Listener close mid-accept-loop** — `close()` while a connection is being accepted; no crash,
   no leaked fd (verify via `lsof`/fd-count check on Linux CI).
6. **connect() to a closed/refused port** — throws `TransportException`, not a native crash or an
   uncaught `errno`-derived exception type.
7. **listen() on an already-bound port** — throws `TransportException("bind failed")` rather than
   hanging or silently rebinding.
8. **Large frame (> 1 MB) round trip** — exercises partial-read/partial-write looping in the raw
   POSIX read/write calls, which (unlike a buffered `java.net.Socket`) can return short reads.
9. **Send after peer closes** — `send()` on a connection whose peer already closed raises
   `TransportException("connection closed")` rather than a silent no-op or a raw `SIGPIPE` crash
   (must set `SO_NOSIGPIPE`/`MSG_NOSIGNAL` as platform-appropriate).
10. **Idempotent close** — calling `WireListener.close()` twice does not throw.

## 8. Non-Goals

- TLS. `JvmTransportTls.kt` exists for the JVM/WS path; a native TLS adapter (if ever needed) is a
  separate component layered on top of this one, not part of it.
- WebSocket framing for native. This component is TCP only, matching Component 26's scope; a
  native WebSocket transport is a separate, larger undertaking (HTTP upgrade handshake in raw
  POSIX sockets) explicitly out of scope here and not currently needed by Zolik's plan (its LAN/
  mobile path is raw TCP; WebSocket is for browser nodes, which are JS, not Kotlin/Native).
- iOS/Android target *configuration* (adding `iosArm64()` etc. to `build.gradle.kts`). That is
  Component 43's job; this component's `nativeMain` source is target-agnostic Kotlin/Native and
  will compile for whichever native targets the module declares, once declared.

## 9. Implementation Notes

- Use `platform.posix.socket`/`bind`/`listen`/`accept`/`connect`/`read`/`write`/`close` directly
  via Kotlin/Native's `cinterop`-generated POSIX bindings — no external native sockets library;
  this matches the project's existing "no external storage dependencies" philosophy (§1.3 of the
  master spec) extended to networking.
- `accept()` blocks the calling thread; run the accept loop on a dedicated
  `kotlinx.coroutines` native dispatcher backed by its own worker, one worker per listener, so it
  never blocks the caller's dispatcher.
- Short reads/writes are normal for raw POSIX sockets (unlike `java.net.Socket`, which buffers
  internally) — every `read`/`write` call site must loop until the requested byte count is
  satisfied or an error/EOF occurs. This is the single most likely source of a subtle native-only
  bug if skipped (the JVM adapter's behavior would mask it entirely, which is exactly why cross-
  runtime interop, test 4, matters more than same-runtime native/native tests).
- `SIGPIPE` handling: writing to a socket whose peer has closed raises `SIGPIPE` by default on
  POSIX, which without `SO_NOSIGPIPE` (BSD/Darwin) or `MSG_NOSIGNAL` (Linux) terminates the
  process rather than returning an error — this must be set per-platform via `expect`/`actual` if
  the flag differs, or handled with a signal handler on platforms lacking both.
- Validate on `linuxX64` and `macosArm64` first (both already have other native targets configured
  in this repo today) before iOS — per the gap analysis's sequencing recommendation, this
  component should not wait on Component 43's iOS work to be useful.

## 10. Estimated Lines

500–900 NBNC: ~150 for the transport/listener shell, ~250 for the buffered read/write-loop
socket wrapper, ~150 for error mapping and platform-specific signal handling, ~150–350 for tests
(native test infra tends to need more boilerplate than JVM's).
