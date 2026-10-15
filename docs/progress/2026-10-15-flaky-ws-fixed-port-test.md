# 2026-10-15 — flaky `twoListenersShareAFixedPortByPath` (branch `fix/flaky-ws-fixed-port-test`)

`NetworkWebSocketWireTransportTest.twoListenersShareAFixedPortByPath` (kdb-transport-ws, jvmTest)
failed in the nightly `kotlin-build` job on about half the runs. Examples: 3229aa6 (run
38700763265), 57408cf (run 38672895140) and 5f969ac (run 38742703774). Gradle's summary line says
`java.net.BindException at NetworkWebSocketWireTransportTest.kt:150`. It turned out to be a product
defect. The suspected port race was not the cause.

## What it was not

The suspicion was the test's port grab: `ServerSocket(0).use { it.localPort }`, closed, then bound
again later, so another process could take the port in between. But line 150 is only the test
method's `runBlocking` frame. The JUnit XML in the `kotlin-test-results-nightly` artifact has the
full trace, and the bind that fails is line 173: `java.net.ServerSocket(port).close() // released`.
That is the check that the port is free again after both listeners stop. Both listeners had
bound, and both paths had answered.

## Cause: a cancelled listener skipped its release, and the server kept listening

- `JvmWebSocketWireTransport.listen` waits in `awaitCancellation()`, and its `finally` calls
  `SharedServers.release`. That takes `mutex.withLock`, removes the path, and stops the server
  when no paths are left.
- The test cancels the two listeners together (`a.cancel(); b.cancel()`), as a service shutting
  down would. Both `finally` blocks then race for the mutex.
- The loser is already cancelled. `Mutex.lock` suspends cancellably, so it throws
  `CancellationException` instead of waiting. Its route is never removed and `stop()` never runs.
- The server keeps its listening socket open for the life of the JVM. Nothing else ever releases
  it, and the port stays taken.

**Evidence** (Linux, `eclipse-temurin:21-jdk` container, the test class run in a loop through
JUnitCore against the locally built classes):

- Before the fix: 3/10 pass, then 0/4 with instrumentation. In an isolated container nothing else
  could take the port.
- The instrumentation dumped `/proc/net/tcp6` on the failure. The port shows state `0A` (LISTEN)
  on `::ffff:127.0.0.1`, and still does 500 ms later; a second bind attempt fails too. The other
  entries are the test's clients in TIME_WAIT.
- After the fix: 30/30 pass.
- Control: the new test with only the release fix reverted fails 9/10.
- The Go/Python probes in the same container showed that accepted sockets (open, half-closed or
  in TIME_WAIT) on the port do not block a `SO_REUSEADDR` rebind on Linux. That ruled out
  lingering connections.

## Fix

- `listen`'s `finally` now runs the release under `withContext(NonCancellable)`
  (`kdb-transport-ws/src/jvmMain/kotlin/dev/kdb/transport/ws/JvmWebSocketWireTransport.kt`).
- `SharedServers` now registers a port-0 server under the port it got (`host:actualPort`). A later
  listen on that port shares the server instead of failing to bind a port this process already
  holds. It still binds a new server for each port-0 listen.
- `networkServer` is now `@Volatile`, since tests poll it from another thread.
- The test no longer grabs and closes a port first. The first listener binds port 0, the test
  reads the port back (`networkListenPort()`), and the second listener joins that port. The race
  the issue suspected is gone too, even though it was not the cause.
- No other test in the module, or anywhere in the repo's Kotlin, uses the
  `ServerSocket(0).use { it.localPort }` pattern.

**Verified:** `./gradlew :kdb-transport-ws:jvmTest --no-daemon --rerun` passed 4 times on macOS.
`:kdb-service:test`, `:kdb-server:test`, `:kdb-replica:jvmTest` and `:kdb-embed:jvmTest` pass.

## Found along the way

The defect is not limited to tests. Any two listeners sharing a port and cancelled together could
leave that port bound until the process exits. kdb-service's peer sync (`/kdb`) and stream
(`/kdb/stream`) share a port by default. The test passed all 4 local runs on macOS, which may be
why it showed up only on the Linux nightly.
