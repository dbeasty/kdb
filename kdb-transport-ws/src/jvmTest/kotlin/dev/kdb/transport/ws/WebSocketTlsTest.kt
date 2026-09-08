package dev.kdb.transport.ws

import dev.kdb.error.TransportException
import dev.kdb.transport.core.TransportConnectOptions
import dev.kdb.transport.core.TransportTlsSettings
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class WebSocketTlsTest {
    private fun newTransport(): JvmWebSocketWireTransport =
        defaultWebSocketWireTransport() as JvmWebSocketWireTransport

    /**
     * Minimal valid KDB wire frame: the 12-byte header (little-endian total length, message
     * type, protocol version, correlation id) plus one payload byte, so 13 in total.
     *
     * This used to build an 8-byte buffer, on the strength of [validateFrameLength] accepting a
     * declared length of 8 - but the frame header alone is 12 bytes, so no such frame can exist;
     * Go's ValidateFrameLength has always rejected it. The floor was the bug, and this fixture
     * was the only thing depending on it.
     */
    private fun minimalFrame(payload: Byte = 42): ByteArray =
        byteArrayOf(13, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, payload)

    @Test
    fun wssRequiresTls_settingsMissing() {
        assertFailsWith<TransportException> {
            runBlocking {
                newTransport().connect(
                    "kdb-wss://127.0.0.1:1/kdb",
                    TransportConnectOptions(),
                )
            }
        }
    }

    @Test
    fun wssRoundtrip_withTrustAll() =
        runBlocking {
            val transport = newTransport()
            val material = TlsTestCertificates.material()
            val serverTls =
                TransportTlsSettings(
                    enabled = true,
                    keyStorePath = material.serverKeyStore.toString(),
                    keyStorePassword = material.password,
                    trustAll = true,
                )
            val listenJob =
                async {
                    try {
                        transport.listen(
                            "kdb-wss://localhost:0/kdb?bind=true",
                            TransportConnectOptions(tls = serverTls),
                        ) { conn ->
                            val frame = conn.incoming().first()
                            conn.send(frame)
                        }
                    } catch (_: CancellationException) {
                    }
                }
            val port = awaitListenPort(transport)
            val client =
                transport.connect(
                    "kdb-wss://localhost:$port/kdb",
                    TransportConnectOptions(
                        tls = TransportTlsSettings(enabled = true, trustAll = true),
                    ),
                )
            val payload = minimalFrame(7)
            client.send(payload)
            assertContentEquals(payload, client.incoming().first())
            client.close()
            listenJob.cancel()
        }

    @Test
    fun wssRoundtrip_withTrustStore() =
        runBlocking {
            val transport = newTransport()
            val material = TlsTestCertificates.material()
            val serverTls =
                TransportTlsSettings(
                    enabled = true,
                    keyStorePath = material.serverKeyStore.toString(),
                    keyStorePassword = material.password,
                )
            val listenJob =
                async {
                    try {
                        transport.listen(
                            "kdb-wss://localhost:0/kdb?bind=true",
                            TransportConnectOptions(tls = serverTls),
                        ) { conn ->
                            val frame = conn.incoming().first()
                            conn.send(frame)
                        }
                    } catch (_: CancellationException) {
                    }
                }
            val port = awaitListenPort(transport)
            val client =
                transport.connect(
                    "kdb-wss://localhost:$port/kdb",
                    TransportConnectOptions(
                        tls =
                            TransportTlsSettings(
                                enabled = true,
                                trustStorePath = material.trustStore.toString(),
                                trustStorePassword = material.password,
                            ),
                    ),
                )
            val payload = minimalFrame(9)
            client.send(payload)
            assertContentEquals(payload, client.incoming().first())
            client.close()
            listenJob.cancel()
        }

    @Test
    fun mTls_clientCertificateRequired() =
        runBlocking {
            val transport = newTransport()
            val material = TlsTestCertificates.material()
            val serverTls =
                TransportTlsSettings(
                    enabled = true,
                    keyStorePath = material.serverKeyStore.toString(),
                    keyStorePassword = material.password,
                    trustStorePath = material.trustStore.toString(),
                    trustStorePassword = material.password,
                    requireClientAuth = true,
                )
            val listenJob =
                async {
                    try {
                        transport.listen(
                            "kdb-wss://localhost:0/kdb?bind=true",
                            TransportConnectOptions(tls = serverTls),
                        ) { conn ->
                            val frame = conn.incoming().first()
                            conn.send(frame)
                        }
                    } catch (_: CancellationException) {
                    }
                }
            val port = awaitListenPort(transport)
            assertFailsWith<Exception> {
                transport.connect(
                    "kdb-wss://localhost:$port/kdb",
                    TransportConnectOptions(
                        tls =
                            TransportTlsSettings(
                                enabled = true,
                                trustStorePath = material.trustStore.toString(),
                                trustStorePassword = material.password,
                            ),
                    ),
                )
            }
            val clientWithCert =
                transport.connect(
                    "kdb-wss://localhost:$port/kdb",
                    TransportConnectOptions(
                        tls =
                            TransportTlsSettings(
                                enabled = true,
                                keyStorePath = material.clientKeyStore.toString(),
                                keyStorePassword = material.password,
                                trustStorePath = material.trustStore.toString(),
                                trustStorePassword = material.password,
                            ),
                    ),
                )
            val payload = minimalFrame(3)
            clientWithCert.send(payload)
            assertContentEquals(payload, clientWithCert.incoming().first())
            clientWithCert.close()
            listenJob.cancel()
        }

    @Test
    fun plainClient_toTlsServer_fails() =
        runBlocking {
            val transport = newTransport()
            val material = TlsTestCertificates.material()
            val serverTls =
                TransportTlsSettings(
                    enabled = true,
                    keyStorePath = material.serverKeyStore.toString(),
                    keyStorePassword = material.password,
                    trustAll = true,
                )
            val listenJob =
                async {
                    try {
                        transport.listen(
                            "kdb-wss://localhost:0/kdb?bind=true",
                            TransportConnectOptions(tls = serverTls),
                        ) { }
                    } catch (_: CancellationException) {
                    }
                }
            val port = awaitListenPort(transport)
            val failure =
                runCatching {
                    transport.connect("kdb-ws://localhost:$port/kdb")
                }
            assertTrue(failure.isFailure)
            listenJob.cancel()
        }

    /**
     * Waits for the listener to be bound and accepting, and returns its port.
     *
     * This replaces a fixed `delay(300)`. Three hundred milliseconds is enough on an idle
     * developer machine and is not enough on a loaded CI runner, where the TLS listener can still
     * be coming up when the client dials - which surfaced as an intermittent
     * `java.net.ConnectException` from the connect below, on a test that had nothing to do with
     * whatever change was being built.
     *
     * Polling for readiness rather than sleeping for it also makes the common case faster: the
     * listener is usually up within a few milliseconds, and the old delay paid the full 300 every
     * time.
     *
     * [JvmWebSocketWireTransport.networkListenPort] throws until the server object exists, and the
     * socket can accept a moment after that, so both are waited on: first the port, then a probe
     * connection that proves something is listening on it.
     */
    private suspend fun awaitListenPort(
        transport: JvmWebSocketWireTransport,
        timeoutMillis: Long = 10_000,
    ): Int {
        val deadline = System.nanoTime() + timeoutMillis * 1_000_000
        var lastFailure: Throwable? = null
        while (System.nanoTime() < deadline) {
            val port =
                try {
                    transport.networkListenPort()
                } catch (t: IllegalStateException) {
                    lastFailure = t
                    delay(5)
                    continue
                }
            if (port > 0 && portAccepts(port)) return port
            delay(5)
        }
        throw AssertionError(
            "WebSocket listener was not accepting within ${timeoutMillis}ms", lastFailure)
    }

    /** Probes the port with a plain TCP connect - enough to know the accept loop is running. */
    private fun portAccepts(port: Int): Boolean =
        try {
            java.net.Socket().use { probe ->
                probe.connect(java.net.InetSocketAddress("localhost", port), 200)
                true
            }
        } catch (_: java.io.IOException) {
            false
        }

}
