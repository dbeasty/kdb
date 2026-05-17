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

    /** Minimal valid KDB wire frame (total length 8 per [validateFrameLength]). */
    private fun minimalFrame(payload: Byte = 42): ByteArray =
        byteArrayOf(8, 0, 0, 0, payload, 0, 0, 0)

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
            delay(300)
            val port = transport.networkListenPort()
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
            delay(300)
            val port = transport.networkListenPort()
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
            delay(300)
            val port = transport.networkListenPort()
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
            delay(300)
            val port = transport.networkListenPort()
            val failure =
                runCatching {
                    transport.connect("kdb-ws://localhost:$port/kdb")
                }
            assertTrue(failure.isFailure)
            listenJob.cancel()
        }
}
