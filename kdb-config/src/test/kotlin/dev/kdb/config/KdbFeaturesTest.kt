package dev.kdb.config

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KdbFeaturesTest {
    @Test
    fun defaults_peerSyncDisabled() {
        val f = KdbFeatures.DEFAULT
        assertEquals(false, f.peerSync.enabled)
        assertEquals(true, f.sqlWire.enabled)
        assertNull(KdbFeatures.peerListenUri(f))
        assertEquals(KdbFeatures.DEFAULT_SQL_LISTEN_URI, KdbFeatures.sqlListenUri(f))
    }

    @Test
    fun requireNetworkPeerSync_blocksTcpWhenDisabled() {
        assertFailsWith<IllegalArgumentException> {
            requireNetworkPeerSyncEnabled(KdbFeatures.DEFAULT, "kdb-tcp://127.0.0.1:9000")
        }
    }

    @Test
    fun requireNetworkPeerSync_allowsMemoryWhenDisabled() {
        requireNetworkPeerSyncEnabled(KdbFeatures.DEFAULT, "memory://hub")
    }

    @Test
    fun loadTlsConfigFromJson() {
        val json =
            """
            {
              "tls": {
                "enabled": true,
                "keyStorePath": "/etc/kdb/server.p12",
                "trustStorePath": "/etc/kdb/trust.p12",
                "requireClientAuth": true
              }
            }
            """.trimIndent()
        val config = kotlinx.serialization.json.Json { ignoreUnknownKeys = true }
            .decodeFromString(KdbProductConfig.serializer(), json)
        assertNotNull(config.tls)
        assertTrue(config.tls!!.enabled)
        assertEquals("/etc/kdb/server.p12", config.tls!!.keyStorePath)
        assertTrue(config.tls!!.requireClientAuth)
    }

    @Test
    fun resolveOverrides_enablePeerSync() {
        val resolved =
            resolveKdbProductConfig(
                configFile = null,
                dataDirConfig = null,
                peerSyncEnabledOverride = true,
            )
        assertEquals(true, resolved.features.peerSync.enabled)
        assertEquals(KdbFeatures.DEFAULT_PEER_LISTEN_URI, KdbFeatures.peerListenUri(resolved.features))
    }
}
