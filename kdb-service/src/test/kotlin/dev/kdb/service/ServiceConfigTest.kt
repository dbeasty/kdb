package dev.kdb.service

import dev.kdb.config.KdbFeatures
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * ServiceConfig.parse is the entire CLI contract of the service binary - a silent
 * mis-parse here means a production deployment listening on the wrong port or with
 * TLS off, so every flag and every rejection path is pinned down.
 */
class ServiceConfigTest {
    @Test
    fun defaults_memoryRuntimeSqlWireOnPeerSyncOff() {
        val config = ServiceConfig.parse(emptyArray())
        // No --data-dir means the service must fall back to the in-memory runtime.
        assertNull(config.dataDir)
        assertEquals("demo/users", config.namespace)
        assertFalse(config.authRegistry)
        assertNull(config.productConfig.authConfigPath)
        assertNull(config.productConfig.tls)
        // Product defaults: SQL wire is the front door (on by default), peer sync is opt-in.
        assertEquals(
            KdbFeatures.DEFAULT_SQL_LISTEN_URI,
            KdbFeatures.sqlListenUri(config.productConfig.features),
        )
        assertNull(KdbFeatures.peerListenUri(config.productConfig.features))
    }

    @Test
    fun dataDir_selectsFileRuntime() {
        val config = ServiceConfig.parse(arrayOf("--data-dir", "/var/lib/kdb"))
        assertEquals("/var/lib/kdb", config.dataDir)
    }

    @Test
    fun dataDirPlusMemory_isRejected() {
        // The two runtimes are mutually exclusive; accepting both would silently pick one
        // and surprise the operator, so parse must refuse.
        val ex =
            assertFailsWith<IllegalStateException> {
                ServiceConfig.parse(arrayOf("--data-dir", "/tmp/x", "--memory"))
            }
        assertEquals("use either --data-dir or --memory", ex.message)
    }

    @Test
    fun memoryPlusDataDir_isRejectedRegardlessOfFlagOrder() {
        assertFailsWith<IllegalStateException> {
            ServiceConfig.parse(arrayOf("--memory", "--data-dir", "/tmp/x"))
        }
    }

    @Test
    fun explicitMemory_matchesDefault() {
        val config = ServiceConfig.parse(arrayOf("--memory"))
        assertNull(config.dataDir)
    }

    @Test
    fun unknownArgument_isRejectedNotIgnored() {
        // A typo'd flag must fail fast, not be dropped - otherwise "--datadir" would
        // quietly launch an in-memory instance that loses all writes on restart.
        val ex =
            assertFailsWith<IllegalStateException> {
                ServiceConfig.parse(arrayOf("--datadir", "/tmp/x"))
            }
        assertEquals("unknown argument: --datadir", ex.message)
    }

    @Test
    fun namespace_overridesDefault() {
        val config = ServiceConfig.parse(arrayOf("--namespace", "prod/orders"))
        assertEquals("prod/orders", config.namespace)
    }

    @Test
    fun namespaceMissingValue_keepsDefault() {
        // Trailing "--namespace" with no value: parse deliberately keeps the default
        // instead of storing null (the `?: namespace` fallback in parse).
        val config = ServiceConfig.parse(arrayOf("--namespace"))
        assertEquals("demo/users", config.namespace)
    }

    @Test
    fun authNone_isAcceptedAndClearsAuthConfig() {
        val config = ServiceConfig.parse(arrayOf("--auth", "none"))
        assertNull(config.productConfig.authConfigPath)
    }

    @Test
    fun authAnythingElse_isRejectedInV1() {
        assertFailsWith<IllegalStateException> {
            ServiceConfig.parse(arrayOf("--auth", "ldap"))
        }
    }

    @Test
    fun authConfig_flowsIntoProductConfig() {
        val config = ServiceConfig.parse(arrayOf("--auth-config", "/etc/kdb/auth.json"))
        assertEquals("/etc/kdb/auth.json", config.productConfig.authConfigPath)
    }

    @Test
    fun authRegistry_flagIsCaptured() {
        assertTrue(ServiceConfig.parse(arrayOf("--auth-registry")).authRegistry)
    }

    @Test
    fun peerSync_enablesDefaultListenUriAndDerivedStreamUri() {
        val features = ServiceConfig.parse(arrayOf("--peer-sync")).productConfig.features
        assertEquals(KdbFeatures.DEFAULT_PEER_LISTEN_URI, KdbFeatures.peerListenUri(features))
        // main() derives the stream listener from the peer URI - if this comes back null
        // the stream host silently never starts, so pin that peer-sync implies a stream URI.
        assertNotNull(KdbFeatures.streamListenUri(features))
    }

    @Test
    fun listenWs_overridesPeerListenUri() {
        val features =
            ServiceConfig.parse(
                arrayOf("--peer-sync", "--listen-ws", "kdb-ws://0.0.0.0:9443/kdb?bind=true"),
            ).productConfig.features
        assertEquals("kdb-ws://0.0.0.0:9443/kdb?bind=true", KdbFeatures.peerListenUri(features))
    }

    @Test
    fun noPeerSync_disablesPeerListener() {
        val features = ServiceConfig.parse(arrayOf("--no-peer-sync")).productConfig.features
        assertNull(KdbFeatures.peerListenUri(features))
        assertNull(KdbFeatures.streamListenUri(features))
    }

    @Test
    fun listenSqlWs_overridesSqlListenUri() {
        val features =
            ServiceConfig.parse(
                arrayOf("--listen-sql-ws", "kdb-ws://0.0.0.0:9444/kdb?bind=true"),
            ).productConfig.features
        assertEquals("kdb-ws://0.0.0.0:9444/kdb?bind=true", KdbFeatures.sqlListenUri(features))
    }

    @Test
    fun tlsFlags_buildFullTlsConfig() {
        val config =
            ServiceConfig.parse(
                arrayOf(
                    "--tls",
                    "--tls-key-store", "/etc/kdb/server.p12",
                    "--tls-trust-store", "/etc/kdb/trust.p12",
                    "--tls-require-client-auth",
                ),
            )
        val tls = assertNotNull(config.productConfig.tls)
        assertTrue(tls.enabled)
        assertEquals("/etc/kdb/server.p12", tls.keyStorePath)
        assertEquals("/etc/kdb/trust.p12", tls.trustStorePath)
        assertTrue(tls.requireClientAuth)
    }

    @Test
    fun noTls_producesDisabledTlsConfig() {
        // --no-tls materializes a config with enabled=false (rather than null) so it can
        // veto a config-file's tls block; the transport mapping must then treat it as
        // plaintext - covered in ServiceTlsTest.
        val tls = assertNotNull(ServiceConfig.parse(arrayOf("--no-tls")).productConfig.tls)
        assertFalse(tls.enabled)
    }

    @Test
    fun configFile_isLoadedAndCliOverridesWin() {
        // Precedence contract: file supplies the base, CLI flags override field-by-field.
        val file: Path = Files.createTempFile("kdb-service-test", ".json")
        try {
            Files.writeString(
                file,
                """
                {
                  "features": {
                    "peerSync": { "enabled": true, "listenUri": "kdb-ws://0.0.0.0:7001/kdb?bind=true" },
                    "sqlWire": { "enabled": true, "listenUri": "kdb-ws://0.0.0.0:7002/kdb?bind=true" }
                  },
                  "authConfigPath": "/from/file/auth.json"
                }
                """.trimIndent(),
            )
            val config =
                ServiceConfig.parse(
                    arrayOf(
                        "--config", file.toString(),
                        "--listen-ws", "kdb-ws://0.0.0.0:8001/kdb?bind=true",
                    ),
                )
            val features = config.productConfig.features
            // CLI --listen-ws beats the file's peer URI...
            assertEquals("kdb-ws://0.0.0.0:8001/kdb?bind=true", KdbFeatures.peerListenUri(features))
            // ...while untouched file settings survive.
            assertEquals("kdb-ws://0.0.0.0:7002/kdb?bind=true", KdbFeatures.sqlListenUri(features))
            assertEquals("/from/file/auth.json", config.productConfig.authConfigPath)
        } finally {
            Files.deleteIfExists(file)
        }
    }
}
