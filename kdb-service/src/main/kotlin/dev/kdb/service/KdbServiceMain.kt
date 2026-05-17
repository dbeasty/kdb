package dev.kdb.service

import dev.kdb.config.KdbFeatures
import dev.kdb.config.KdbProductConfig
import dev.kdb.config.resolveKdbProductConfig
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.materializeCommit
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.runPeerSyncOverWebSocketListen
import dev.kdb.auth.AllowAllAuth
import dev.kdb.auth.static.staticAuthEngineFromFile
import dev.kdb.server.runSqlWireListen
import dev.kdb.server.sqlWireHostFactory
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncHostFactory
import dev.kdb.server.KdbServerRuntime
import dev.kdb.transport.ws.defaultWebSocketWireTransport
import dev.kdb.schema.KdbSchema
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.nio.file.Path

public fun main(args: Array<String>) {
    val config = ServiceConfig.parse(args)
    val runtime = openRuntime(config)
    val wire = defaultWireCodec()
    val serverRuntime = KdbServerRuntime(runtime)
    val product = config.productConfig
    val auth = product.authConfigPath?.let { staticAuthEngineFromFile(it) } ?: AllowAllAuth
    val peerListenUri = KdbFeatures.peerListenUri(product.features)
    val sqlListenUri = KdbFeatures.sqlListenUri(product.features)
    val peerHostConfig =
        PeerHostConfig(
            namespaceId = config.namespace,
            nodeId = "kdb-service",
            transportHub = "ws",
            materializeCommit = { commit -> materializeCommit(runtime, config.namespace, commit) },
        )
    val peerHostFactory = peerSyncHostFactory(wire, runtime.dag, runtime.storage, peerHostConfig, auth)
    val sqlHostFactory = sqlWireHostFactory(wire, serverRuntime, config.namespace, auth)
    val transportOptions = transportOptionsForProduct(product.tls)
    val transport = defaultWebSocketWireTransport()
    val peerStatus = peerListenUri ?: "disabled"
    val sqlStatus = sqlListenUri ?: "disabled"
    println("KDB service peer=$peerStatus sql=$sqlStatus namespace=${config.namespace}")
    runBlocking {
        if (peerListenUri != null) {
            launch {
                runPeerSyncOverWebSocketListen(
                    transport,
                    peerListenUri,
                    peerHostFactory,
                    transportOptions = transportOptions,
                )
            }
        }
        if (sqlListenUri != null) {
            runSqlWireListen(transport, sqlListenUri, sqlHostFactory, transportOptions = transportOptions)
        } else {
            error("sql wire is disabled and no listener is configured")
        }
    }
}

private fun openRuntime(config: ServiceConfig): EmbeddedKdbRuntime =
    when {
        config.dataDir != null ->
            openFileRuntime(
                dataRoot = config.dataDir,
                catalog = config.namespace.substringBefore('/').ifEmpty { "app" },
                namespaceId = config.namespace,
                schema = config.schema,
            )
        else -> openMemoryRuntimeBlocking("app", config.namespace, config.schema)
    }

internal data class ServiceConfig(
    val dataDir: String?,
    val namespace: String,
    val schema: KdbSchema,
    val productConfig: KdbProductConfig,
) {
    companion object {
        fun parse(args: Array<String>): ServiceConfig {
            var dataDir: String? = null
            var memory = false
            var namespace = "demo/users"
            var configPath: String? = null
            var listenWs: String? = null
            var listenSqlWs: String? = null
            var authConfigPath: String? = null
            var peerSyncEnabled: Boolean? = null
            var sqlWireEnabled: Boolean? = null
            var tlsEnabled: Boolean? = null
            var tlsKeyStore: String? = null
            var tlsTrustStore: String? = null
            var tlsRequireClientAuth: Boolean? = null
            var i = 0
            while (i < args.size) {
                when (args[i]) {
                    "--data-dir" -> dataDir = args.getOrNull(++i)
                    "--memory" -> memory = true
                    "--namespace" -> namespace = args.getOrNull(++i) ?: namespace
                    "--config" -> configPath = args.getOrNull(++i)
                    "--listen-ws" -> listenWs = args.getOrNull(++i)
                    "--listen-sql-ws" -> listenSqlWs = args.getOrNull(++i)
                    "--auth-config" -> authConfigPath = args.getOrNull(++i)
                    "--peer-sync" -> peerSyncEnabled = true
                    "--no-peer-sync" -> peerSyncEnabled = false
                    "--tls" -> tlsEnabled = true
                    "--no-tls" -> tlsEnabled = false
                    "--tls-key-store" -> tlsKeyStore = args.getOrNull(++i)
                    "--tls-trust-store" -> tlsTrustStore = args.getOrNull(++i)
                    "--tls-require-client-auth" -> tlsRequireClientAuth = true
                    "--auth" -> {
                        when (args.getOrNull(++i)) {
                            "none" -> authConfigPath = null
                            else -> error("--auth requires 'none' in v1 (use --auth-config for static auth)")
                        }
                    }
                    else -> error("unknown argument: ${args[i]}")
                }
                i++
            }
            if (dataDir == null && !memory) {
                memory = true
            }
            if (dataDir != null && memory) {
                error("use either --data-dir or --memory")
            }
            val productConfig =
                resolveKdbProductConfig(
                    configFile = configPath?.let { Path.of(it) },
                    dataDirConfig = null,
                    peerSyncEnabledOverride = peerSyncEnabled,
                    sqlWireEnabledOverride = sqlWireEnabled,
                    listenWsOverride = listenWs,
                    listenSqlWsOverride = listenSqlWs,
                    authConfigPathOverride = authConfigPath,
                    tlsEnabledOverride = tlsEnabled,
                    tlsKeyStorePathOverride = tlsKeyStore,
                    tlsTrustStorePathOverride = tlsTrustStore,
                    tlsRequireClientAuthOverride = tlsRequireClientAuth,
                )
            return ServiceConfig(dataDir, namespace, KdbSchema.NONE, productConfig)
        }
    }
}
