package dev.kdb.service

import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.materializeCommit
import dev.kdb.embed.openMemoryRuntimeBlocking
import dev.kdb.embed.runPeerSyncOverWebSocketListen
import dev.kdb.auth.AllowAllAuth
import dev.kdb.auth.static.staticAuthEngineFromFile
import dev.kdb.server.runSqlWireListen
import dev.kdb.server.sqlWireHostFactory
import dev.kdb.jdbc.file.openFileRuntime
import dev.kdb.peersync.peerSyncHost
import dev.kdb.server.KdbServerRuntime
import dev.kdb.transport.ws.defaultWebSocketWireTransport
import dev.kdb.schema.KdbSchema
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

public fun main(args: Array<String>) {
    val config = ServiceConfig.parse(args)
    val runtime = openRuntime(config)
    val wire = defaultWireCodec()
    val peerHost = peerSyncHost(wire, runtime.dag, runtime.storage)
    val serverRuntime = KdbServerRuntime(runtime)
    val auth = config.authConfigPath?.let { staticAuthEngineFromFile(it) } ?: AllowAllAuth
    val sqlHostFactory = sqlWireHostFactory(wire, serverRuntime, config.namespace, auth)
    val peerUri = config.listenWs ?: "kdb-ws://0.0.0.0:7443/kdb?bind=true"
    val sqlUri = config.listenSqlWs ?: "kdb-ws://0.0.0.0:7444/kdb?bind=true"
    println("KDB service peer=$peerUri sql=$sqlUri namespace=${config.namespace}")
    runBlocking {
        peerHost.start(
            dev.kdb.peersync.PeerHostConfig(
                namespaceId = config.namespace,
                nodeId = "kdb-service",
                transportHub = "ws",
                materializeCommit = { commit -> materializeCommit(runtime, config.namespace, commit) },
            ),
        )
        launch {
            runPeerSyncOverWebSocketListen(defaultWebSocketWireTransport(), peerUri, peerHost)
        }
        runSqlWireListen(defaultWebSocketWireTransport(), sqlUri, sqlHostFactory)
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
    val listenWs: String?,
    val listenSqlWs: String?,
    val schema: KdbSchema,
    val authConfigPath: String?,
) {
    companion object {
        fun parse(args: Array<String>): ServiceConfig {
            var dataDir: String? = null
            var memory = false
            var namespace = "demo/users"
            var listenWs: String? = null
            var listenSqlWs: String? = null
            var authConfigPath: String? = null
            var i = 0
            while (i < args.size) {
                when (args[i]) {
                    "--data-dir" -> dataDir = args.getOrNull(++i)
                    "--memory" -> memory = true
                    "--namespace" -> namespace = args.getOrNull(++i) ?: namespace
                    "--listen-ws" -> listenWs = args.getOrNull(++i)
                    "--listen-sql-ws" -> listenSqlWs = args.getOrNull(++i)
                    "--auth-config" -> authConfigPath = args.getOrNull(++i)
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
            return ServiceConfig(dataDir, namespace, listenWs, listenSqlWs, KdbSchema.NONE, authConfigPath)
        }
    }
}
