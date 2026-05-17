package dev.kdb.embed.js

import dev.kdb.embed.EmbedSchemaDto
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.getJson
import dev.kdb.embed.materializeCommitHistory
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.embed.putJson
import dev.kdb.embed.querySql
import dev.kdb.embed.syncEmbedSchema
import dev.kdb.embed.toJsonString
import dev.kdb.embed.toKdbSchema
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone
import dev.kdb.transport.ws.JsWebSocketWireTransport
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.promise
import kotlinx.serialization.json.Json
import kotlin.js.Promise

@JsExport
public class KdbBrowserHandle internal constructor(
    private val scope: CoroutineScope,
    internal val runtime: EmbeddedKdbRuntime,
    private val namespaceId: String,
    private val schema: KdbSchema,
    private val remotePeerUri: String?,
) {
    private var peerClient: dev.kdb.peersync.PeerSyncClient? = null
    private var peerSession: dev.kdb.peersync.PeerSession? = null

    public fun put(json: String): Promise<String> =
        scope.promise {
            putJson(runtime, namespaceId, json, schema)
        }

    public fun get(docId: String): Promise<String> =
        scope.promise {
            getJson(runtime, namespaceId, docId)
        }

    public fun query(sql: String): Promise<String> =
        scope.promise {
            querySql(runtime, namespaceId, sql, schema).toJsonString()
        }

    public fun sync(): Promise<String> =
        scope.promise {
            val uri =
                remotePeerUri
                    ?: throw IllegalStateException("sync() requires remote mode with peerUri")
            val wire = defaultWireCodec()
            val transport = JsWebSocketWireTransport()
            val client = peerSyncClient(wire, transport, runtime.dag, runtime.storage)
            peerClient = client
            val session =
                client.connect(
                    PeerClientConfig(
                        namespaceId = namespaceId,
                        nodeId = "browser-${kotlin.random.Random.nextInt()}",
                        peerUri = uri,
                    ),
                )
            peerSession = session
            val result = session.pullMissing()
            materializeCommitHistory(runtime, namespaceId, schema)
            if (!schema.isNone) {
                syncEmbedSchema(runtime, namespaceId, schema)
            }
            """{"appliedCommits":${result.appliedCommits},"pushedCommits":${result.pushedCommits},"head":"${result.finalHead.toHex()}"}"""
        }

    public fun close(): Promise<Unit> =
        scope.promise {
            peerSession = null
            peerClient?.disconnect()
            peerClient = null
        }
}

@JsExport
public object KdbBrowser {
    public fun open(namespace: String): Promise<KdbBrowserHandle> = openWithOptions(namespace, null, null, null)

    public fun openWithSchema(
        namespace: String,
        schemaJson: String,
    ): Promise<KdbBrowserHandle> = openWithOptions(namespace, schemaJson, null, null)

    public fun openRemote(
        namespace: String,
        peerUri: String,
        schemaJson: String? = null,
    ): Promise<KdbBrowserHandle> = openWithOptions(namespace, schemaJson, "remote", peerUri)

    private fun openWithOptions(
        namespace: String,
        schemaJson: String?,
        mode: String?,
        peerUri: String?,
    ): Promise<KdbBrowserHandle> {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        return scope.promise {
            val schema = parseSchemaJson(schemaJson)
            val catalog = namespace.substringBefore('/').ifEmpty { "app" }
            val runtime = openMemoryRuntime(catalog, namespace, schema)
            KdbBrowserHandle(
                scope = scope,
                runtime = runtime,
                namespaceId = namespace,
                schema = schema,
                remotePeerUri = if (mode == "remote") peerUri else null,
            )
        }
    }
}

private fun parseSchemaJson(schemaJson: String?): KdbSchema {
    val raw = schemaJson?.takeIf { it.isNotBlank() } ?: return KdbSchema.NONE
    return Json.decodeFromString(EmbedSchemaDto.serializer(), raw).toKdbSchema()
}
