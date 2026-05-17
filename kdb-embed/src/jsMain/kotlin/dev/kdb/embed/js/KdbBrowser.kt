package dev.kdb.embed.js

import dev.kdb.codec.KdbHash
import dev.kdb.embed.EmbedSchemaDto
import dev.kdb.embed.EmbeddedKdbRuntime
import dev.kdb.embed.StreamReconnectPolicy
import dev.kdb.embed.applyRemoteStreamDelta
import dev.kdb.embed.getJson
import dev.kdb.embed.pushCommitsSinceRemoteHead
import dev.kdb.embed.putJson
import dev.kdb.embed.querySql
import dev.kdb.embed.recoverInboundViaPeerSync
import dev.kdb.embed.streamReconnectingJson
import dev.kdb.embed.streamRecoveryCompletedJson
import dev.kdb.embed.streamRecoveryFailedJson
import dev.kdb.embed.streamRecoveryStartedJson
import dev.kdb.embed.syncEmbeddedWithPeer
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.embed.toJsonString
import dev.kdb.embed.toKdbSchema
import dev.kdb.peersync.PeerClientConfig
import dev.kdb.peersync.peerSyncClient
import dev.kdb.schema.KdbSchema
import dev.kdb.stream.StreamClientMode
import dev.kdb.stream.StreamEvent
import dev.kdb.stream.StreamSubscriberConfig
import dev.kdb.stream.streamSubscriber
import dev.kdb.stream.streamUriFromPeerUri
import dev.kdb.transport.ws.JsWebSocketWireTransport
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
    private val nodeId: String = "browser-${kotlin.random.Random.nextInt()}"
    private var peerClient: dev.kdb.peersync.PeerSyncClient? = null
    private var peerSession: dev.kdb.peersync.PeerSession? = null
    private var lastRemoteHead: KdbHash? = null
    private var streamSub: dev.kdb.stream.StreamSubscriber? = null
    private var streamEventsJob: Job? = null
    private var subscribeWanted: Boolean = false
    private var streamConnected: Boolean = false
    private var streamReconnectAttempt: Int = 0

    public fun put(json: String): Promise<String> =
        scope.promise {
            val docId = putJson(runtime, namespaceId, json, schema)
            if (remotePeerUri != null) {
                val session = ensurePeerSession()
                val remoteHead = lastRemoteHead ?: session.remoteHead
                val result = pushCommitsSinceRemoteHead(session, runtime.dag, remoteHead)
                lastRemoteHead = result.localHead
            }
            docId
        }

    public fun get(docId: String): Promise<String> =
        scope.promise {
            getJson(runtime, namespaceId, docId)
        }

    public fun query(sql: String): Promise<String> =
        scope.promise {
            querySql(runtime, namespaceId, sql, schema).toJsonString()
        }

    /** Bidirectional peer-sync catch-up (use when stream subscribe is down). */
    public fun sync(): Promise<String> =
        scope.promise {
            remotePeerUri
                ?: throw IllegalStateException("sync() requires remote mode with peerUri")
            val session = ensurePeerSession()
            val result = syncEmbeddedWithPeer(runtime, session, namespaceId, schema)
            lastRemoteHead = result.finalHead
            streamReconnectAttempt = 0
            """{"appliedCommits":${result.appliedCommits},"pushedCommits":${result.pushedCommits},"head":"${result.finalHead.toHex()}"}"""
        }

    /**
     * Subscribe to server commit notifications over stream mode.
     * On disconnect or error, runs [sync] via peer sync once, then reconnects with backoff.
     */
    public fun subscribe(onEventJson: (String) -> Unit): Promise<Unit> =
        scope.promise {
            remotePeerUri
                ?: throw IllegalStateException("subscribe() requires remote mode with peerUri")
            subscribeWanted = true
            if (streamEventsJob?.isActive == true) return@promise
            startStreamSubscriptionLoop(onEventJson)
        }

    /** Stop stream subscribe without closing the database. */
    public fun unsubscribe(): Promise<Unit> =
        scope.promise {
            subscribeWanted = false
            streamConnected = false
            streamEventsJob?.cancel()
            streamEventsJob = null
            streamSub?.disconnect()
            streamSub = null
        }

    public fun isSubscribeConnected(): Promise<Boolean> = scope.promise { streamConnected }

    public fun close(): Promise<Unit> =
        scope.promise {
            subscribeWanted = false
            streamConnected = false
            streamEventsJob?.cancel()
            streamEventsJob = null
            streamSub?.disconnect()
            streamSub = null
            peerSession = null
            lastRemoteHead = null
            peerClient?.disconnect()
            peerClient = null
        }

    private fun startStreamSubscriptionLoop(onEventJson: (String) -> Unit) {
        streamEventsJob =
            scope.launch {
                runStreamSubscriptionLoop(onEventJson)
            }
    }

    private suspend fun runStreamSubscriptionLoop(onEventJson: (String) -> Unit) {
        val peerUri =
            remotePeerUri
                ?: return
        while (subscribeWanted) {
            streamConnected = false
            val wire = defaultWireCodec()
            val transport = JsWebSocketWireTransport()
            val sub = streamSubscriber(wire, transport, runtime.indexManager)
            streamSub = sub
            var shouldReconnect = false
            try {
                val session = ensurePeerSession()
                val resumeFrom = lastRemoteHead ?: session.remoteHead
                sub.connect(
                    StreamSubscriberConfig(
                        namespaceId = namespaceId,
                        nodeId = "$nodeId-stream",
                        mode = StreamClientMode.READ_ONLY,
                        coordinatorUri = streamUriFromPeerUri(peerUri),
                        resumeFrom = resumeFrom,
                    ),
                )
                sub.events.collect { event ->
                    if (!subscribeWanted) return@collect
                    when (event) {
                        is StreamEvent.Connected -> streamConnected = true
                        is StreamEvent.Disconnected -> streamConnected = false
                        is StreamEvent.Error -> streamConnected = false
                        else -> {}
                    }
                    val json = streamEventToJson(event) ?: return@collect
                    if (event is StreamEvent.DeltaReceived) {
                        val peer = ensurePeerSession()
                        applyRemoteStreamDelta(runtime, peer, namespaceId, event.commitHash, schema)
                        lastRemoteHead = runtime.dag.head()
                    }
                    onEventJson(json)
                    if (event is StreamEvent.Error) {
                        performStreamRecovery(onEventJson, event.throwable.message)
                        shouldReconnect = subscribeWanted
                        return@collect
                    }
                    if (event is StreamEvent.Disconnected) {
                        performStreamRecovery(onEventJson, event.cause?.message ?: "disconnected")
                        shouldReconnect = subscribeWanted
                        return@collect
                    }
                }
                if (subscribeWanted) {
                    streamConnected = false
                    performStreamRecovery(onEventJson, "stream connection closed")
                    shouldReconnect = true
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                if (subscribeWanted) {
                    streamConnected = false
                    performStreamRecovery(onEventJson, e.message)
                    shouldReconnect = true
                }
            } finally {
                streamConnected = false
                streamSub?.disconnect()
                streamSub = null
            }
            if (!subscribeWanted || !shouldReconnect) break
            if (!StreamReconnectPolicy.shouldRetry(streamReconnectAttempt)) {
                onEventJson("""{"type":"ReconnectGaveUp","attempts":$streamReconnectAttempt}""")
                break
            }
            val backoff = StreamReconnectPolicy.backoffMs(streamReconnectAttempt)
            streamReconnectAttempt++
            onEventJson(streamReconnectingJson(streamReconnectAttempt, backoff))
            delay(backoff)
        }
    }

    private suspend fun performStreamRecovery(
        onEventJson: (String) -> Unit,
        reason: String?,
    ) {
        onEventJson(streamRecoveryStartedJson(reason))
        try {
            val session = ensurePeerSession()
            val result = recoverInboundViaPeerSync(runtime, session, namespaceId, schema)
            lastRemoteHead = result.finalHead
            streamReconnectAttempt = 0
            onEventJson(streamRecoveryCompletedJson(result))
        } catch (e: Throwable) {
            onEventJson(streamRecoveryFailedJson(e))
        }
    }

    private suspend fun ensurePeerSession(): dev.kdb.peersync.PeerSession {
        peerSession?.let { return it }
        val uri =
            remotePeerUri
                ?: throw IllegalStateException("peer session requires remote mode with peerUri")
        val wire = defaultWireCodec()
        val transport = JsWebSocketWireTransport()
        val client = peerSyncClient(wire, transport, runtime.dag, runtime.storage)
        peerClient = client
        val session =
            client.connect(
                PeerClientConfig(
                    namespaceId = namespaceId,
                    nodeId = nodeId,
                    peerUri = uri,
                ),
            )
        peerSession = session
        lastRemoteHead = session.remoteHead
        return session
    }

    private fun streamEventToJson(event: StreamEvent): String? =
        when (event) {
            is StreamEvent.Connected ->
                """{"type":"Connected","subscribeConnected":true}"""
            is StreamEvent.DeltaReceived ->
                """{"type":"DeltaReceived","commitHash":"${event.commitHash.toHex()}","hintCount":${event.hintCount}}"""
            is StreamEvent.PositionUpdated ->
                """{"type":"PositionUpdated","commitHash":"${event.commitHash.toHex()}"}"""
            is StreamEvent.Disconnected ->
                """{"type":"Disconnected","subscribeConnected":false}"""
            is StreamEvent.Error ->
                """{"type":"Error","subscribeConnected":false,"message":"${event.throwable.message?.replace("\"", "'") ?: "unknown"}"}"""
            is StreamEvent.CompactionWarning,
            is StreamEvent.IceArchived,
            -> null
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
