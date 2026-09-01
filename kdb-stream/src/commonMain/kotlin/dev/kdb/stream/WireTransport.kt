package dev.kdb.stream

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public interface WireTransport {
    public suspend fun connect(uri: String): WireConnection
}

public interface WireConnection {
    public suspend fun send(frame: ByteArray)
    public fun incoming(): Flow<ByteArray>
    public suspend fun close()
    public fun tryPoll(): ByteArray? = null
}

public class InMemoryWireTransport : WireTransport {
    override suspend fun connect(uri: String): WireConnection {
        val name = uri.removePrefix("memory://")
        return InMemoryWireTransportHub.connect(name)
    }
}

public object InMemoryWireTransportHub {
    private val hubsMutex = Mutex()
    private val hubs = mutableMapOf<String, Hub>()

    public suspend fun hub(name: String): Hub =
        hubsMutex.withLock {
            hubs.getOrPut(name) { Hub(name) }
        }

    internal suspend fun connect(name: String): WireConnection {
        val h = hub(name)
        return h.createConnection()
    }

    /**
     * Hands [frame] to [hubName]'s server handler, as though a client had sent it. The in-process
     * counterpart of a server receiving a frame off a socket, and the entry point a host without
     * its own transport drives this hub through.
     *
     * Public, not `internal`, and it has to be: Kotlin/JS mangles internal names with a per-module
     * salt, and `commonTest` compiles as its own module even though associate-compilation makes
     * internals *visible* to it. A call from a test therefore emitted a mangled name that does not
     * exist in the main module's JS output, and only blew up at runtime -
     * `dispatchToServer_2szq8w_k$ is not a function` on both `js, node` and `js, browser`, while
     * every JVM target passed. [hub] and [serverSend], the other two members a caller outside this
     * file touches, were already public; this one was the odd one out.
     */
    public suspend fun dispatchToServer(hubName: String, frame: ByteArray) {
        hub(hubName).serverHandler?.invoke(frame)
    }

    public class Hub(internal val name: String) {
        private val clientsMutex = Mutex()
        private val clients = mutableListOf<ClientLink>()

        public var serverHandler: (suspend (ByteArray) -> Unit)? = null

        internal suspend fun createConnection(): ClientLink {
            val link = ClientLink(this)
            clientsMutex.withLock { clients += link }
            return link
        }

        public suspend fun serverSend(frame: ByteArray) {
            val snapshot = clientsMutex.withLock { clients.toList() }
            for (c in snapshot) {
                c.deliverFromServer(frame)
            }
        }

        internal class ClientLink(private val hub: Hub) : WireConnection {
            private val serverToClient = Channel<ByteArray>(Channel.UNLIMITED)

            override suspend fun send(frame: ByteArray) {
                dispatchToServer(hub.name, frame)
            }

            override fun incoming(): Flow<ByteArray> = serverToClient.receiveAsFlow()

            override suspend fun close() {
                serverToClient.close()
            }

            internal suspend fun deliverFromServer(frame: ByteArray) {
                serverToClient.send(frame)
            }

            override fun tryPoll(): ByteArray? = serverToClient.tryReceive().getOrNull()
        }
    }
}
