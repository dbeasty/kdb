package dev.kdb.stream

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow

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
    private val hubs = mutableMapOf<String, Hub>()

    public fun hub(name: String): Hub =
        synchronized(hubs) {
            hubs.getOrPut(name) { Hub(name) }
        }

    internal suspend fun connect(name: String): WireConnection {
        val h = hub(name)
        return h.createConnection()
    }

    internal suspend fun dispatchToServer(hubName: String, frame: ByteArray) {
        hub(hubName).serverHandler?.invoke(frame)
    }

    public class Hub(internal val name: String) {
        private val clients = mutableListOf<ClientLink>()

        public var serverHandler: (suspend (ByteArray) -> Unit)? = null

        internal fun createConnection(): ClientLink {
            val link = ClientLink(this)
            synchronized(clients) { clients += link }
            return link
        }

        public suspend fun serverSend(frame: ByteArray) {
            val snapshot = synchronized(clients) { clients.toList() }
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
