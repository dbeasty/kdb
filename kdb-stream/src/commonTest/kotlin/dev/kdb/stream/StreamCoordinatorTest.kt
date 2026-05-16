package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.indexManager
import dev.kdb.index.memoryIndexStoreFactory
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class StreamCoordinatorTest {
    @Test
    fun publishDeliversDeltaToSubscriber() =
        runTest {
            val ns = "coord-test"
            val wire = defaultWireCodec()
            val transport = InMemoryWireTransport()
            val dag = inMemoryCommitDag(ns)
            val parent = dag.head()
            val child = KdbHash.fromHex("22".repeat(32))
            val coordinator =
                streamCoordinator(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(dag)),
                    dag,
                    InMemoryStorageAdapter(),
                )
            coordinator.start(StreamSessionConfig(ns, "coord", headProvider = { parent }))
            val subscriber =
                streamSubscriber(wire, transport, indexManager(memoryIndexStoreFactory(dag)))
            val conn =
                subscriber.connect(
                    StreamSubscriberConfig(
                        namespaceId = ns,
                        nodeId = "sub",
                        mode = StreamClientMode.READ_ONLY,
                        coordinatorUri = "memory://$ns",
                        resumeFrom = parent,
                    ),
                )
            coordinator.publish(
                PublishedCommit(
                    commitHash = child,
                    parentHash = parent,
                    timestampMicros = 0L,
                ),
            )
            advanceUntilIdle()
            assertEquals(child, conn.position())
        }
}
