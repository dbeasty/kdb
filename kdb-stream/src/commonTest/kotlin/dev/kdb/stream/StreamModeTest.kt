package dev.kdb.stream

import dev.kdb.codec.KdbHash
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.indexManager
import dev.kdb.index.memoryIndexStoreFactory
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.compaction.CompactionIntent
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class StreamModeTest {
    private val wire = defaultWireCodec()
    private val transport = InMemoryWireTransport()

    @Test
    fun mode1ReceiveDelta() =
        runTest {
            val ns = "app/stream"
            val dag = inMemoryCommitDag(ns)
            val head = dag.head()
            val parent = KdbHash.fromHex("11".repeat(32))
            val child = KdbHash.fromHex("22".repeat(32))
            val coordinator =
                streamCoordinator(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(dag)),
                    dag,
                    InMemoryStorageAdapter(),
                )
            coordinator.start(StreamSessionConfig(ns, "coord", headProvider = { head }))
            val subscriber =
                streamSubscriber(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(dag)),
                )
            val conn =
                subscriber.connect(
                    StreamSubscriberConfig(
                        namespaceId = ns,
                        nodeId = "sub-1",
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

    @Test
    fun outOfOrderDesync() =
        runTest {
            val ns = "app/desync"
            val dag = inMemoryCommitDag(ns)
            val expected = KdbHash.fromHex("aa".repeat(32))
            val coordinator =
                streamCoordinator(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(dag)),
                    dag,
                    InMemoryStorageAdapter(),
                )
            coordinator.start(StreamSessionConfig(ns, "coord", headProvider = { dag.head() }))
            val subscriber =
                streamSubscriber(wire, transport, indexManager(memoryIndexStoreFactory(dag)))
            val conn =
                subscriber.connect(
                    StreamSubscriberConfig(
                        namespaceId = ns,
                        nodeId = "sub",
                        mode = StreamClientMode.READ_ONLY,
                        coordinatorUri = "memory://$ns",
                        resumeFrom = expected,
                    ),
                )
            coordinator.publish(
                PublishedCommit(
                    commitHash = KdbHash.fromHex("cc".repeat(32)),
                    parentHash = KdbHash.fromHex("bb".repeat(32)),
                    timestampMicros = 0L,
                ),
            )
            advanceUntilIdle()
            assertEquals(expected, conn.position())
        }

    @Test
    fun compactionNoticeBoundary() =
        runTest {
            val ns = "app/compact"
            val boundary = KdbHash.fromHex("01".repeat(32))
            val pos = KdbHash.fromHex("00".repeat(32))
            val dag = inMemoryCommitDag(ns)
            val coordinator =
                streamCoordinator(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(dag)),
                    dag,
                    InMemoryStorageAdapter(),
                )
            coordinator.start(StreamSessionConfig(ns, "coord", headProvider = { dag.head() }))
            val subscriber =
                streamSubscriber(wire, transport, indexManager(memoryIndexStoreFactory(dag)))
            val conn =
                subscriber.connect(
                    StreamSubscriberConfig(
                        namespaceId = ns,
                        nodeId = "sub",
                        mode = StreamClientMode.READ_ONLY,
                        coordinatorUri = "memory://$ns",
                        resumeFrom = pos,
                    ),
                )
            val hub = InMemoryWireTransportHub.hub(ns)
            hub.serverSend(
                wire.encode(
                    WireMessage.CompactionNotice(
                        WireHeader(WireMessageType.COMPACTION_NOTICE, 1, 9, 0),
                        CompactionIntent(ns, boundary, 100L),
                    ),
                ),
            )
            advanceUntilIdle()
            assertEquals(pos, conn.position())
        }

    @Test
    fun coordinatorFanOutTwoSubs() =
        runTest {
            val ns = "app/fanout"
            val dag = inMemoryCommitDag(ns)
            val parent = dag.head()
            val child = KdbHash.fromHex("33".repeat(32))
            val coordinator =
                streamCoordinator(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(dag)),
                    dag,
                    InMemoryStorageAdapter(),
                )
            coordinator.start(StreamSessionConfig(ns, "coord", headProvider = { parent }))
            val sub1 = streamSubscriber(wire, transport, indexManager(memoryIndexStoreFactory(dag)))
            val sub2 = streamSubscriber(wire, transport, indexManager(memoryIndexStoreFactory(dag)))
            val c1 =
                sub1.connect(
                    StreamSubscriberConfig(ns, "a", StreamClientMode.READ_ONLY, "memory://$ns", parent),
                )
            val c2 =
                sub2.connect(
                    StreamSubscriberConfig(ns, "b", StreamClientMode.READ_ONLY, "memory://$ns", parent),
                )
            coordinator.publish(PublishedCommit(child, parent, timestampMicros = 0L))
            advanceUntilIdle()
            assertEquals(child, c1.position())
            assertEquals(child, c2.position())
        }

    @Test
    fun writeBackRequiresEngine() {
        runTest {
            val sub =
                streamSubscriber(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(inMemoryCommitDag("ns"))),
                )
            assertFailsWith<IllegalArgumentException> {
                sub.connect(
                    StreamSubscriberConfig(
                        "ns",
                        "n",
                        StreamClientMode.WRITE_BACK,
                        "memory://ns",
                    ),
                )
            }
        }
    }

    @Test
    fun iceNoticeForwarded() =
        runTest {
            val ns = "app/ice"
            val dag = inMemoryCommitDag(ns)
            val coordinator =
                streamCoordinator(
                    wire,
                    transport,
                    indexManager(memoryIndexStoreFactory(dag)),
                    dag,
                    InMemoryStorageAdapter(),
                )
            coordinator.start(StreamSessionConfig(ns, "coord", headProvider = { dag.head() }))
            val subscriber =
                streamSubscriber(wire, transport, indexManager(memoryIndexStoreFactory(dag)))
            val events = mutableListOf<StreamEvent>()
            val job = launch { subscriber.events.collect { events.add(it) } }
            subscriber.connect(
                StreamSubscriberConfig(ns, "sub", StreamClientMode.READ_ONLY, "memory://$ns"),
            )
            val orig = KdbHash.fromHex("44".repeat(32))
            InMemoryWireTransportHub.hub(ns).serverSend(
                wire.encode(
                    WireMessage.IceArchiveNotice(
                        WireHeader(WireMessageType.ICE_ARCHIVE_NOTICE, 1, 1, 0),
                        ns,
                        orig,
                        "ice://b/x",
                        KdbHash.fromHex("55".repeat(32)),
                    ),
                ),
            )
            advanceUntilIdle()
            job.cancel()
            assertTrue(events.any { it is StreamEvent.IceArchived })
        }
}
