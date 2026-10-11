package dev.kdb.storage.delta

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaAuthorshipEnvelope
import dev.kdb.storage.DeltaRecord
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

/**
 * listSegments used to skip any segment scanSegmentRef could not read, so every commit in it
 * vanished from replay without a word - the hole Go's U6c (#121) closed with
 * delta.UnreadableSegmentError. These pin the same semantics here: an unreadable segment is a
 * typed [UnreadableSegmentException] naming it, newest or not, and the one case that is not damage
 * - a segment deleted between the listing and its read - still lists without it.
 */
class UnreadableSegmentTest {
    /** Wraps a shim: reading [failing] throws, as an I/O error would; reading [vanishing] deletes it first. */
    private class FaultyShim(
        private val inner: PlatformIoShim,
    ) : PlatformIoShim {
        var failing: String? = null
        var vanishing: String? = null

        override suspend fun appendToSegment(
            segmentName: String,
            bytes: ByteArray,
        ): Long = inner.appendToSegment(segmentName, bytes)

        override suspend fun readFromSegment(
            segmentName: String,
            offset: Long,
            length: Int,
        ): ByteArray {
            if (segmentName == failing) throw IllegalStateException("simulated I/O error reading $segmentName")
            if (segmentName == vanishing) {
                vanishing = null
                inner.deleteSegment(segmentName)
            }
            return inner.readFromSegment(segmentName, offset, length)
        }

        override suspend fun flushSegment(segmentName: String): Unit = inner.flushSegment(segmentName)

        override suspend fun sealSegment(segmentName: String): Unit = inner.sealSegment(segmentName)

        override suspend fun listSegments(namespaceId: String): List<String> = inner.listSegments(namespaceId)

        override suspend fun deleteSegment(segmentName: String): Unit = inner.deleteSegment(segmentName)

        override suspend fun availableBytes(): Long = inner.availableBytes()

        override suspend fun readSnapshot(key: String): ByteArray? = inner.readSnapshot(key)

        override suspend fun writeSnapshot(
            key: String,
            data: ByteArray,
        ): Unit = inner.writeSnapshot(key, data)

        override suspend fun deleteSnapshot(key: String): Unit = inner.deleteSnapshot(key)
    }

    private val ns = "ns1"

    private fun record(parent: KdbHash?): DeltaRecord {
        val commit =
            KdbCommit.build(
                parentHashes = listOfNotNull(parent),
                namespaceId = ns,
                transactionId = KdbUuid.random(),
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
                operations = listOf(KdbOp.Write(KdbUuid.random(), """{"x":1}""")),
                documentTreeHash = KdbHash.fromHex("cc".repeat(32)),
                schemaHash = null,
            )
        return DeltaRecord(
            commitHash = commit.hash,
            namespaceId = ns,
            authorship = DeltaAuthorshipEnvelope(principal = "p", timestamp = commit.timestamp, rightsToken = "", clientContext = ""),
            commitPayload = commit.toPayloadBytes(),
            documentPatches = emptyList(),
        )
    }

    /** Writes three segments (sequences 0, 1, 2) of two chained commits each. */
    private suspend fun threeSegments(): Pair<FaultyShim, DeltaSegmentFactory> {
        val shim = FaultyShim(InMemoryPlatformIoShim())
        val factory =
            DeltaSegmentFactory(
                StorageEngineConfig(globalMemoryBudgetBytes = 2_000_000, ioShim = shim, compressionCodec = CompressionCodec.NONE),
            )
        var parent: KdbHash? = null
        repeat(3) {
            val writer = factory.openWriter(ns)
            repeat(2) {
                val r = record(parent)
                writer.append(r)
                parent = r.commitHash
            }
            writer.seal()
        }
        return shim to factory
    }

    @Test
    fun anUnreadableSegmentIsATypedErrorNotASilentOmission() =
        runTest {
            for (victim in listOf(1L, 2L)) {
                val (shim, factory) = threeSegments()
                val name = SegmentNameBuilder.deltaSequenced(ns, victim)
                shim.failing = name
                val e = assertFailsWith<UnreadableSegmentException>("segment $victim must not drop out of the listing") {
                    factory.openReader(ns).listSegments()
                }
                assertEquals(ns, e.namespaceId)
                assertEquals(victim, e.sequenceNumber)
                assertEquals(name, e.segmentName)
                assertTrue(e.cause is IllegalStateException, "the cause should stay reachable: ${e.cause}")
                assertTrue(e.message!!.contains("kdb-inspect verify"), e.message)
            }
        }

    @Test
    fun aSegmentDeletedWhileListingIsSkipped() =
        runTest {
            val (shim, factory) = threeSegments()
            shim.vanishing = SegmentNameBuilder.deltaSequenced(ns, 0)
            val refs = factory.openReader(ns).listSegments()
            assertEquals(listOf(1L, 2L), refs.map { it.sequenceNumber })
        }
}
