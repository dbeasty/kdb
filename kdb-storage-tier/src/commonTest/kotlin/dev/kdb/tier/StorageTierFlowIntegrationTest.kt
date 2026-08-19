package dev.kdb.tier

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.error.ArchiveRestoreException
import dev.kdb.policy.NamespacePolicy
import dev.kdb.policy.TierBand
import dev.kdb.policy.TierPolicy
import dev.kdb.policy.defaultMutable
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaSegmentRef
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.storage.manager.tier.DefaultDeltaLogTierRegistry
import dev.kdb.storage.manager.tier.SegmentTier
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

/**
 * Walks a segment through the full HOT -> WARM -> COLD -> ICE lifecycle plus the commit-level
 * ice archive/restore path, proving:
 *  - real bytes move between real backends (not tier-label bookkeeping with no data movement)
 *  - data is byte-identical after every hop, read back from wherever it currently lives
 *  - HOT storage is actually freed once a segment is demoted (no leftover copy)
 *  - policy age bands (not manual calls) drive the HOT->WARM and WARM->COLD demotions
 *  - an archived (ICE) segment is correctly *not* readable without an explicit restore
 *  - the existing commit-level archive/restore path still round-trips a real document
 */
class StorageTierFlowIntegrationTest {

    private class FakeClock(startMillis: Long = 0L) {
        var nowMillis: Long = startMillis
            private set

        fun advanceBy(deltaMillis: Long) {
            nowMillis += deltaMillis
        }

        fun asFn(): () -> Long = { nowMillis }
    }

    @Test
    fun segmentLifecycle_hotWarmColdIce_thenCommitArchiveRestore() =
        runTest {
            val ns = "app/tiered"
            val dag = inMemoryCommitDag(ns)
            val ioShim = InMemoryPlatformIoShim()
            val clock = FakeClock(startMillis = 1_000_000L)
            val registry = DefaultDeltaLogTierRegistry(clockMillis = clock.asFn())
            val backends = platformIoTierBackendRegistry(ioShim)

            val dayMs = 24L * 3600 * 1000
            val policy: NamespacePolicy =
                defaultMutable(ns).copy(
                    tiers =
                        TierPolicy(
                            hot = TierBand(maxAgeMillis = 7 * dayMs),
                            warm = TierBand(maxAgeMillis = 30 * dayMs),
                            cold = TierBand(maxAgeMillis = 365 * dayMs),
                        ),
                )

            val manager =
                storageTierManager(
                    dag,
                    InMemoryStorageAdapter(),
                    registry,
                    policyProvider = { policy },
                    backends = backends,
                    ioShim = ioShim,
                    clockMillis = clock.asFn(),
                )

            // --- Seal a real HOT segment: raw bytes actually written through PlatformIoShim. ---
            val segmentId = KdbUuid.random()
            val payload = ByteArray(4096) { (it % 251).toByte() }
            val segmentName = "ns/$ns/delta/$segmentId"
            ioShim.appendToSegment(segmentName, payload)
            ioShim.sealSegment(segmentName)
            val ref =
                DeltaSegmentRef(
                    segmentId = segmentId,
                    namespaceId = ns,
                    firstCommitHash = KdbHash.fromHex("11".repeat(32)),
                    lastCommitHash = KdbHash.fromHex("22".repeat(32)),
                    sizeBytes = payload.size.toLong(),
                    compressionCodec = CompressionCodec.NONE,
                )
            registry.onSegmentSealed(ref, ns)

            assertEquals(SegmentTier.HOT, registry.tierOf(segmentId))
            assertContentEquals(payload, manager.readSegmentBytes(ns, segmentId))

            // A cycle before anything is old enough must not move anything.
            val tooEarly = manager.runCycle(ns)
            assertEquals(0, tooEarly.segmentsMoved)
            assertEquals(SegmentTier.HOT, registry.tierOf(segmentId))

            // --- Age past the HOT band: policy-driven demotion to WARM. ---
            clock.advanceBy(8 * dayMs)
            val toWarm = manager.runCycle(ns)
            assertEquals(1, toWarm.segmentsMoved)
            assertEquals(0, toWarm.errors.size)
            assertEquals(SegmentTier.WARM, registry.tierOf(segmentId))

            // HOT copy must actually be gone now, not just relabeled.
            assertFailsWith<Throwable> { ioShim.readFromSegment(segmentName, 0, payload.size) }
            // But the data is still byte-identical, now served from WARM.
            assertContentEquals(payload, manager.readSegmentBytes(ns, segmentId))

            // --- Age past the WARM band: policy-driven demotion to COLD. ---
            clock.advanceBy(31 * dayMs)
            val toCold = manager.runCycle(ns)
            assertEquals(1, toCold.segmentsMoved)
            assertEquals(SegmentTier.COLD, registry.tierOf(segmentId))
            assertContentEquals(payload, manager.readSegmentBytes(ns, segmentId))

            // Further cycles are no-ops once nothing else is due.
            val settled = manager.runCycle(ns)
            assertEquals(0, settled.segmentsMoved)

            // --- Explicit move to ICE (archival is deliberate, not age-cycled at segment level). ---
            val toIce = manager.moveSegment(SegmentMoveRequest(ns, segmentId, SegmentTier.ICE))
            assertEquals(payload.size.toLong(), toIce.bytesMoved)
            assertEquals(SegmentTier.ICE, registry.tierOf(segmentId))

            // Archived data is intentionally not transparently readable.
            assertFailsWith<Throwable> { manager.readSegmentBytes(ns, segmentId) }

            // --- Separately: the existing commit-level ice archive/restore path still works. ---
            val head = dag.head()
            val tree = DocumentTree.build(mapOf(KdbUuid.random() to KdbHash.fromHex("ab".repeat(32))))
            dag.putDocumentTree(tree)
            val commit =
                KdbCommit.build(
                    parentHashes = listOf(head),
                    namespaceId = ns,
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"archived":true}""")),
                    documentTreeHash = tree.treeHash,
                    schemaHash = null,
                )
            dag.putCommit(commit)
            val archived = manager.archiveCommit(ArchiveRequest(ns, commit.hash))
            val restored = manager.restoreArchive(RestoreRequest(archived.bundleLocation, "app/tiered-restored"))
            assertEquals(1, restored.documentsImported)
        }

    @Test
    fun restoreArchive_rejectsCorruptBundle_realIoBackend() =
        runTest {
            val ioShim = InMemoryPlatformIoShim()
            val backends = platformIoTierBackendRegistry(ioShim)
            val backend = backends.get("default-ice")
            val loc = backend.put("bad", byteArrayOf(1, 2, 3))
            val manager =
                storageTierManager(
                    inMemoryCommitDag("x"),
                    InMemoryStorageAdapter(),
                    DefaultDeltaLogTierRegistry(),
                    { defaultMutable("x") },
                    backends,
                    ioShim = ioShim,
                )
            assertFailsWith<ArchiveRestoreException> {
                manager.restoreArchive(RestoreRequest(loc, "app/bad"))
            }
        }

    @Test
    fun moveSegment_sameSourceAndTarget_isNoop() =
        runTest {
            val ns = "app/noop"
            val ioShim = InMemoryPlatformIoShim()
            val registry = DefaultDeltaLogTierRegistry()
            val manager =
                storageTierManager(
                    inMemoryCommitDag(ns),
                    InMemoryStorageAdapter(),
                    registry,
                    { defaultMutable(ns) },
                    ioShim = ioShim,
                )
            val segmentId = KdbUuid.random()
            val segmentName = "ns/$ns/delta/$segmentId"
            ioShim.appendToSegment(segmentName, byteArrayOf(1, 2, 3))
            registry.onSegmentSealed(
                DeltaSegmentRef(segmentId, ns, KdbHash.fromHex("00".repeat(32)), KdbHash.fromHex("00".repeat(32)), 3L, CompressionCodec.NONE),
                ns,
            )
            val result = manager.moveSegment(SegmentMoveRequest(ns, segmentId, SegmentTier.HOT))
            assertEquals(0L, result.bytesMoved)
            assertTrue(result.sourcePath == null && result.destPath == null)
        }
}
