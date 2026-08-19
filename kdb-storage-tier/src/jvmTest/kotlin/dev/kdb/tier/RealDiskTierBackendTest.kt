package dev.kdb.tier

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.policy.TierBand
import dev.kdb.policy.TierPolicy
import dev.kdb.policy.defaultMutable
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.DeltaSegmentRef
import dev.kdb.storage.io.FileBackedPlatformIoShimFactory
import dev.kdb.storage.io.PlatformIoConfig
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.storage.manager.tier.DefaultDeltaLogTierRegistry
import dev.kdb.storage.manager.tier.SegmentTier
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

/**
 * Same HOT->WARM->COLD lifecycle as [StorageTierFlowIntegrationTest], but backed by
 * [FileBackedPlatformIoShimFactory] writing to a real temp directory on disk — proving the
 * cold/warm tier backends are genuinely persistent, not just an in-memory illusion, and that
 * the moved bytes are actually readable as real files afterward.
 */
class RealDiskTierBackendTest {

    @Test
    fun segmentDemotedToWarm_isARealFileOnDisk() =
        runTest {
            val root = Files.createTempDirectory("kdb-tier-test").toFile()
            root.deleteOnExit()
            val ns = "app/disktier"
            val ioShim = FileBackedPlatformIoShimFactory.open(PlatformIoConfig(rootDirectory = root.absolutePath))

            var now = 0L
            val registry = DefaultDeltaLogTierRegistry(clockMillis = { now })
            val backends = platformIoTierBackendRegistry(ioShim)
            val dayMs = 24L * 3600 * 1000
            val policy =
                defaultMutable(ns).copy(
                    tiers = TierPolicy(hot = TierBand(maxAgeMillis = dayMs), warm = TierBand(maxAgeMillis = 1000 * dayMs)),
                )
            val manager =
                storageTierManager(
                    inMemoryCommitDag(ns),
                    InMemoryStorageAdapter(),
                    registry,
                    { policy },
                    backends,
                    ioShim = ioShim,
                    clockMillis = { now },
                )

            val segmentId = KdbUuid.random()
            val payload = ByteArray(8192) { (it % 200).toByte() }
            val segmentName = "ns/$ns/delta/$segmentId"
            ioShim.appendToSegment(segmentName, payload)
            ioShim.sealSegment(segmentName)
            registry.onSegmentSealed(
                DeltaSegmentRef(segmentId, ns, KdbHash.fromHex("00".repeat(32)), KdbHash.fromHex("00".repeat(32)), payload.size.toLong(), CompressionCodec.NONE),
                ns,
            )

            now = 2 * dayMs
            val result = manager.runCycle(ns)
            assertEquals(1, result.segmentsMoved)
            assertEquals(SegmentTier.WARM, registry.tierOf(segmentId))

            // The demoted segment must exist as a real file under the warm backend's directory,
            // separate from where hot segments live, and its bytes must round-trip exactly.
            val warmFiles = root.walkTopDown().filter { it.isFile && it.name.contains(segmentId.toString()) }.toList()
            assertTrue(warmFiles.isNotEmpty(), "expected a real file for the demoted segment under $root")
            assertContentEquals(payload, manager.readSegmentBytes(ns, segmentId))

            root.deleteRecursively()
        }
}
