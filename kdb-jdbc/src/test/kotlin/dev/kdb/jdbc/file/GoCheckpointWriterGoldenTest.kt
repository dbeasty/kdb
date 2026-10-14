package dev.kdb.jdbc.file

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CheckpointCommit
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbBranch
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTag
import dev.kdb.document.kdbSha256
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Kotlin's checkpoint writer against Go's: go/kdb/embed/kotlin_checkpoint_golden_test.go checkpoints
 * the same state with Go's writeCheckpoint, and these are its snapshots - the version 2 record, and
 * the version 6 record and its section. Kotlin must write the very same bytes, and read them back.
 */
class GoCheckpointWriterGoldenTest {
    private val golden: Map<String, Map<String, String>> =
        mapOf(
            "v2" to
                mapOf(
                    "kdb:checkpoint:app/data" to
                        "94080104020000000209086170702f6461746103050700000000000000040a028201010f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f020a00030f00000000000000010000000000000001040540e2cfeeb5400600050f00000000000000020000000000000002060f32ebb1abcc1c601ceb9c4e3c4faba0caa5b85bb98c4f1e6612c40faa528a91c908090462617365090400000000f101010f90aebbef33e0c7a5aeb2d566e1181c97a6abf1934183592b44ea47b77429402c020a02ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc1030f0000000000000003fffffffffffffffd04052746ddeeb5400600050ffffffffffffffffc0000000000000004060f950cfa706e1e39c8d7f57676fe67c41698921c82b35b5d971842d283cd7680f1070e01d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35080910736e617073686f7420726f6f7420c3a9090401000000050a023d0109046d61696e020f90aebbef33e0c7a5aeb2d566e1181c97a6abf1934183592b44ea47b77429402c030540e2cfeeb540060004058084eceeb54006003d01090473696465020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb5400600040540e2cfeeb5400600060a01390109027631020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb54006000409056669727374070a0134010f0000000000001001fffffffffffffff9020f4bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a080a02360105020000000000000002050010000000000000030f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f4a0105070000000000000002056400000000000000030f90aebbef33e0c7a5aeb2d566e1181c97a6abf1934183592b44ea47b77429402c04057b000000000000000505015e8087adc66c18090a01f701f501010a02ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc10209086170702f64617461030f0000000000000003fffffffffffffffd04052746ddeeb5400600050ffffffffffffffffc0000000000000004060a01001c010f0000000000001001fffffffffffffff90209077b2261223a317d070f950cfa706e1e39c8d7f57676fe67c41698921c82b35b5d971842d283cd7680f1080e01d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35090910736e617073686f7420726f6f7420c3a90a050200000000000000",
                ),
            "v6" to
                mapOf(
                    "kdb:checkpoint-section:app/data:ea3cb52d8ddcd46456784a391b43be81" to
                        "4b44424350533031f0000000000000007c010000000000000000000000001001fffffffffffffff94bf5122f344554c53bde2ebb8cd2b7e3d1600ad631c385a5d7cce23c7785459a0000000000001002fffffffffffffff2dbc1b4c900ffe48d575b5da5c638040125f65db0fe3e24494b76ea986457d9860000000000001003ffffffffffffffeb084fed08b978af4d7d196a7446a86b58009e636b611db16211b65a9aadff29c50000000000001004ffffffffffffffe4e52d9c508c502347344d8c07ad91cbd6068afc75ff6292f062a09ca381c89e710000000000001005ffffffffffffffdde77b9a9ae9e30b0dbdb6f510a264ef9de781501d7b6b92ae89eb059c5ab743db17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f32ebb1abcc1c601ceb9c4e3c4faba0caa5b85bb98c4f1e6612c40faa528a91c901000000000000000100000000000000020000000000000002000000000000007bc02cc899010000c8010000000000000400000062617365df8ea8449970bba5b6f9f7fa59ce7d60734ae902ec24633713653f6effdfde953da8e3d2db6e75d782544592ac555787cd6069356ac93667b5de9f96f2bb7abe0300000000000000fdfffffffffffffffcffffffffffffff0400000000000000e8c32cc899010000e70303020100000010000000ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc1d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35dabd3aff769f07eb2965401eb029974ebba3407afd02b26ddb564ea5f8efae72736e617073686f7420726f6f7420c3a92a39ad74",
                    "kdb:checkpoint:app/data" to
                        "98060104060000000209086170702f6461746103050700000000000000040a00050a023d0109046d61696e020fdf8ea8449970bba5b6f9f7fa59ce7d60734ae902ec24633713653f6effdfde95030540e2cfeeb540060004058084eceeb54006003d01090473696465020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb5400600040540e2cfeeb5400600060a01390109027631020f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f030540e2cfeeb54006000409056669727374070a00080a02360105020000000000000002050010000000000000030f17d0b4ba276ae1aadd46cf4c50e9c3ab58d1a7ef2d23fe449db20af14458054f4a0105070000000000000002056400000000000000030fdf8ea8449970bba5b6f9f7fa59ce7d60734ae902ec24633713653f6effdfde9504057b000000000000000505015e8087adc66c18090a019a029802010a02ba5ec51d07a4ac0e951608704431d59a02b21a4e951acc10505a8dc407c501ee684888c0ebb17f374298b65ee2807526c066094c701bcc7ebbe1c1095f494fc10209086170702f64617461030f0000000000000003fffffffffffffffd04052746ddeeb5400600050ffffffffffffffffc0000000000000004060a01001c010f0000000000001001fffffffffffffff90209077b2261223a317d070f3da8e3d2db6e75d782544592ac555787cd6069356ac93667b5de9f96f2bb7abe080e01d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35090910736e617073686f7420726f6f7420c3a90a0e01dabd3aff769f07eb2965401eb029974ebba3407afd02b26ddb564ea5f8efae720a0502000000000000000f0804010203041009406b64623a636865636b706f696e742d73656374696f6e3a6170702f646174613a6561336362353264386464636434363435363738346133393162343362653831110fa235a37fa92db1b451c907150bea0d8d55d4acd0f0f87833b5298d09f127bcae120505000000000000001305f00000000000000014057c01000000000000150101",
                ),
        )

    private val ns = "app/data"

    private fun h(seed: Int) = KdbHash.fromBytes(kdbSha256(byteArrayOf(seed.toByte())))

    private fun hex(b: ByteArray) = b.joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }

    /** go: goldenCheckpointState. */
    private fun state(attach: Boolean): GoCheckpointWriter.State {
        val live = (1..(if (attach) 5 else 1)).associate { KdbUuid(0x1000L + it, it * -7L) to h(it) }
        val tree = DocumentTree.build(live)
        val ts = KdbTimestamp.fromEpochMicros(1_760_000_000_123_456)
        val base = KdbCommit.build(emptyList(), ns, KdbUuid(1, 1), ts, KdbUuid(2, 2), emptyList(), h(40), null, "base")
        val root =
            KdbCommit.build(
                listOf(h(41), h(42)), ns, KdbUuid(3, -3), KdbTimestamp.fromEpochMicros(1_760_000_001_000_999), KdbUuid(-4, 4),
                listOf(KdbOp.Write(KdbUuid(0x1001, -7), "{\"a\":1}")), tree.treeHash, h(50), "snapshot root \u00e9",
                if (attach) h(60) else null,
            )
        return GoCheckpointWriter.State(
            namespaceId = ns,
            throughSequence = 7,
            floorSequence = 2,
            commits = listOf(CheckpointCommit(base, 0), CheckpointCommit(root, 1)),
            branches =
                listOf(
                    KdbBranch("main", ns, root.hash, ts, KdbTimestamp.fromEpochMicros(1_760_000_002_000_000)),
                    KdbBranch("side", ns, base.hash, ts, ts),
                ),
            tags = listOf(KdbTag("v1", ns, base.hash, ts, "first")),
            liveTree = live,
            segments =
                listOf(
                    GoCheckpoint.Fingerprint(2, 4096, base.hash, null, null),
                    GoCheckpoint.Fingerprint(7, 100, root.hash, 123, 1_760_000_003_000_000_001),
                ),
            headCommits = listOf(root.toPayloadBytes()),
            hasAttachments = attach,
            attachmentTables = if (attach) byteArrayOf(1, 2, 3, 4) else ByteArray(0),
        )
    }

    private fun check(version: String, attach: Boolean) =
        runBlocking {
            val shim = InMemoryPlatformIoShim()
            val s = state(attach)
            GoCheckpointWriter.write(shim, s)
            for ((key, want) in golden.getValue(version)) {
                assertEquals(want, hex(shim.readSnapshot(key) ?: error("$version: no snapshot $key")), "$version $key")
            }
            val back = GoCheckpoint.read(shim, ns)!!
            assertEquals(if (attach) 6 else 2, back.version)
            assertEquals(7, back.throughSequence)
            assertEquals(DocumentTree.build(s.liveTree).treeHash, back.liveTree.treeHash)
            assertEquals(s.commits.map { it.commit.hash }, back.commits().map { it.commit.hash }.toList())
            assertEquals(listOf("main", "side"), back.branches.map { it.name })
            assertEquals(123L, back.segments[1].stampSize)
            assertNull(back.segments[0].stampSize)
            if (attach) {
                assertEquals(h(60), back.commits().last().commit.attachmentTreeHash)
                assertTrue(back.attachmentTables!!.contentEquals(byteArrayOf(1, 2, 3, 4)))
            }
        }

    @Test
    fun writesGoVersion2() = check("v2", attach = false)

    @Test
    fun writesGoVersion6WithAttachments() = check("v6", attach = true)

    /** A rewrite drops the section the previous version 6 record named. */
    @Test
    fun aNewCheckpointDeletesThePreviousSection() =
        runBlocking {
            val shim = InMemoryPlatformIoShim()
            GoCheckpointWriter.write(shim, state(true))
            val first = golden.getValue("v6").keys.single { it.startsWith("kdb:checkpoint-section:") }
            val s = state(true)
            GoCheckpointWriter.write(
                shim,
                GoCheckpointWriter.State(
                    s.namespaceId, 8, s.floorSequence, s.commits, s.branches, s.tags, s.liveTree, s.segments, s.headCommits,
                    true, byteArrayOf(9),
                ),
            )
            // Same tree and commits, so the same section: kept.
            assertTrue(shim.readSnapshot(first) != null)
            GoCheckpointWriter.write(shim, state(false))
            assertNull(shim.readSnapshot(first), "the old section outlived a version 2 rewrite")
        }
}
