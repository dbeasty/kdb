package dev.kdb.recovery

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.compression.Crc32
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.integrity.scanVerifiedCommits
import dev.kdb.storage.CompressionCodec
import dev.kdb.storage.PlatformIoShim
import dev.kdb.storage.io.SegmentNameBuilder
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class RestoreTest {
    @Test
    fun restoreFromBackupOnlyRebuildsCleanly() =
        runTest {
            val ns = "ns1"
            val backup = newDirShim()
            val c0 = buildCommit(ns, null)
            val c1 = buildCommit(ns, c0.hash)
            appendSegment(backup, ns, 0, rawFrame(c0), rawFrame(c1))

            val out = newDirShim()
            val result = hybridRestore(listOf(Source("backup", backup)), ns, CompressionCodec.NONE, out)
            assertEquals(2, result.appliedCount)
            assertTrue(result.missingHashes.isEmpty())

            val restored = scanVerifiedCommits(out, ns, CompressionCodec.NONE)
            assertEquals(2, restored.size)
            assertTrue(restored.containsKey(c0.hash.toHex()))
            assertTrue(restored.containsKey(c1.hash.toHex()))
        }

    @Test
    fun hybridRestoreLocalAheadOfBackup() =
        runTest {
            val ns = "ns1"
            val c0 = buildCommit(ns, null)
            val c1 = buildCommit(ns, c0.hash)
            val c2 = buildCommit(ns, c1.hash)
            val c3 = buildCommit(ns, c2.hash) // will be torn in the local copy

            val backup = newDirShim()
            appendSegment(backup, ns, 0, rawFrame(c0), rawFrame(c1)) // stale: missing c2, c3

            val local = newDirShim()
            val torn = rawFrame(c3).copyOfRange(0, 8)
            appendSegment(local, ns, 0, rawFrame(c0), rawFrame(c1), rawFrame(c2), torn)

            val out = newDirShim()
            val result = hybridRestore(listOf(Source("local", local), Source("backup", backup)), ns, CompressionCodec.NONE, out)
            assertEquals(3, result.appliedCount, "expected 3 applied commits (c0,c1,c2), got $result")
            assertTrue(result.missingHashes.isEmpty())

            val restored = scanVerifiedCommits(out, ns, CompressionCodec.NONE)
            for (h in listOf(c0.hash, c1.hash, c2.hash)) {
                assertTrue(restored.containsKey(h.toHex()), "expected restored log to contain ${h.toHex()}")
            }
            assertFalse(restored.containsKey(c3.hash.toHex()), "restored log must not contain the torn, unverified commit c3")
        }

    @Test
    fun hybridRestoreFillsGapFromPeer() =
        runTest {
            val ns = "ns1"
            val c0 = buildCommit(ns, null)
            val c1 = buildCommit(ns, c0.hash) // missing from both local and backup
            val c2 = buildCommit(ns, c1.hash)

            val local = newDirShim()
            appendSegment(local, ns, 0, rawFrame(c0))
            appendSegment(local, ns, 1, rawFrame(c2)) // c2's parent c1 is absent

            val withoutPeer = hybridRestore(listOf(Source("local", local)), ns, CompressionCodec.NONE, newDirShim())
            assertTrue(withoutPeer.missingHashes.isNotEmpty(), "expected missing hashes without the peer source, got $withoutPeer")

            val peer = newDirShim()
            appendSegment(peer, ns, 0, rawFrame(c1))

            val out = newDirShim()
            val result = hybridRestore(listOf(Source("local", local), Source("peer", peer)), ns, CompressionCodec.NONE, out)
            assertEquals(3, result.appliedCount)
            assertTrue(result.missingHashes.isEmpty(), "expected the peer source to fill the gap, got $result")
        }

    @Test
    fun restoreNeverTrustsUnverifiedLocalFrames() =
        runTest {
            val ns = "ns1"
            val c0 = buildCommit(ns, null)
            val c1 = buildCommit(ns, c0.hash)

            val local = newDirShim()
            val corrupt = rawFrame(c1).copyOf()
            corrupt[19] = (corrupt[19].toInt() xor 0xFF).toByte() // CRC mismatch, not truncation
            appendSegment(local, ns, 0, rawFrame(c0), corrupt)

            val out = newDirShim()
            val result = hybridRestore(listOf(Source("local", local)), ns, CompressionCodec.NONE, out)
            assertEquals(1, result.appliedCount, "expected only the verified commit c0 to be restored, got $result")

            val restored = scanVerifiedCommits(out, ns, CompressionCodec.NONE)
            assertFalse(restored.containsKey(c1.hash.toHex()), "restore must never apply a commit whose frame failed CRC verification")
        }
}

private fun newDirShim(): PlatformIoShim = InMemoryPlatformIoShim()

private fun buildCommit(ns: String, parent: KdbHash?): KdbCommit =
    KdbCommit.build(
        parentHashes = if (parent != null) listOf(parent) else emptyList(),
        namespaceId = ns,
        transactionId = KdbUuid.random(),
        timestamp = KdbTimestamp.now(),
        authorNodeId = KdbUuid.random(),
        operations = listOf(KdbOp.Write(KdbUuid.random(), "{}")),
        documentTreeHash = DocumentTree.EMPTY.treeHash,
        schemaHash = null,
        message = "test",
    )

private fun rawFrame(c: KdbCommit): ByteArray {
    val payload = c.toPayloadBytes()
    val out = ByteArray(16 + payload.size)
    out[0] = 0x4B; out[1] = 0x44; out[2] = 0x42; out[3] = 0x50
    writeIntBe(out, 4, payload.size)
    writeIntBe(out, 8, payload.size)
    writeIntBe(out, 12, Crc32.of(payload))
    payload.copyInto(out, 16)
    return out
}

private fun writeIntBe(a: ByteArray, o: Int, v: Int) {
    a[o] = (v ushr 24).toByte(); a[o + 1] = (v ushr 16).toByte()
    a[o + 2] = (v ushr 8).toByte(); a[o + 3] = v.toByte()
}

private suspend fun appendSegment(shim: PlatformIoShim, ns: String, seq: Long, vararg frames: ByteArray) {
    val name = SegmentNameBuilder.deltaSequenced(ns, seq)
    for (f in frames) shim.appendToSegment(name, f)
}
