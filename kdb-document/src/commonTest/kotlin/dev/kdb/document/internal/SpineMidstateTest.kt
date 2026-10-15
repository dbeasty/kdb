package dev.kdb.document.internal

import dev.kdb.document.kdbTrieFoldLeaf
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals

/**
 * The portable midstate path (JS and native) and the shared fold give the full preimage's hash,
 * so every Kotlin trie keeps Go's hashes.
 */
class SpineMidstateTest {
    private fun fullPreimage(
        nib: Int,
        child: ByteArray,
    ): ByteArray {
        val buf = ByteArray(1 + 16 * 32)
        buf[0] = 0x01
        child.copyInto(buf, 1 + nib * 32)
        return sha256DigestPortable(buf)
    }

    @Test
    fun midstateMatchesTheFullPreimageForEveryNibble() {
        val r = Random(1)
        repeat(16 * 20) { i ->
            val child = r.nextBytes(32)
            val nib = i % 16
            val want = fullPreimage(nib, child)
            assertContentEquals(want, sha256OnlyChildMidstate(nib, child), "midstate, nib $nib")
            assertContentEquals(want, sha256OnlyChild(nib, child), "platform, nib $nib")
        }
    }

    @Test
    fun sharedFoldMatchesTheReferenceFromEveryDepth() {
        val r = Random(2)
        repeat(33 * 2) { i ->
            val id = r.nextBytes(16)
            val content = r.nextBytes(32)
            val depth = i % 33
            val leaf = ByteArray(49)
            id.copyInto(leaf, 1)
            content.copyInto(leaf, 17)
            var want = sha256DigestPortable(leaf)
            for (d in 31 downTo depth) {
                val b = id[d shr 1].toInt() and 0xff
                want = fullPreimage(if (d and 1 == 1) b and 0x0f else b shr 4, want)
            }
            assertContentEquals(want, kdbTrieFoldLeaf(id, content, depth), "depth $depth")
        }
    }

    /** The resumable portable digest at every length around the block boundaries. */
    @Test
    fun portableDigestAgreesWithThePlatformAtEveryLength() {
        assertContentEquals(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".chunked(2).map { it.toInt(16).toByte() }.toByteArray(),
            sha256DigestPortable("abc".encodeToByteArray()),
        )
        for (n in 0..200) {
            val m = Random(n).nextBytes(n)
            assertContentEquals(sha256Digest(m), sha256DigestPortable(m), "length $n")
        }
    }
}
