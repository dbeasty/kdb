package dev.kdb.document.internal

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals

/** The JVM's platform digest must agree with the portable one that JS and native run. */
class Sha256PlatformTest {
    @Test
    fun agreesWithThePortableDigest() {
        val r = Random(5)
        // Every length across several block and padding boundaries, then some long inputs.
        val lengths = (0..300).toList() + listOf(513, 1000, 4096, 65_537)
        for (len in lengths) {
            val m = r.nextBytes(len)
            assertContentEquals(sha256DigestPortable(m), sha256Digest(m), "length $len")
        }
    }

    @Test
    fun onlyChildMidstateAgreesForEveryNibble() {
        val r = Random(6)
        repeat(20) {
            val child = r.nextBytes(32)
            for (nib in 0 until 16) assertContentEquals(sha256OnlyChildPortable(nib, child), sha256OnlyChild(nib, child), "nib $nib")
        }
    }

    @Test
    fun agreesAcrossThreads() {
        val inputs = List(64) { Random(it).nextBytes(it * 37) }
        val want = inputs.map { sha256DigestPortable(it) }
        val threads =
            List(8) {
                Thread {
                    repeat(50) {
                        for ((i, m) in inputs.withIndex()) assertContentEquals(want[i], sha256Digest(m))
                        val child = inputs[it].copyOf(32)
                        for (nib in 0 until 16) assertContentEquals(sha256OnlyChildPortable(nib, child), sha256OnlyChild(nib, child))
                    }
                }
            }
        val failures = java.util.concurrent.ConcurrentLinkedQueue<Throwable>()
        threads.forEach { t -> t.setUncaughtExceptionHandler { _, e -> failures += e } }
        threads.forEach { it.start() }
        threads.forEach { it.join() }
        failures.firstOrNull()?.let { throw it }
    }
}
