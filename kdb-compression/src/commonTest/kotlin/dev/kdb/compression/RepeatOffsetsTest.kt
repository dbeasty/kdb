package dev.kdb.compression

import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [RepeatOffsets] against a model of the decoder written straight from RFC 8878 §3.1.2.5: every
 * Offset_Value the encoder picks must resolve, in the decoder's history, to the offset it meant.
 * A round trip catches most slips in this logic, but not all - a wrong update to the third slot
 * can sit unread for a long time - so this checks every step.
 */
class RepeatOffsetsTest {
    /** The decoder side: resolves [value] after [litLen] literals and updates its history. */
    private class Model {
        val reps = intArrayOf(1, 4, 8)

        fun resolve(litLen: Int, value: Int): Int {
            if (value > 3) {
                val offset = value - 3
                reps[2] = reps[1]; reps[1] = reps[0]; reps[0] = offset
                return offset
            }
            val index = if (litLen == 0) value else value - 1 // 0-based: rep1, rep2, rep3 or rep1 - 1
            val offset = if (index == 3) reps[0] - 1 else reps[index]
            when (index) {
                0 -> {}
                1 -> { reps[1] = reps[0]; reps[0] = offset }
                else -> { reps[2] = reps[1]; reps[1] = reps[0]; reps[0] = offset }
            }
            return offset
        }
    }

    @Test
    fun everyCodeResolvesToTheIntendedOffset() {
        val rnd = Random(5)
        val used = IntArray(4)
        repeat(200) {
            val enc = RepeatOffsets()
            val dec = Model()
            repeat(500) { step ->
                // Mostly offsets the history holds (and their neighbours), so every code is hit.
                val offset =
                    when (rnd.nextInt(4)) {
                        0 -> 1 + rnd.nextInt(20)
                        else -> maxOf(1, dec.reps[rnd.nextInt(3)] + rnd.nextInt(3) - 1)
                    }
                val litLen = if (rnd.nextBoolean()) 0 else 1 + rnd.nextInt(5)
                val value = enc.encode(litLen, offset)
                used[minOf(value, 4) - 1]++
                assertEquals(offset, dec.resolve(litLen, value), "step $step: litLen $litLen offset $offset coded as $value")
                assertEquals(dec.reps.toList(), enc.snapshot().toList(), "step $step: histories diverged")
            }
        }
        assertTrue(used.all { it > 1000 }, "every Offset_Value class is exercised: ${used.toList()}")
    }
}
