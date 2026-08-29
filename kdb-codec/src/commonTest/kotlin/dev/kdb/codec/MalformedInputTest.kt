package dev.kdb.codec

import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.error.KdbDecodeException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The value decoder is fed bytes that came off the network (a peer's commit payload) and off disk
 * (a delta page, an SSTable block). Both are outside this process's control, so a malformed
 * length field has to end as a [KdbDecodeException] - not as an allocation sized by whatever the
 * input asked for.
 *
 * Mirrors go/kdb/codec/malformed_input_test.go.
 */
class MalformedInputTest {
    /**
     * A nine-byte LEB128 varint for a count in the 2^60 range: small enough to encode in a
     * handful of bytes, large enough that using it as a list capacity is fatal.
     */
    private val hugeLeb =
        byteArrayOf(
            0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0xff.toByte(),
            0xff.toByte(), 0xff.toByte(), 0xff.toByte(), 0x7f,
        )

    @Test
    fun decodeRejectsArrayLengthLargerThanInput() {
        val reg = KdbTypeRegistry.builtin()
        val type = KdbType.Array(KdbType.Primitive(PhysicalKind.INT32))
        assertFailsWith<KdbDecodeException> {
            KdbValue.decodeFromBytes(hugeLeb, type, reg)
        }
    }

    @Test
    fun decodeRejectsMapLengthLargerThanInput() {
        val reg = KdbTypeRegistry.builtin()
        val type =
            KdbType.Map(
                KdbType.Primitive(PhysicalKind.STRING),
                KdbType.Primitive(PhysicalKind.INT32),
            )
        assertFailsWith<KdbDecodeException> {
            KdbValue.decodeFromBytes(hugeLeb, type, reg)
        }
    }

    /**
     * The bound is "no more elements than there are bytes left", so a count just past the
     * remaining input is rejected while a plausible one is still allowed through to fail on its
     * actual contents.
     */
    @Test
    fun decodeArrayLengthBoundIsRemainingInput() {
        val reg = KdbTypeRegistry.builtin()
        val type = KdbType.Array(KdbType.Primitive(PhysicalKind.INT32))
        // One length byte, then three bytes of body: a count of 4 is already more than the three
        // remaining bytes can hold.
        assertFailsWith<KdbDecodeException> {
            KdbValue.decodeFromBytes(byteArrayOf(0x04, 0, 0, 0), type, reg)
        }
        // A count within the remaining bytes gets as far as decoding elements and fails there on
        // running out of input - the bound must not be what rejects a plausible count.
        assertFailsWith<KdbDecodeException> {
            KdbValue.decodeFromBytes(byteArrayOf(0x02, 0, 0, 0), type, reg)
        }
    }

    /** The bound is only allowed to reject input that could not possibly be valid. */
    @Test
    fun decodeAcceptsWellFormedArrayAndMap() {
        val reg = KdbTypeRegistry.builtin()

        val arrType = KdbType.Array(KdbType.Primitive(PhysicalKind.INT32))
        val arr =
            KdbValue.ArrayVal(
                listOf(KdbValue.Int32Val(1), KdbValue.Int32Val(2), KdbValue.Int32Val(3)),
            )
        val decodedArr = KdbValue.decodeFromBytes(arr.encodeToBytes(arrType, reg), arrType, reg)
        assertEquals(3, (decodedArr as KdbValue.ArrayVal).elements.size)

        val mapType =
            KdbType.Map(
                KdbType.Primitive(PhysicalKind.STRING),
                KdbType.Primitive(PhysicalKind.INT32),
            )
        val map =
            KdbValue.MapVal(
                listOf(
                    KdbValue.StringVal("a") to KdbValue.Int32Val(1),
                    KdbValue.StringVal("b") to KdbValue.Int32Val(2),
                ),
            )
        val decodedMap = KdbValue.decodeFromBytes(map.encodeToBytes(mapType, reg), mapType, reg)
        assertEquals(2, (decodedMap as KdbValue.MapVal).entries.size)
    }

    /**
     * A legitimately large array whose elements are each several bytes must still decode; the
     * "count <= remaining bytes" bound is conservative, never tight, and this is the case that
     * would break if someone tightened it to bytes-per-element.
     */
    @Test
    fun decodeAcceptsLargeWellFormedArray() {
        val reg = KdbTypeRegistry.builtin()
        val type = KdbType.Array(KdbType.Primitive(PhysicalKind.INT64))
        val value = KdbValue.ArrayVal((0 until 2000).map { KdbValue.Int64Val(it.toLong()) })
        val decoded = KdbValue.decodeFromBytes(value.encodeToBytes(type, reg), type, reg)
        assertEquals(2000, (decoded as KdbValue.ArrayVal).elements.size)
    }

    @Test
    fun decodeRejectsTruncatedVarint() {
        val reg = KdbTypeRegistry.builtin()
        val type = KdbType.Array(KdbType.Primitive(PhysicalKind.INT32))
        // Continuation bit set on every byte, then the input ends.
        assertFailsWith<KdbDecodeException> {
            KdbValue.decodeFromBytes(
                byteArrayOf(0xff.toByte(), 0xff.toByte(), 0xff.toByte()),
                type,
                reg,
            )
        }
    }
}
