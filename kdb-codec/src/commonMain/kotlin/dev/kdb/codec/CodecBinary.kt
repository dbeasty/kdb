package dev.kdb.codec

import dev.kdb.codec.internal.WireCodec
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import kotlinx.io.Sink

/** @see dev.kdb.codec docs Layer 0 §8.1 */
public fun KdbValue.encodeToBytes(type: KdbType, registry: KdbTypeRegistry): ByteArray =
    WireCodec.encode(this, type, registry)

public fun KdbValue.encodeTo(sink: Sink, type: KdbType, registry: KdbTypeRegistry) {
    val blob = encodeToBytes(type, registry)
    sink.write(blob, 0, blob.size)
}

public fun KdbValue.encodedSize(type: KdbType, registry: KdbTypeRegistry): Int =
    WireCodec.encodedSize(this, type, registry)
