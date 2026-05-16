package dev.kdb.codec

import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.codec.schema.LogicalAnnotation
import kotlin.reflect.KClass

public interface KdbCodec<T : Any> {
    public val schema: KdbType

    public fun encode(value: T): KdbValue

    public fun decode(value: KdbValue): T
}

public object KdbCodecRegistry {
    private val map = HashMap<KClass<*>, KdbCodec<*>>()

    init {
        register(
            KdbUuid::class,
            object : KdbCodec<KdbUuid> {
                override val schema: KdbType =
                    KdbType.Primitive(PhysicalKind.FIXED, LogicalAnnotation.Uuid)

                override fun encode(value: KdbUuid): KdbValue = value.toUuidVal()

                override fun decode(value: KdbValue): KdbUuid = (value as KdbValue.UuidVal).toKdbUuid()
            },
        )
        register(
            KdbHash::class,
            object : KdbCodec<KdbHash> {
                override val schema: KdbType = KdbType.Primitive(PhysicalKind.BYTES)

                override fun encode(value: KdbHash): KdbValue = KdbValue.BytesVal(value.bytes.copyOf())

                override fun decode(value: KdbValue): KdbHash {
                    val b = (value as KdbValue.BytesVal).v
                    return KdbHash.fromBytes(b)
                }
            },
        )
        register(
            KdbTimestamp::class,
            object : KdbCodec<KdbTimestamp> {
                override val schema: KdbType =
                    KdbType.Primitive(PhysicalKind.INT64, LogicalAnnotation.TimestampMicros(null))

                override fun encode(value: KdbTimestamp): KdbValue = value.toTimestampVal()

                override fun decode(value: KdbValue): KdbTimestamp = (value as KdbValue.TimestampVal).toKdbTimestamp()
            },
        )
    }

    public fun <T : Any> register(kClass: KClass<T>, codec: KdbCodec<T>) {
        @Suppress("UNCHECKED_CAST")
        map[kClass] = codec as KdbCodec<*>
    }

    public fun <T : Any> get(kClass: KClass<T>): KdbCodec<T>? {
        @Suppress("UNCHECKED_CAST")
        return map[kClass] as KdbCodec<T>?
    }

    public fun <T : Any> getOrThrow(kClass: KClass<T>): KdbCodec<T> =
        get(kClass) ?: throw NoSuchElementException("no KdbCodec registered for ${kClass.simpleName}")
}

public fun <T : Any> T.toKdbValue(): KdbValue {
    @Suppress("UNCHECKED_CAST")
    val k = this::class as KClass<T>
    return KdbCodecRegistry.getOrThrow(k).encode(this)
}

@Suppress("UNCHECKED_CAST")
public inline fun <reified T : Any> KdbValue.decode(): T = KdbCodecRegistry.getOrThrow(T::class).decode(this) as T
