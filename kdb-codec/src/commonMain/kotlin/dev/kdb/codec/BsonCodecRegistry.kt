package dev.kdb.codec

import kotlin.reflect.KClass

public interface BsonCodec<T : Any> {
    public fun encode(value: T): BsonValue
    public fun decode(bson: BsonValue): T
}

public object BsonCodecRegistry {
    private val map = HashMap<KClass<*>, BsonCodec<*>>()

    init {
        register(
            KdbUuid::class,
            object : BsonCodec<KdbUuid> {
                override fun encode(value: KdbUuid): BsonValue = value.toBsonBinary()
                override fun decode(bson: BsonValue): KdbUuid = (bson as BsonBinary).toKdbUuid()
            },
        )
        register(
            KdbHash::class,
            object : BsonCodec<KdbHash> {
                override fun encode(value: KdbHash): BsonValue = value.toBsonBinary()
                override fun decode(bson: BsonValue): KdbHash = (bson as BsonBinary).toKdbHash()
            },
        )
        register(
            KdbTimestamp::class,
            object : BsonCodec<KdbTimestamp> {
                override fun encode(value: KdbTimestamp): BsonValue = value.toBsonDate()
                override fun decode(bson: BsonValue): KdbTimestamp = (bson as BsonDateTime).toKdbTimestamp()
            },
        )
    }

    public fun <T : Any> register(kClass: KClass<T>, codec: BsonCodec<T>) {
        @Suppress("UNCHECKED_CAST")
        map[kClass] = codec as BsonCodec<*>
    }

    public fun <T : Any> get(kClass: KClass<T>): BsonCodec<T>? {
        @Suppress("UNCHECKED_CAST")
        return map[kClass] as BsonCodec<T>?
    }

    public fun <T : Any> getOrThrow(kClass: KClass<T>): BsonCodec<T> =
        get(kClass) ?: throw NoSuchElementException("no BSON codec registered for ${kClass.simpleName}")
}

public fun <T : Any> T.toBsonValue(): BsonValue {
    @Suppress("UNCHECKED_CAST")
    val k = this::class as KClass<T>
    return BsonCodecRegistry.getOrThrow(k).encode(this)
}

@Suppress("UNCHECKED_CAST")
public inline fun <reified T : Any> BsonValue.decode(): T =
    BsonCodecRegistry.getOrThrow(T::class).decode(this) as T
