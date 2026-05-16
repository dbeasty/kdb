package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.codec.KdbValue
import dev.kdb.codec.toKdbUuid
import dev.kdb.codec.toUuidVal

public fun IndexHint.toKdbValue(): KdbValue =
    KdbValue.RecordVal(
        mapOf(
            1 to indexId.toUuidVal(),
            2 to KdbValue.StringVal(fieldName),
            3 to KdbValue.Int64Val(type.ordinal.toLong()),
            4 to KdbValue.Int64Val(action.ordinal.toLong()),
            5 to docId.toUuidVal(),
            6 to KdbValue.FixedVal(commitHash.bytes.copyOf()),
            7 to key.toWireUnion(),
        ),
    )

private fun IndexKey?.toWireUnion(): KdbValue =
    when (this) {
        null -> KdbValue.Null
        IndexKey.NullKey ->
            KdbValue.UnionVal(0, KdbValue.RecordVal(emptyMap()))

        is IndexKey.BoolKey ->
            KdbValue.UnionVal(1, KdbValue.RecordVal(mapOf(1 to KdbValue.Bool(value))))

        is IndexKey.Int32Key ->
            KdbValue.UnionVal(2, KdbValue.RecordVal(mapOf(1 to KdbValue.Int64Val(value.toLong()))))

        is IndexKey.Int64Key ->
            KdbValue.UnionVal(3, KdbValue.RecordVal(mapOf(1 to KdbValue.Int64Val(value))))

        is IndexKey.Float64Key ->
            KdbValue.UnionVal(4, KdbValue.RecordVal(mapOf(1 to KdbValue.Float64Val(value))))

        is IndexKey.TimestampKey ->
            KdbValue.UnionVal(5, KdbValue.RecordVal(mapOf(1 to KdbValue.Int64Val(epochMillis))))

        is IndexKey.StringKey ->
            KdbValue.UnionVal(6, KdbValue.RecordVal(mapOf(1 to KdbValue.StringVal(value))))

        is IndexKey.UuidKey ->
            KdbValue.UnionVal(7, KdbValue.RecordVal(mapOf(1 to id.toUuidVal())))

        is IndexKey.VectorKey ->
            throw UnsupportedOperationException("vector IndexHint serialization not wired")

        is IndexKey.CompositeKey ->
            throw UnsupportedOperationException("composite IndexHint serialization not wired")
    }


public fun IndexHint.Companion.fromKdbValue(value: KdbValue): IndexHint {
    val rec =
        value as? KdbValue.RecordVal
            ?: throw IllegalArgumentException("IndexHint expects record")
    fun reqUuid(idx: Int): KdbUuid {
        val v = rec.fields[idx] as? KdbValue.UuidVal
            ?: throw IllegalArgumentException("missing uuid $idx")
        return v.toKdbUuid()
    }

    fun reqString(idx: Int): String {
        val v = rec.fields[idx] as? KdbValue.StringVal
            ?: throw IllegalArgumentException("missing string $idx")
        return v.v
    }

    fun reqFixed(idx: Int): KdbHash {
        val v = rec.fields[idx] as? KdbValue.FixedVal
            ?: throw IllegalArgumentException("missing fixed $idx")
        return KdbHash.fromBytes(v.v.copyOf())
    }

    val hintIndexId = reqUuid(1)
    val fieldName = reqString(2)
    val typeOrd =
        ((rec.fields[3] as? KdbValue.Int64Val)?.v)
            ?: throw IllegalArgumentException("missing type ordinal")
    val actionOrd =
        ((rec.fields[4] as? KdbValue.Int64Val)?.v)
            ?: throw IllegalArgumentException("missing action ordinal")

    val docId = reqUuid(5)
    val commitHash = reqFixed(6)
    val keyVal = rec.fields[7]

    val keyParsed =
        when (keyVal) {
            null, KdbValue.Null -> null
            is KdbValue.UnionVal -> keyVal.decodeTyped()
            else -> throw IllegalArgumentException("unsupported key envelope")
        }

    return IndexHint(
        hintIndexId,
        fieldName,
        IndexType.entries[typeOrd.toInt()],
        IndexHintAction.entries[actionOrd.toInt()],
        docId,
        keyParsed,
        commitHash,
    )
}

private fun KdbValue.UnionVal.decodeTyped(): IndexKey {
    val r = value as? KdbValue.RecordVal ?: throw IllegalArgumentException("key union expects record payload")
    return when (branch) {
        0 -> IndexKey.NullKey
        1 -> IndexKey.BoolKey((r.fields[1] as? KdbValue.Bool)?.v ?: error("bool missing"))
        2 ->
            IndexKey.Int32Key(
                ((r.fields[1] as? KdbValue.Int64Val)?.v ?: error("i32 missing")).toInt(),
            )

        3 -> IndexKey.Int64Key((r.fields[1] as? KdbValue.Int64Val)?.v ?: error("i64 missing"))
        4 -> IndexKey.Float64Key((r.fields[1] as? KdbValue.Float64Val)?.v ?: error("f64 missing"))
        5 -> IndexKey.TimestampKey((r.fields[1] as? KdbValue.Int64Val)?.v ?: error("ts missing"))
        6 -> IndexKey.StringKey((r.fields[1] as? KdbValue.StringVal)?.v ?: error("string missing"))

        7 -> {
            val uuidVal =
                r.fields[1] as? KdbValue.UuidVal
                    ?: error("uuid missing")
            IndexKey.UuidKey(uuidVal.toKdbUuid())
        }

        else -> IndexKey.NullKey
    }
}
