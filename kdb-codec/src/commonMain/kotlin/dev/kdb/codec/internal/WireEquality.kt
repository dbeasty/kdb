package dev.kdb.codec.internal

import dev.kdb.codec.KdbValue

internal fun wireValueEquals(a: KdbValue, b: KdbValue): Boolean {
    when {
        a === b -> return true
        a::class != b::class -> return false
    }
    return when (a) {
        KdbValue.Null -> true
        is KdbValue.Bool -> a.v == (b as KdbValue.Bool).v
        is KdbValue.Int8Val -> a.v == (b as KdbValue.Int8Val).v
        is KdbValue.Int16Val -> a.v == (b as KdbValue.Int16Val).v
        is KdbValue.Int32Val -> a.v == (b as KdbValue.Int32Val).v
        is KdbValue.Int64Val -> a.v == (b as KdbValue.Int64Val).v
        is KdbValue.Float32Val -> a.v == (b as KdbValue.Float32Val).v
        is KdbValue.Float64Val -> a.v == (b as KdbValue.Float64Val).v
        is KdbValue.BytesVal -> a.v.contentEquals((b as KdbValue.BytesVal).v)
        is KdbValue.StringVal -> a.v == (b as KdbValue.StringVal).v
        is KdbValue.ArrayVal -> {
            val o = b as KdbValue.ArrayVal
            a.elements.size == o.elements.size &&
                a.elements.zip(o.elements).all { (x, y) -> wireValueEquals(x, y) }
        }

        is KdbValue.MapVal -> {
            val o = b as KdbValue.MapVal
            a.entries.size == o.entries.size &&
                a.entries.zip(o.entries).all { (x, y) ->
                    wireValueEquals(x.first, y.first) && wireValueEquals(x.second, y.second)
                }
        }

        is KdbValue.RecordVal -> wireRecordFieldsEqual(a.fields, (b as KdbValue.RecordVal).fields)

        is KdbValue.EnumVal -> a.ordinal == (b as KdbValue.EnumVal).ordinal && a.symbol == (b as KdbValue.EnumVal).symbol
        is KdbValue.UnionVal ->
            a.branch == (b as KdbValue.UnionVal).branch && wireValueEquals(a.value, (b as KdbValue.UnionVal).value)

        is KdbValue.FixedVal -> a.v.contentEquals((b as KdbValue.FixedVal).v)
        is KdbValue.DateVal -> a.daysSinceEpoch == (b as KdbValue.DateVal).daysSinceEpoch
        is KdbValue.TimeMicrosVal -> a.microsSinceMidnight == (b as KdbValue.TimeMicrosVal).microsSinceMidnight
        is KdbValue.TimestampVal ->
            a.epochMicros == (b as KdbValue.TimestampVal).epochMicros && a.tz == (b as KdbValue.TimestampVal).tz

        is KdbValue.UuidVal -> a.msb == (b as KdbValue.UuidVal).msb && a.lsb == (b as KdbValue.UuidVal).lsb
        is KdbValue.DecimalVal -> a.scale == (b as KdbValue.DecimalVal).scale && a.unscaled.contentEquals((b as KdbValue.DecimalVal).unscaled)
        is KdbValue.BigIntegerVal -> a.magnitude.contentEquals((b as KdbValue.BigIntegerVal).magnitude)
        is KdbValue.BigDecimalVal ->
            a.scale == (b as KdbValue.BigDecimalVal).scale && a.unscaled.contentEquals((b as KdbValue.BigDecimalVal).unscaled)

        is KdbValue.DurationVal ->
            a.months == (b as KdbValue.DurationVal).months &&
                a.days == (b as KdbValue.DurationVal).days &&
                a.micros == (b as KdbValue.DurationVal).micros
    }
}

private fun wireRecordFieldsEqual(
    a: Map<Int, KdbValue>,
    b: Map<Int, KdbValue>,
): Boolean {
    if (a.size != b.size) return false
    for ((k, v) in a) {
        val o = b[k] ?: return false
        if (!wireValueEquals(v, o)) return false
    }
    return true
}

internal fun omitField(value: KdbValue?, schemaDefault: KdbValue?): Boolean {
    if (value == null && schemaDefault == null) return true
    if (value == null || schemaDefault == null) return false
    return wireValueEquals(value, schemaDefault)
}
