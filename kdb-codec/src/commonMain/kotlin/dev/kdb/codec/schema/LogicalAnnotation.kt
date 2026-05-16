package dev.kdb.codec.schema

/** Built-in logical annotations; extend via [LogicalAnnotation.Custom] + [LogicalTypeRegistry]. */
public sealed class LogicalAnnotation {
    public data object Date : LogicalAnnotation()

    public data object TimeMicros : LogicalAnnotation()

    public data class TimestampMicros(val timezone: String? = null) : LogicalAnnotation()

    public data class TimestampMillis(val timezone: String? = null) : LogicalAnnotation()

    public data object Uuid : LogicalAnnotation()

    public data class Decimal(val precision: Int, val scale: Int) : LogicalAnnotation()

    public data object BigInteger : LogicalAnnotation()

    public data object BigDecimal : LogicalAnnotation()

    public data object Duration : LogicalAnnotation()

    public data class Custom(val id: String, val params: Map<String, String> = emptyMap()) : LogicalAnnotation()
}
