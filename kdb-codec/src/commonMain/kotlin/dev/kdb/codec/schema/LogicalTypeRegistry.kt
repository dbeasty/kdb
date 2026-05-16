package dev.kdb.codec.schema

import dev.kdb.codec.KdbValue

/** Optional custom logical codecs ([Layer 0 spec §7.2]). */
public interface LogicalTypeHandler {
    public fun validate(annotation: LogicalAnnotation.Custom, physical: PhysicalKind)

    public fun encode(value: KdbValue, annotation: LogicalAnnotation.Custom): KdbValue

    public fun decode(value: KdbValue, annotation: LogicalAnnotation.Custom): KdbValue
}

public object LogicalTypeRegistry {
    private val handlers = mutableMapOf<String, LogicalTypeHandler>()

    public fun register(id: String, handler: LogicalTypeHandler) {
        handlers[id] = handler
    }

    internal fun resolve(id: String): LogicalTypeHandler? = handlers[id]
}
