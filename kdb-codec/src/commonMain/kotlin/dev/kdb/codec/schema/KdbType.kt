package dev.kdb.codec.schema

/**
 * Immutable type expression ([Layer 0 spec §5][dev.kdb.codec.docs]).
 */
public sealed class KdbType {
    public data class Primitive(
        val physical: PhysicalKind,
        val logical: LogicalAnnotation? = null,
    ) : KdbType()

    public data class Ref(val fullyQualifiedName: String) : KdbType()

    public data class Nullable(val inner: KdbType) : KdbType()

    public data class Array(val element: KdbType) : KdbType()

    public data class Map(val key: KdbType, val value: KdbType) : KdbType()

    public data class Union(val branches: List<KdbType>) : KdbType() {
        init {
            require(branches.size >= 2) { "Union requires ≥2 branches" }
        }
    }
}
