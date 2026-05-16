package dev.kdb.codec.schema

import dev.kdb.error.KdbSchemaException

/**
 * Registry for named Record, Enum, and Fixed schemas ([Layer 0 spec §7.1]). Immutable after [freeze].
 */
public class KdbTypeRegistry private constructor() {
    /** Null means frozen; then [frozenRecords]/[frozenEnums]/[frozenFixed] hold snapshots. */
    private var mr: MutableMap<String, RecordSchema>? = linkedMapOf()
    private var me: MutableMap<String, EnumSchema>? = linkedMapOf()
    private var mf: MutableMap<String, FixedSchema>? = linkedMapOf()

    private var frozenRecords: Map<String, RecordSchema>? = null
    private var frozenEnums: Map<String, EnumSchema>? = null
    private var frozenFixed: Map<String, FixedSchema>? = null

    public var isFrozen: Boolean = false
        private set

    internal fun snapshotRecords(): Map<String, RecordSchema> = frozenRecords ?: mr!!
    internal fun snapshotEnums(): Map<String, EnumSchema> = frozenEnums ?: me!!
    internal fun snapshotFixed(): Map<String, FixedSchema> = frozenFixed ?: mf!!

    public fun registerRecord(schema: RecordSchema) {
        checkMutable()
        mr!!.put(schema.fullyQualifiedName, schema)
    }

    public fun registerEnum(schema: EnumSchema) {
        checkMutable()
        me!!.put(schema.fullyQualifiedName, schema)
    }

    public fun registerFixed(schema: FixedSchema) {
        checkMutable()
        mf!!.put(schema.fullyQualifiedName, schema)
    }

    public fun freeze() {
        if (isFrozen) return
        validateBeforeFreeze(mr!!, me!!, mf!!)
        frozenRecords = mr!!.toMap(HashMap<String, RecordSchema>())
        frozenEnums = me!!.toMap(HashMap<String, EnumSchema>())
        frozenFixed = mf!!.toMap(HashMap<String, FixedSchema>())
        mr = null
        me = null
        mf = null
        isFrozen = true
    }

    private fun checkMutable() {
        if (isFrozen) throw KdbSchemaException("registry is frozen")
        if (mr == null || me == null || mf == null) throw IllegalStateException("registry mutated after freeze")
    }

    public fun resolveRecord(fqn: String): RecordSchema =
        snapshotRecords()[fqn] ?: throw NoSuchElementException("unknown Record type $fqn")

    public fun resolveEnum(fqn: String): EnumSchema =
        snapshotEnums()[fqn] ?: throw NoSuchElementException("unknown Enum type $fqn")

    public fun resolveFixed(fqn: String): FixedSchema =
        snapshotFixed()[fqn] ?: throw NoSuchElementException("unknown Fixed type $fqn")

    public fun resolve(fqn: String): Any =
        snapshotRecords()[fqn] ?: snapshotEnums()[fqn]
        ?: snapshotFixed()[fqn]
        ?: throw NoSuchElementException("unknown named type $fqn")

    public fun resolveOrNull(fqn: String): Any? =
        snapshotRecords()[fqn] ?: snapshotEnums()[fqn] ?: snapshotFixed()[fqn]

    private fun validateBeforeFreeze(
        records: Map<String, RecordSchema>,
        enums: Map<String, EnumSchema>,
        fixed: Map<String, FixedSchema>,
    ) {
        val fqns = mutableSetOf<String>()
        for (schema in records.values) {
            if (!fqns.add(schema.fullyQualifiedName)) throw KdbSchemaException("duplicate type ${schema.fullyQualifiedName}")
        }
        for (schema in enums.values) {
            if (!fqns.add(schema.fullyQualifiedName)) throw KdbSchemaException("duplicate type ${schema.fullyQualifiedName}")
        }
        for (schema in fixed.values) {
            if (!fqns.add(schema.fullyQualifiedName)) throw KdbSchemaException("duplicate type ${schema.fullyQualifiedName}")
        }

        val rKeys = records.keys.toSet()
        val eKeys = enums.keys.toSet()
        val fKeys = fixed.keys.toSet()
        for (rec in records.values) {
            val seenIds = mutableSetOf<Int>()
            for (fld in rec.fields) {
                if (!seenIds.add(fld.id)) throw KdbSchemaException("duplicate field id ${fld.id} in ${rec.fullyQualifiedName}")
                validateTypeRefs(fld.type, rKeys, eKeys, fKeys)
            }
        }
        for (en in enums.values) {
            if (en.symbols.isEmpty()) throw KdbSchemaException("enum ${en.fullyQualifiedName} requires symbols")
        }
        for (fx in fixed.values) {
            if (fx.size < 1) throw KdbSchemaException("fixed ${fx.fullyQualifiedName} size must be ≥1")
            validateLogicalFixed(fx)
        }
    }

    private fun validateLogicalFixed(fx: FixedSchema) {
        when (fx.logical) {
            LogicalAnnotation.Duration -> require(fx.size == 12) { "duration fixed must be 12 bytes (${fx.fullyQualifiedName})" }
            LogicalAnnotation.Uuid -> require(fx.size == 16) { "uuid fixed must be 16 bytes (${fx.fullyQualifiedName})" }
            else -> Unit
        }
    }

    private fun validateTypeRefs(type: KdbType, rs: Set<String>, es: Set<String>, fs: Set<String>) {
        when (type) {
            is KdbType.Primitive -> validatePrimitiveLogicalCompat(type.physical, type.logical)
            is KdbType.Ref -> {
                val ok = rs.contains(type.fullyQualifiedName) || es.contains(type.fullyQualifiedName) ||
                    fs.contains(type.fullyQualifiedName)
                if (!ok) throw KdbSchemaException("unresolved Ref ${type.fullyQualifiedName}")
            }
            is KdbType.Nullable -> validateTypeRefs(type.inner, rs, es, fs)
            is KdbType.Array -> validateTypeRefs(type.element, rs, es, fs)
            is KdbType.Map -> {
                validateTypeRefs(type.key, rs, es, fs)
                validateTypeRefs(type.value, rs, es, fs)
            }
            is KdbType.Union -> type.branches.forEach { validateTypeRefs(it, rs, es, fs) }
        }
    }

    private fun validatePrimitiveLogicalCompat(kind: PhysicalKind, logical: LogicalAnnotation?) {
        if (logical == null) return
        val ok =
            when (logical) {
                is LogicalAnnotation.Date -> kind == PhysicalKind.INT32
                is LogicalAnnotation.TimeMicros -> kind == PhysicalKind.INT64
                is LogicalAnnotation.TimestampMicros -> kind == PhysicalKind.INT64
                is LogicalAnnotation.TimestampMillis -> kind == PhysicalKind.INT64
                is LogicalAnnotation.Uuid -> kind == PhysicalKind.FIXED
                is LogicalAnnotation.Decimal -> kind == PhysicalKind.BYTES
                is LogicalAnnotation.BigInteger -> kind == PhysicalKind.BYTES
                is LogicalAnnotation.BigDecimal -> kind == PhysicalKind.BYTES
                is LogicalAnnotation.Duration -> kind == PhysicalKind.FIXED
                is LogicalAnnotation.Custom -> true
            }
        if (!ok) throw KdbSchemaException("$logical incompatible with physical $kind")
    }

    public companion object {
        private val builtinLazy: KdbTypeRegistry by lazy {
            val r = KdbTypeRegistry()
            r.freeze()
            r
        }

        /** Empty, frozen registry. */
        public fun builtin(): KdbTypeRegistry = builtinLazy

        public fun create(): KdbTypeRegistry = KdbTypeRegistry()
    }
}
