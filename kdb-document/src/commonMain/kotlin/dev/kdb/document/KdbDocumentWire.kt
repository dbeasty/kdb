package dev.kdb.document

import dev.kdb.codec.schema.FieldSchema
import dev.kdb.codec.schema.FixedSchema
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.LogicalAnnotation
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.codec.schema.RecordSchema

internal object DocFqn {
    const val NS = "dev.kdb.document"
    const val HASH32 = "$NS.Hash32"
    const val DOCUMENT_BODY = "$NS.DocumentBody"
    const val OP_WRITE = "$NS.OpWrite"
    const val OP_DELETE = "$NS.OpDelete"
    const val OP_FILE_WRITE = "$NS.OpFileWrite"
    const val OP_SCHEMA_MIGRATION = "$NS.OpSchemaMigration"
    const val DOC_TREE_ENTRY = "$NS.DocumentTreeEntry"
    const val COMMIT_PAYLOAD = "$NS.CommitPayload"
    const val COMMIT_STUB_WIRE = "$NS.CommitStubWire"
}

private val uuidTy = KdbType.Primitive(PhysicalKind.FIXED, LogicalAnnotation.Uuid)

private val hashRef = KdbType.Ref(DocFqn.HASH32)

private val timestampTy =
    KdbType.Primitive(PhysicalKind.INT64, LogicalAnnotation.TimestampMicros(null))

private fun buildRegistry(): KdbTypeRegistry {
    val reg = KdbTypeRegistry.create()
    reg.registerFixed(
        FixedSchema(
            name = "Hash32",
            namespace = DocFqn.NS,
            size = 32,
        ),
    )
    val opWrite =
        RecordSchema(
            name = "OpWrite",
            namespace = DocFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "docId", uuidTy),
                    FieldSchema(2, "patch", KdbType.Primitive(PhysicalKind.STRING)),
                ),
        )
    val opDelete =
        RecordSchema(
            name = "OpDelete",
            namespace = DocFqn.NS,
            fields = listOf(FieldSchema(1, "docId", uuidTy)),
        )
    val opFileWrite =
        RecordSchema(
            name = "OpFileWrite",
            namespace = DocFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "path", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(2, "blobHash", hashRef),
                ),
        )
    val opSchemaMigration =
        RecordSchema(
            name = "OpSchemaMigration",
            namespace = DocFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "migrationId", uuidTy),
                    FieldSchema(2, "migrationPayload", KdbType.Primitive(PhysicalKind.STRING)),
                ),
        )
    reg.registerRecord(opWrite)
    reg.registerRecord(opDelete)
    reg.registerRecord(opFileWrite)
    reg.registerRecord(opSchemaMigration)

    val kdbOpWire =
        KdbType.Union(
            listOf(
                KdbType.Ref(DocFqn.OP_WRITE),
                KdbType.Ref(DocFqn.OP_DELETE),
                KdbType.Ref(DocFqn.OP_FILE_WRITE),
                KdbType.Ref(DocFqn.OP_SCHEMA_MIGRATION),
            ),
        )

    val documentBody =
        RecordSchema(
            name = "DocumentBody",
            namespace = DocFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "id", uuidTy),
                    FieldSchema(2, "json", KdbType.Primitive(PhysicalKind.STRING)),
                ),
        )
    reg.registerRecord(documentBody)

    val treeEntry =
        RecordSchema(
            name = "DocumentTreeEntry",
            namespace = DocFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "docId", uuidTy),
                    FieldSchema(2, "contentHash", hashRef),
                ),
        )
    reg.registerRecord(treeEntry)

    val commitPayload =
        RecordSchema(
            name = "CommitPayload",
            namespace = DocFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "parentHashes", KdbType.Array(hashRef)),
                    FieldSchema(2, "namespaceId", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(3, "transactionId", uuidTy),
                    FieldSchema(4, "timestamp", timestampTy),
                    FieldSchema(5, "authorNodeId", uuidTy),
                    FieldSchema(6, "operations", KdbType.Array(kdbOpWire)),
                    FieldSchema(7, "documentTreeHash", hashRef),
                    FieldSchema(
                        8,
                        "schemaHash",
                        KdbType.Nullable(hashRef),
                        default = dev.kdb.codec.KdbValue.Null,
                    ),
                    FieldSchema(9, "message", KdbType.Primitive(PhysicalKind.STRING)),
                ),
        )
    reg.registerRecord(commitPayload)

    reg.registerRecord(
        RecordSchema(
            name = "CommitStubWire",
            namespace = DocFqn.NS,
            fields =
                listOf(
                    FieldSchema(1, "originalHash", hashRef),
                    FieldSchema(2, "archiveLocation", KdbType.Primitive(PhysicalKind.STRING)),
                    FieldSchema(3, "stubbedAt", timestampTy),
                ),
        ),
    )

    reg.freeze()
    return reg
}

private val documentWireRegistryLazy: KdbTypeRegistry by lazy { buildRegistry() }

/** Builtin wire registry for [KdbDocumentWireRegistry]. */
public fun KdbDocumentWireRegistry(): KdbTypeRegistry = documentWireRegistryLazy

/** Resolved types from [KdbDocumentWireRegistry]. */
public val DocumentBodyType: KdbType = KdbType.Ref(DocFqn.DOCUMENT_BODY)

public val CommitPayloadType: KdbType = KdbType.Ref(DocFqn.COMMIT_PAYLOAD)

public val KdbOpWireType: KdbType =
    KdbType.Union(
        listOf(
            KdbType.Ref(DocFqn.OP_WRITE),
            KdbType.Ref(DocFqn.OP_DELETE),
            KdbType.Ref(DocFqn.OP_FILE_WRITE),
            KdbType.Ref(DocFqn.OP_SCHEMA_MIGRATION),
        ),
    )

public val DocumentTreeWireType: KdbType = KdbType.Array(KdbType.Ref(DocFqn.DOC_TREE_ENTRY))

public val CommitStubWireType: KdbType = KdbType.Ref(DocFqn.COMMIT_STUB_WIRE)
