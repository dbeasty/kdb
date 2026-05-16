package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.index.IndexHint
import dev.kdb.index.IndexHintAction
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType

internal fun KdbOp.toOpDto(): OpDto =
    when (this) {
        is KdbOp.Write ->
            OpDto(
                kind = "write",
                docId = docId.toString(),
                patch = patch,
            )
        is KdbOp.Delete ->
            OpDto(
                kind = "delete",
                docId = docId.toString(),
            )
        is KdbOp.FileWrite ->
            OpDto(
                kind = "fileWrite",
                path = path,
                blobHashHex = blobHash.toHex(),
            )
        is KdbOp.SchemaMigration ->
            OpDto(
                kind = "schemaMigration",
                migrationId = migrationId.toString(),
                migrationPayload = migrationPayload,
            )
    }

internal fun OpDto.toKdbOp(): KdbOp =
    when (kind) {
        "write" ->
            KdbOp.Write(
                docId = KdbUuid.fromString(docId ?: error("write missing docId")),
                patch = patch ?: error("write missing patch"),
            )
        "delete" ->
            KdbOp.Delete(
                docId = KdbUuid.fromString(docId ?: error("delete missing docId")),
            )
        "fileWrite" ->
            KdbOp.FileWrite(
                path = path ?: error("fileWrite missing path"),
                blobHash = KdbHash.fromHex(blobHashHex ?: error("fileWrite missing blobHash")),
            )
        "schemaMigration" ->
            KdbOp.SchemaMigration(
                migrationId = KdbUuid.fromString(migrationId ?: error("schemaMigration missing id")),
                migrationPayload = migrationPayload ?: error("schemaMigration missing payload"),
            )
        else -> error("unknown op kind: $kind")
    }

internal fun IndexHint.toHintDto(): IndexHintDto =
    IndexHintDto(
        indexId = indexId.toString(),
        fieldName = fieldName,
        indexType = type.name,
        action = action.name,
        docId = docId.toString(),
        key = key?.let { indexKeyLabel(it) },
        commitHashHex = commitHash.toHex(),
    )

internal fun IndexHintDto.toIndexHint(): IndexHint =
    IndexHint(
        indexId = KdbUuid.fromString(indexId),
        fieldName = fieldName,
        type = IndexType.valueOf(indexType),
        action = IndexHintAction.valueOf(action),
        docId = KdbUuid.fromString(docId),
        key = null,
        commitHash = KdbHash.fromHex(commitHashHex),
    )

private fun indexKeyLabel(key: IndexKey): String =
    when (key) {
        IndexKey.NullKey -> "null"
        is IndexKey.BoolKey -> key.value.toString()
        is IndexKey.Int32Key -> key.value.toString()
        is IndexKey.Int64Key -> key.value.toString()
        is IndexKey.Float64Key -> key.value.toString()
        is IndexKey.TimestampKey -> key.epochMillis.toString()
        is IndexKey.StringKey -> key.value
        is IndexKey.UuidKey -> key.id.toString()
        is IndexKey.VectorKey -> "vector"
        is IndexKey.CompositeKey -> "composite"
    }
