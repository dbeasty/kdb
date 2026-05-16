package dev.kdb.transaction

import dev.kdb.codec.*
import dev.kdb.document.*
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaMigration
import dev.kdb.schema.fromBytes
import dev.kdb.schema.toBytes
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public object SchemaMigrationCodec {
    public fun encode(migration: SchemaMigration): String =
        migration.toBytes().toLowerHex()

    public fun decode(payload: String): SchemaMigration =
        SchemaMigration.fromBytes(payload.decodeLowerHex())
}

/** Fluent transaction builder ([Component 7]). */
public class TransactionBuilder(
    public val namespaceId: String,
    public val baseVersion: KdbHash,
    public val authorNodeId: KdbUuid,
    public val schema: KdbSchema = KdbSchema.NONE,
) {

    private val mutex = Mutex()
    private val ops = mutableListOf<KdbOp>()

    public suspend fun write(
        docId: KdbUuid,
        patchJson: String,
    ): TransactionBuilder {
        mutex.withLock {
            ops += KdbOp.Write(docId, patchJson)
        }
        return this
    }

    public suspend fun writeDocument(document: KdbDocument): TransactionBuilder =
        write(document.id, document.json)

    public suspend fun delete(docId: KdbUuid): TransactionBuilder {
        mutex.withLock {
            ops += KdbOp.Delete(docId)
        }
        return this
    }

    public suspend fun fileWrite(
        path: String,
        blobHash: KdbHash,
    ): TransactionBuilder {
        mutex.withLock {
            ops += KdbOp.FileWrite(path, blobHash)
        }
        return this
    }

    public suspend fun schemaMigration(migration: SchemaMigration): TransactionBuilder {
        mutex.withLock {
            ops +=
                KdbOp.SchemaMigration(
                    migrationId = migration.migrationId,
                    migrationPayload = SchemaMigrationCodec.encode(migration),
                )
        }
        return this
    }

    public suspend fun build(timestamp: KdbTimestamp = KdbTimestamp.now()): KdbTransaction =
        mutex.withLock {
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion = baseVersion,
                operations = ops.toList(),
                timestamp = timestamp,
                authorNodeId = authorNodeId,
            )
        }
}
