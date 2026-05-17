package dev.kdb.wire

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbTransaction
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

private val json = Json { ignoreUnknownKeys = true }

@Serializable
private data class WireKdbTransactionDto(
    val id: String,
    val baseVersionHex: String,
    val timestampMicros: Long,
    val authorNodeId: String,
    val operations: List<OpDto>,
)

public object TransactionWireCodec {
    public fun encode(transaction: KdbTransaction): ByteArray {
        val dto =
            WireKdbTransactionDto(
                id = transaction.id.toString(),
                baseVersionHex = transaction.baseVersion.toHex(),
                timestampMicros = transaction.timestamp.toEpochMicros(),
                authorNodeId = transaction.authorNodeId.toString(),
                operations = transaction.operations.map { it.toOpDto() },
            )
        return json.encodeToString(dto).encodeToByteArray()
    }

    public fun decode(bytes: ByteArray): KdbTransaction {
        val dto = json.decodeFromString<WireKdbTransactionDto>(bytes.decodeToString())
        return KdbTransaction(
            id = KdbUuid.fromString(dto.id),
            baseVersion = KdbHash.fromHex(dto.baseVersionHex),
            operations = dto.operations.map { it.toKdbOp() },
            timestamp = KdbTimestamp.fromEpochMicros(dto.timestampMicros),
            authorNodeId = KdbUuid.fromString(dto.authorNodeId),
        )
    }
}
