package dev.kdb.integration

import dev.kdb.codec.KdbValue
import dev.kdb.codec.encodeToBytes
import dev.kdb.codec.schema.FieldSchema
import dev.kdb.codec.schema.KdbType
import dev.kdb.codec.schema.KdbTypeRegistry
import dev.kdb.codec.schema.PhysicalKind
import dev.kdb.codec.schema.RecordSchema
import kotlin.io.path.isDirectory
import java.io.File
import java.nio.file.Paths
import kotlin.test.Test

/**
 * Writes wire golden files for Go interop tests under go/testdata/golden/codec/.
 * Run: ./gradlew :kdb-integration:test --tests "dev.kdb.integration.ExportGoldenTest"
 */
class ExportGoldenTest {

    private val goldenDir: File by lazy {
        val root = Paths.get(System.getProperty("user.dir"))
        val repo = generateSequence(root) { it.parent }
            .firstOrNull { it.resolve("go").isDirectory() }
            ?: root
        repo.resolve("go/testdata/golden/codec").toFile().also { it.mkdirs() }
    }

    @Test
    fun exportCodecGoldens() {
        val reg = mkDoc(listOf(Pair("n", KdbType.Primitive(PhysicalKind.INT32))))
        val t = KdbType.Ref("demo.Doc")
        val docBlob =
            KdbValue.RecordVal(mapOf(1 to KdbValue.Int32Val(-3))).encodeToBytes(t, reg)
        writeHex("doc_n_minus3.hex", docBlob)

        val builtin = KdbTypeRegistry.builtin()
        val intMin =
            KdbValue.Int32Val(Int.MIN_VALUE).encodeToBytes(
                KdbType.Primitive(PhysicalKind.INT32),
                builtin,
            )
        writeHex("int32_min.hex", intMin)

        val str =
            KdbValue.StringVal("〰 KDB").encodeToBytes(
                KdbType.Primitive(PhysicalKind.STRING),
                builtin,
            )
        writeHex("string_utf8.hex", str)
    }

    private fun writeHex(name: String, bytes: ByteArray) {
        val hex = bytes.joinToString("") { "%02x".format(it) }
        goldenDir.resolve(name).writeText(hex + "\n")
    }
}

private fun mkDoc(props: List<Pair<String, KdbType>>): KdbTypeRegistry {
    val reg = KdbTypeRegistry.create()
    val fields =
        props.mapIndexed { idx, pair ->
            FieldSchema(idx + 1, pair.first, pair.second)
        }
    reg.registerRecord(RecordSchema(name = "Doc", namespace = "demo", fields = fields))
    reg.freeze()
    return reg
}
