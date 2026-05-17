package dev.kdb.file

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream

internal data class ZipEntryPayload(
    val pathInArchive: String,
    val bytes: ByteArray,
)

internal object ZipArchive {
    fun zipSingle(entryName: String, bytes: ByteArray): ByteArray = zip(listOf(ZipEntryPayload(entryName, bytes)))

    fun zip(entries: List<ZipEntryPayload>): ByteArray {
        require(entries.isNotEmpty()) { "zip requires at least one entry" }
        val names = entries.map { it.pathInArchive }
        require(names.distinct().size == names.size) { "duplicate zip entry paths: ${names.groupingBy { it }.eachCount().filter { it.value > 1 }.keys}" }
        val out = ByteArrayOutputStream()
        ZipOutputStream(out).use { zos ->
            for (entry in entries) {
                val ze = ZipEntry(entry.pathInArchive)
                zos.putNextEntry(ze)
                zos.write(entry.bytes)
                zos.closeEntry()
            }
        }
        return out.toByteArray()
    }

    fun unzipAll(zipBytes: ByteArray): List<ZipEntryPayload> {
        val result = mutableListOf<ZipEntryPayload>()
        ZipInputStream(ByteArrayInputStream(zipBytes)).use { zis ->
            var entry = zis.nextEntry
            while (entry != null) {
                if (!entry.isDirectory) {
                    val bytes = zis.readBytes()
                    result += ZipEntryPayload(entry.name, bytes)
                }
                zis.closeEntry()
                entry = zis.nextEntry
            }
        }
        return result
    }

    fun extractEntry(zipBytes: ByteArray, entryName: String): ByteArray {
        ZipInputStream(ByteArrayInputStream(zipBytes)).use { zis ->
            var entry = zis.nextEntry
            while (entry != null) {
                if (!entry.isDirectory && entry.name == entryName) {
                    return zis.readBytes()
                }
                zis.closeEntry()
                entry = zis.nextEntry
            }
        }
        throw IllegalArgumentException("zip entry not found: $entryName")
    }

    fun soleEntryBytes(zipBytes: ByteArray): ByteArray {
        val entries = unzipAll(zipBytes)
        require(entries.size == 1) { "expected exactly one zip entry, found ${entries.size}" }
        return entries.single().bytes
    }
}
