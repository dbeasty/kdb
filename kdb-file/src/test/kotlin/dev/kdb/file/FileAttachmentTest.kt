package dev.kdb.file

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class FileAttachmentTest {
  @Test
  fun zipRoundTrip_singleFile() {
    val raw = "hello kdb file".encodeToByteArray()
    val zipped = ZipArchive.zipSingle("test.txt", raw)
    val back = ZipArchive.soleEntryBytes(zipped)
    assertContentEquals(raw, back)
  }

  @Test
  fun ingest_single_raw_survivesRead() =
      runTest {
        val ns = "app/files"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val fileId = KdbUuid.fromString("00000000-0000-0000-0000-0000000000f1")
        val raw = byteArrayOf(1, 2, 3, 4)
        FileAttachmentIngest.ingestSingle(
            ns,
            dag,
            storage,
            raw,
            "bin.dat",
            fileId = fileId,
            zip = false,
        )
        val out =
            FileAttachmentExtract.readFileBytes(ns, dag, storage, fileId)
        assertContentEquals(raw, out)
      }

  @Test
  fun ingest_single_zip_survivesRead() =
      runTest {
        val ns = "app/files"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val fileId = KdbUuid.random()
        val raw = "zip me".encodeToByteArray()
        FileAttachmentIngest.ingestSingle(
            ns,
            dag,
            storage,
            raw,
            "note.txt",
            fileId = fileId,
            zip = true,
        )
        val out =
            FileAttachmentExtract.readFileBytes(ns, dag, storage, fileId)
        assertContentEquals(raw, out)
      }

  @Test
  fun ingest_bundle_zip_extractMember() =
      runTest {
        val ns = "app/bundles"
        val dag = inMemoryCommitDag(ns)
        val storage = InMemoryStorageAdapter()
        val bundleId = KdbUuid.random()
        val a = "aaa".encodeToByteArray()
        val b = "bbb".encodeToByteArray()
        val result =
            FileAttachmentIngest.ingestBundleZip(
                ns,
                dag,
                storage,
                listOf(
                    FileIngestInput(a, "a.txt"),
                    FileIngestInput(b, "b.txt"),
                ),
                bundleId = bundleId,
            )
        assertEquals(2, result.memberFileIds.size)
        val member0 =
            FileAttachmentExtract.readBundleMemberBytes(
                ns,
                dag,
                storage,
                bundleId,
                result.memberFileIds[0],
            )
        assertTrue(a.contentEquals(member0) || b.contentEquals(member0))
      }
}
