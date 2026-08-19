package dev.kdb.storage.chunking

import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.file.FileAttachmentExtract
import dev.kdb.file.FileAttachmentIngest
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

/**
 * Proves chunk-level dedup applies to the real `kdb-file` ingest/extract path with zero
 * changes to that module: it only knows about [dev.kdb.storage.StorageAdapter], so passing
 * a [ChunkedStorageAdapter] instead of a plain adapter is enough.
 */
class FileIngestChunkingIntegrationTest {

    @Test
    fun ingestingARevisedFile_dedupsAgainstThePreviousRevision() =
        runTest {
            val ns = "app/files"
            val dag = inMemoryCommitDag(ns)
            val adapter = ChunkedStorageAdapter(InMemoryStorageAdapter())

            val v1 = randomBytes(2 * 1024 * 1024, seed = 70)
            val v2 = withInsertion(v1, atOffset = 1024 * 1024, insertedLength = 300, seed = 71)

            val ingestV1 =
                FileAttachmentIngest.ingestSingle(
                    ns,
                    dag,
                    adapter,
                    v1,
                    "report.bin",
                )
            val bytesAfterV1 = (adapter.blobStore.chunks() as InMemoryChunkStore).totalBytes

            val ingestV2 =
                FileAttachmentIngest.ingestSingle(
                    ns,
                    dag,
                    adapter,
                    v2,
                    "report.bin",
                )
            val bytesAfterV2 = (adapter.blobStore.chunks() as InMemoryChunkStore).totalBytes

            assertContentEquals(v1, FileAttachmentExtract.readFileBytes(ns, dag, adapter, ingestV1.fileId))
            assertContentEquals(v2, FileAttachmentExtract.readFileBytes(ns, dag, adapter, ingestV2.fileId))

            assertTrue(
                bytesAfterV2 - bytesAfterV1 < v1.size / 4,
                "ingesting a near-duplicate revision through kdb-file should mostly reuse chunks, " +
                    "not store a second full copy",
            )
        }
}
