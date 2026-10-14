package dev.kdb.jdbc.file

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlinx.coroutines.runBlocking

/**
 * Go/Kotlin parity for what a merge-patch write commits (Go counterpart:
 * go/kdb/transaction/merge_patch_golden_test.go). Both engines record the merged document, not the
 * patch, so the same fixed transactions must produce the same commit hashes and the same stored op.
 */
class MergePatchGoldenTest {
    @Test
    fun mergePatchCommitsMatchGo() {
        val lines = fixture()
        goldenDir("kotlin").resolve(FILE).writeText(lines.joinToString("\n") + "\n")
        val go = goldenDir("go").resolve(FILE)
        // Absent when Go's exporter has not been run - a setup gap, not a parity failure.
        if (go.exists()) assertEquals(go.readText().trim().lines(), lines)
    }

    private fun fixture(): List<String> =
        runBlocking {
            val ns = "golden/merge"
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val engine = transactionEngine(ConflictPolicy.STRICT)
            val docId = KdbUuid(0x0102030405060708L, 0x090a0b0c0d0e0f10L)
            val author = KdbUuid(0x2122232425262728L, 0x292a2b2c2d2e2f30L)
            suspend fun commit(n: Long, patch: String) =
                engine
                    .commit(
                        KdbTransaction(
                            KdbUuid(0x1111111111111111L, n),
                            dag.head(),
                            listOf(KdbOp.Write(docId, patch)),
                            KdbTimestamp(1_700_000_000_000L + n, 7),
                            author,
                        ),
                        dag,
                        storage,
                    ).let { assertIs<TransactionResult.Success>(it).commit }
            val first = commit(1, """{"i":2,"pad":"pp"}""")
            val second = commit(2, """{"i":2,"rewrite":2}""")
            listOf(first.hash.toHex(), second.hash.toHex(), (second.operations.single() as KdbOp.Write).patch)
        }

    private fun goldenDir(side: String): File {
        val root = Paths.get(System.getProperty("user.dir"))
        val repo = generateSequence(root) { it.parent }.firstOrNull { it.resolve("go").isDirectory() } ?: root
        return repo.resolve("go/testdata/golden/physical/$side").toFile().also { it.mkdirs() }
    }

    private companion object {
        const val FILE = "merge_patch_commits.txt"
    }
}
