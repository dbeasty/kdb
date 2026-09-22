package dev.kdb.script

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.error.ConflictOperationType
import dev.kdb.transaction.DocumentConflict
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The Kotlin half of Go's `kdb/script/resolver_test.go`, over the same decisions. */
class ProcedureConflictResolverTest {
    private fun conflict(
        existing: String? = """{"qty":5}""",
        incoming: String? = """{"qty":9}""",
    ): DocumentConflict {
        val id = KdbUuid.random()
        return DocumentConflict(
            docId = id,
            operationType = ConflictOperationType.CONCURRENT_WRITE,
            existingDoc = existing?.let { KdbDocument(id, it) },
            incomingDoc = incoming?.let { KdbDocument(id, it) },
            baseDoc = KdbDocument(id, """{"qty":1}"""),
        )
    }

    @Test
    fun takesASide() =
        runTest {
            val r = ProcedureConflictResolver("""function main(c) { return { take: c.local.qty > c.remote.qty ? "local" : "remote" }; }""")
            assertEquals("""{"qty":9}""", r.resolve(conflict())!!.json)
        }

    @Test
    fun returnsABuiltDocument() =
        runTest {
            val r = ProcedureConflictResolver("function main(c) { return { doc: { qty: c.local.qty + c.remote.qty } }; }")
            val c = conflict()
            val got = r.resolve(c)!!
            assertEquals("""{"qty":14}""", got.json)
            assertEquals(c.docId, got.id)
        }

    @Test
    fun deferLeavesTheConflictReported() =
        runTest {
            // null with no exception is what the engine reads as "report it", which is what defer
            // means here.
            assertNull(ProcedureConflictResolver("function main(c) { return { defer: true }; }").resolve(conflict()))
        }

    @Test
    fun refusesWhatALocalCommitCannotDo() =
        runTest {
            for (source in listOf(
                """function main(c) { return { fork: { keep: "local" } }; }""",
                "function main(c) { return { delete: true }; }",
            )) {
                val e = assertFailsWith<ProcException>(source) { ProcedureConflictResolver(source).resolve(conflict()) }
                assertTrue(e.message!!.contains("only take, doc and defer"), e.message!!)
            }
        }

    @Test
    fun refusesToTakeADeletedSide() =
        runTest {
            val r = ProcedureConflictResolver("""function main(c) { return { take: "remote" }; }""")
            val e = assertFailsWith<ProcException> { r.resolve(conflict(incoming = null)) }
            assertTrue(e.message!!.contains("is a delete"), e.message!!)
        }

    @Test
    fun seesWhichFieldsConflict() =
        runTest {
            val r = ProcedureConflictResolver("""function main(c) { return { doc: { changed: c.fields[0].path + ":" + c.fields[0].kind } }; }""")
            assertEquals("""{"changed":"/qty:both"}""", r.resolve(conflict())!!.json)
        }
}
