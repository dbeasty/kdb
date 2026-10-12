package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.json.JsonValue
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Keyed `kdb_id` lookups, the examined-rows budget and the result bound. Runs the cases in
 * `go/testdata/golden/sql/cost_behaviour.json`, which the Go tree reads too
 * (`go/kdb/sql/cost_behaviour_fixture_test.go`), so both engines are held to the same cost
 * behaviour, not just the same answers.
 */
class CostBehaviourFixtureTest {
    private fun fixture(): JsonValue.JObject {
        val start = Paths.get(System.getProperty("user.dir"))
        val dir: File =
            assertNotNull(
                generateSequence(start) { it.parent }
                    .firstOrNull { it.resolve("go/testdata/golden/sql").isDirectory() }
                    ?.resolve("go/testdata/golden/sql")
                    ?.toFile(),
                "go/testdata/golden/sql not found above $start",
            )
        return JsonValue.fromJsonString(dir.resolve("cost_behaviour.json").readText()) as JsonValue.JObject
    }

    private fun JsonValue.JObject.int(name: String): Int? =
        when (val v = fields[name]) {
            is JsonValue.JInt -> v.value.toInt()
            is JsonValue.JNumber -> v.value.toInt()
            else -> null
        }

    private fun JsonValue.JObject.str(name: String): String? = (fields[name] as? JsonValue.JString)?.value

    private fun JsonValue.JObject.strings(name: String): List<String>? =
        (fields[name] as? JsonValue.JArray)?.elements?.map { (it as JsonValue.JString).value }

    private val idShorthand = Regex("""\$\{id:(\d+)}""")

    private fun expandIds(s: String): String =
        idShorthand.replace(s) { m -> "00000000-0000-4000-8000-" + m.groupValues[1].padStart(12, '0') }

    @Test
    fun everyCaseAgreesWithTheGoEngine() =
        runTest {
            val fx = fixture()
            val docs = fx.int("docs")!!
            val rt = schemalessRuntime(ns = "app/cost")
            for (i in 0 until docs) {
                val id = KdbUuid.fromString(expandIds("\${id:${i + 1}}"))
                rt.put("""{"name":"n${i.toString().padStart(3, '0')}","rank":$i}""", id)
            }
            for (case in (fx.fields.getValue("cases") as JsonValue.JArray).elements) {
                val c = case as JsonValue.JObject
                val name = c.str("name")!!
                val sql = expandIds(c.str("sql")!!)
                val params = (c.strings("params") ?: emptyList()).map { SqlParameter.StringParam(expandIds(it)) }
                var ctx = rt.ctx(params).copy(rowBudget = c.int("rowBudget") ?: 0)
                c.int("maxRows")?.let { ctx = ctx.copy(maxRows = it) }

                val budgetError = c.int("budgetError")
                if (budgetError != null) {
                    val e = assertFailsWith<SqlRowBudgetException>("$name: $sql") { rt.engine.execute(sql, ctx) }
                    assertEquals(budgetError, e.budget, name)
                    continue
                }
                val result = rt.engine.execute(sql, ctx)
                val got =
                    result.rows.map { row ->
                        when (val cell = row.values[0]) {
                            is SqlCell.StringVal -> cell.value
                            is SqlCell.LongVal -> cell.value.toString()
                            else -> cell.toString()
                        }
                    }
                assertEquals(c.strings("rows"), got, "$name: $sql")

                val access = c.str("access")
                if (access != null) {
                    val plan = describeAccessPath(rt.engine.explain(sql, ctx).plan)
                    if (access == "keylookup") {
                        assertTrue(plan.startsWith("KeyLookup"), "$name: plan was $plan")
                    } else {
                        assertTrue(!plan.startsWith("KeyLookup"), "$name: served by key, want a scan")
                    }
                }
            }
        }
}
