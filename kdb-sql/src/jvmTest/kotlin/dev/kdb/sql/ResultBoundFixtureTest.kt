package dev.kdb.sql

import dev.kdb.json.JsonValue
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

/**
 * `maxRows` bounds what a query may return, never what it may look at. This runs the cases in
 * `go/testdata/golden/sql/result_bound.json`, which the Go tree reads too
 * (`go/kdb/sql/result_bound_fixture_test.go`), so the two engines are held to the same answers.
 */
class ResultBoundFixtureTest {
    private fun fixture(): JsonValue.JObject {
        val start = Paths.get(System.getProperty("user.dir"))
        val dir: File =
            assertNotNull(
                generateSequence(start) { it.parent }
                    .firstOrNull { it.resolve("go/testdata/golden/sql").isDirectory() }
                    ?.resolve("go/testdata/golden/sql")
                    ?.toFile(),
                "go/testdata/golden/sql not found above ${start}",
            )
        return JsonValue.fromJsonString(dir.resolve("result_bound.json").readText()) as JsonValue.JObject
    }

    private fun JsonValue.JObject.int(name: String): Int =
        when (val v = fields.getValue(name)) {
            is JsonValue.JInt -> v.value.toInt()
            is JsonValue.JNumber -> v.value.toInt()
            else -> error("$name: expected a number")
        }

    private fun JsonValue.JObject.str(name: String): String = (fields.getValue(name) as JsonValue.JString).value

    @Test
    fun everyCaseAgreesWithTheGoEngine() =
        runTest {
            val fx = fixture()
            val docs = fx.int("docs")
            val maxRows = fx.int("maxRows")
            val rt = schemalessRuntime(ns = "app/bound")
            for (i in 0 until docs) rt.put("""{"name":"n${i.toString().padStart(3, '0')}","rank":$i}""")
            val ctx = rt.ctx().copy(maxRows = maxRows)

            for (case in (fx.fields.getValue("cases") as JsonValue.JArray).elements) {
                val c = case as JsonValue.JObject
                val name = c.str("name")
                val sql = c.str("sql")
                if (c.fields["error"] == JsonValue.JBool(true)) {
                    val e = assertFailsWith<SqlResultLimitException>("$name: $sql") { rt.engine.execute(sql, ctx) }
                    assertEquals(maxRows, e.limit, name)
                    continue
                }
                val result = rt.engine.execute(sql, ctx)
                val count = c.fields["count"]
                if (count != null) {
                    assertEquals(c.int("count"), result.rows.size, "$name: $sql")
                } else {
                    val want = (c.fields.getValue("rows") as JsonValue.JArray).elements.map { (it as JsonValue.JString).value }
                    val got = result.rows.map { row ->
                        when (val cell = row.values[0]) {
                            is SqlCell.StringVal -> cell.value
                            is SqlCell.LongVal -> cell.value.toString()
                            else -> cell.toString()
                        }
                    }
                    assertEquals(want, got, "$name: $sql")
                }
            }
        }
}
