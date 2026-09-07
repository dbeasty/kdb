package dev.kdb.sql

import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.SchemaField
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Component 70 §3 item 4: a parser that panics on malformed input is a bug. Nothing but
 * [SqlParseException] / [SqlPlanningException] may escape parsing or planning, however broken the
 * statement is.
 */
class SqlMalformedInputTest {
    private val malformed =
        listOf(
            "",
            "   ",
            "SELECT",
            "SELECT FROM t",
            "SELECT *",
            "SELECT * FROM",
            "SELECT * FROM t WHERE",
            "SELECT * FROM t WHERE a =",
            "SELECT * FROM t WHERE = 5",
            "SELECT * FROM t WHERE a == 5",
            "SELECT * FROM t WHERE (a = 5",
            "SELECT * FROM t WHERE a = 'unterminated",
            "SELECT * FROM t WHERE a IN ()",
            "SELECT * FROM t WHERE a IN (1,",
            "SELECT * FROM t WHERE a BETWEEN 1",
            "SELECT * FROM t WHERE a BETWEEN AND 2",
            "SELECT * FROM t WHERE a LIKE",
            "SELECT * FROM t WHERE a NOT",
            "SELECT * FROM t WHERE a NOT SOMETHING 'x'",
            "SELECT * FROM t WHERE a IS",
            "SELECT * FROM t WHERE a IS NOT",
            "SELECT * FROM t ORDER BY",
            "SELECT * FROM t ORDER BY a ASCENDING",
            "SELECT * FROM t GROUP BY",
            "SELECT * FROM t LIMIT",
            "SELECT * FROM t LIMIT abc",
            "SELECT * FROM t LIMIT 1 OFFSET",
            "SELECT * FROM t LIMIT 99999999999999999999",
            "SELECT COUNT( FROM t",
            "SELECT COUNT(*, a) FROM t",
            "SELECT MATCH() FROM t",
            "SELECT MATCH(idx) FROM t",
            "SELECT SIMILARITY(v) FROM t",
            "SELECT SIMILARITY(v, 'text') FROM t",
            "SELECT FUSE(MATCH(i,'a')) FROM t",
            "SELECT FUSE('rrf') FROM t",
            "SELECT ARRAY_LENGTH() FROM t",
            "SELECT * FROM t WHERE ARRAY_CONTAINS()",
            "SELECT * FROM t WHERE ARRAY_CONTAINS('literal', 1)",
            "SELECT * FROM t t2 t3",
            "SELEC * FROM t",
            "SELECT * FROM t GARBAGE",
            "SELECT * FROM t; DROP TABLE t",
            "INSERT INTO t VALUES (1)",
            "INSERT INTO t (a) VALUES",
            "INSERT INTO t (a) VALUES (1, 2)",
            "UPDATE t SET",
            "UPDATE t SET a",
            "UPDATE t SET a = WHERE b = 1",
            "DELETE t",
            "DELETE FROM",
            "CREATE TABLE t",
            "CREATE TABLE t ()",
            "CREATE TABLE t (a NOTATYPE)",
            "CREATE TABLE t (UNIQUE ())",
            "CREATE INDEX",
            "CREATE INDEX i ON t",
            "CREATE INDEX i ON t ()",
            "CREATE INDEX i ON t (a) USING NOTATYPE",
            "CREATE INDEX i ON t (a WEIGHT) USING FULLTEXT",
            "CREATE INDEX i ON t (a WEIGHT 0) USING FULLTEXT",
            "CREATE INDEX i ON t (a) USING VECTOR WITH (",
            "CREATE INDEX i ON t (a) USING VECTOR WITH (dimensions)",
            "DROP INDEX i",
            "DROP",
            "ALTER TABLE t",
            "ALTER TABLE t ADD COLUMN",
            "GRANT write ON DATABASE d",
            "BEGIN TRANSACTION EXTRA",
            "SELECT * FROM t WHERE a = [1,]",
            "SELECT * FROM t WHERE a = []",
            "SELECT * FROM t WHERE a = 1.2.3",
            "SELECT * FROM t WHERE a = 1e",
        )

    /** Every malformed statement fails as a parse or planning error, never with another exception. */
    @Test
    fun malformedStatementsRaiseOnlySqlExceptions() =
        runTest {
            val fx =
                schemaRuntime(
                    listOf(
                        SchemaField("a", KdbFieldType.StringType, required = false, indexed = true),
                        SchemaField("b", KdbFieldType.Int64Type, required = false, indexed = true),
                    ),
                    ns = "app/t",
                )
            fx.put("""{"a":"x","b":1}""")
            assertTrue(malformed.size >= 40, "expected ~40 malformed statements, got ${malformed.size}")
            for (sql in malformed) {
                try {
                    fx.engine.execute(sql, fx.ctx())
                    // A statement that happens to be valid is fine as long as it did not crash.
                } catch (e: SqlParseException) {
                    // expected
                } catch (e: SqlPlanningException) {
                    // expected
                } catch (e: Throwable) {
                    fail("`$sql` raised ${e::class.simpleName}: ${e.message}")
                }
            }
        }

    /** The same guarantee for the DML entry point. */
    @Test
    fun malformedDmlRaisesOnlySqlExceptions() =
        runTest {
            val fx =
                schemaRuntime(
                    listOf(SchemaField("a", KdbFieldType.StringType, required = false, indexed = true)),
                    ns = "app/t",
                )
            for (sql in malformed) {
                try {
                    fx.engine.executeDml(sql, fx.ctx())
                } catch (e: SqlParseException) {
                    // expected
                } catch (e: SqlPlanningException) {
                    // expected
                } catch (e: Throwable) {
                    fail("`$sql` raised ${e::class.simpleName}: ${e.message}")
                }
            }
        }

    /** `WHERE stringcol = 5` is a type mismatch, not a crash: it simply matches nothing (§2). */
    @Test
    fun typeMismatchPredicateReturnsNoRows() =
        runTest {
            val fx =
                schemaRuntime(
                    listOf(SchemaField("a", KdbFieldType.StringType, required = false, indexed = true)),
                    ns = "app/t",
                )
            fx.put("""{"a":"x"}""")
            assertTrue(fx.query("SELECT a FROM t WHERE a = 5").rows.isEmpty())
        }
}
