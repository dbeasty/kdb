package dev.kdb.sql

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Component 71 — predicate coverage over documents (Layer 16 §4): dotted paths with implicit
 * array traversal, the LIKE family, NOT forms, the ARRAY_* functions, and bare boolean columns.
 */
class SqlPredicateCoverageTest {
    /** The qtask-shaped corpus: nested objects, arrays of objects, and arrays of scalars. */
    private suspend fun tasks(): SqlTestRuntime {
        val fx = schemalessRuntime()
        fx.put(
            """{"title":"Deploy staging","done":true,"tags":["ops","urgent"],"projectIds":["p1","p2"],
               "collaborators":[{"userId":"u1"},{"userId":"u2"}],
               "steps":[{"text":"prepare deploy"},{"text":"verify"}],"meta":{"points":3}}""".trimIndent(),
        )
        fx.put(
            """{"title":"Write docs","done":false,"tags":["docs"],"projectIds":["p3"],
               "collaborators":[{"userId":"u3"}],
               "steps":[{"text":"outline"}],"meta":{"points":8}}""".trimIndent(),
        )
        return fx
    }

    /** A dotted path into an array of objects matches when ANY element satisfies it (§2). */
    @Test
    fun nestedArrayPathUsesAnySemantics() =
        runTest {
            val fx = tasks()
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE collaborators.userId = 'u2'").strings())
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE collaborators.userId = 'nobody'").rows.size)
        }

    /** A scalar array compares as membership: `projectIds = 'p1'` is "contains p1". */
    @Test
    fun scalarArrayEqualityIsMembership() =
        runTest {
            val fx = tasks()
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE projectIds = 'p1'").strings())
        }

    /** LIKE traverses arrays the same way, so `steps.text LIKE '%deploy%'` matches any step. */
    @Test
    fun likeOverAnArrayPath() =
        runTest {
            val fx = tasks()
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE steps.text LIKE '%deploy%'").strings())
        }

    /** LIKE is case-sensitive (it used to ignore case); ILIKE is the case-insensitive form. */
    @Test
    fun likeIsCaseSensitiveAndIlikeIsNot() =
        runTest {
            val fx = tasks()
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE title LIKE 'deploy%'").rows.size)
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE title LIKE 'Deploy%'").strings())
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE title ILIKE 'deploy%'").strings())
        }

    /** `_` matches exactly one character and `%` any run. */
    @Test
    fun likeWildcards() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"code":"ab"}""")
            fx.put("""{"code":"abc"}""")
            assertEquals(listOf("ab"), fx.query("SELECT code FROM tasks WHERE code LIKE 'a_'").strings())
            assertEquals(2, fx.query("SELECT code FROM tasks WHERE code LIKE 'a%'").rows.size)
        }

    /** Regex metacharacters in a LIKE pattern are literal, not pattern syntax. */
    @Test
    fun likeEscapesRegexMetacharacters() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"code":"a.c"}""")
            fx.put("""{"code":"abc"}""")
            assertEquals(listOf("a.c"), fx.query("SELECT code FROM tasks WHERE code LIKE 'a.c'").strings())
            assertEquals(listOf("a.c"), fx.query("SELECT code FROM tasks WHERE code LIKE 'a.%'").strings())
        }

    /** NOT LIKE / NOT ILIKE negate the whole match. */
    @Test
    fun notLikeAndNotIlike() =
        runTest {
            val fx = tasks()
            assertEquals(listOf("Write docs"), fx.query("SELECT title FROM tasks WHERE title NOT LIKE 'Deploy%'").strings())
            assertEquals(listOf("Write docs"), fx.query("SELECT title FROM tasks WHERE title NOT ILIKE 'deploy%'").strings())
        }

    /** NOT IN excludes every listed value. */
    @Test
    fun notInList() =
        runTest {
            val fx = tasks()
            assertEquals(listOf("Write docs"), fx.query("SELECT title FROM tasks WHERE title NOT IN ('Deploy staging')").strings())
            assertEquals(2, fx.query("SELECT title FROM tasks WHERE title NOT IN ('other')").rows.size)
        }

    /** BETWEEN is inclusive and NOT BETWEEN is its complement. */
    @Test
    fun betweenAndNotBetween() =
        runTest {
            val fx = tasks()
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE meta.points BETWEEN 1 AND 3").strings())
            assertEquals(listOf("Write docs"), fx.query("SELECT title FROM tasks WHERE meta.points NOT BETWEEN 1 AND 3").strings())
        }

    /** A bare column predicate is true iff a candidate is boolean `true`. */
    @Test
    fun bareBooleanColumnPredicate() =
        runTest {
            val fx = tasks()
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE done").strings())
            assertEquals(listOf("Write docs"), fx.query("SELECT title FROM tasks WHERE NOT done").strings())
        }

    /** ARRAY_CONTAINS with one value is membership; with several it is the superset test. */
    @Test
    fun arrayContainsIsSupersetWithSeveralValues() =
        runTest {
            val fx = tasks()
            assertEquals(
                listOf("Deploy staging"),
                fx.query("SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'ops')").strings(),
            )
            assertEquals(
                listOf("Deploy staging"),
                fx.query("SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'ops', 'urgent')").strings(),
            )
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'ops', 'docs')").rows.size)
        }

    /** ARRAY_CONTAINS_ANY needs only one of the listed values. */
    @Test
    fun arrayContainsAnyNeedsOneValue() =
        runTest {
            val fx = tasks()
            assertEquals(
                2,
                fx.query("SELECT title FROM tasks WHERE ARRAY_CONTAINS_ANY(tags, 'ops', 'docs')").rows.size,
            )
            assertEquals(0, fx.query("SELECT title FROM tasks WHERE ARRAY_CONTAINS_ANY(tags, 'nope')").rows.size)
        }

    /** ARRAY_CONTAINS compares numbers numerically, so 1 and 1.0 are the same element. */
    @Test
    fun arrayContainsComparesNumbersNumerically() =
        runTest {
            val fx = schemalessRuntime()
            fx.put("""{"title":"a","nums":[1,2]}""")
            assertEquals(1, fx.query("SELECT title FROM tasks WHERE ARRAY_CONTAINS(nums, 1.0)").rows.size)
        }

    /** ARRAY_LENGTH is an integer operand, and NULL when the value is not an array. */
    @Test
    fun arrayLengthAsOperandAndOnNonArrays() =
        runTest {
            val fx = tasks()
            assertEquals(
                listOf("Deploy staging"),
                fx.query("SELECT title FROM tasks WHERE ARRAY_LENGTH(tags) = 2").strings(),
            )
            assertEquals(listOf("Deploy staging"), fx.query("SELECT title FROM tasks WHERE ARRAY_LENGTH(tags) > 1").strings())
            val lengths = fx.query("SELECT ARRAY_LENGTH(tags) AS n FROM tasks ORDER BY n ASC")
            assertEquals(listOf(1L, 2L), lengths.column(0).map { it.long() })
            assertEquals(SqlCell.Null, fx.query("SELECT ARRAY_LENGTH(title) AS n FROM tasks LIMIT 1").rows.single().values[0])
        }

    /** A table alias qualifying a column is stripped before the path is resolved (§2). */
    @Test
    fun tableAliasQualifierIsStripped() =
        runTest {
            val fx = tasks()
            assertEquals(
                listOf("Deploy staging"),
                fx.query("SELECT t.title FROM tasks t WHERE t.collaborators.userId = 'u1'").strings(),
            )
            assertEquals(
                listOf("Deploy staging"),
                fx.query("SELECT tasks.title FROM tasks WHERE tasks.title = 'Deploy staging'").strings(),
            )
        }

    /** ORDER BY and projection take the first candidate of an array path. */
    @Test
    fun firstCandidateIsUsedForProjectionAndOrdering() =
        runTest {
            val fx = tasks()
            val result = fx.query("SELECT collaborators.userId AS who FROM tasks ORDER BY collaborators.userId ASC")
            assertEquals(listOf("u1", "u3"), result.strings())
        }

    /**
     * The old `SqlPredicate.eval` silently answered false for any expression kind it did not
     * understand; unsupported kinds are now an explicit error.
     */
    @Test
    fun unsupportedPredicateExpressionIsAnError() =
        runTest {
            val fx = tasks()
            assertFailsWith<SqlPlanningException> { fx.query("SELECT title FROM tasks WHERE 5") }
            assertFailsWith<SqlPlanningException> { fx.query("SELECT title FROM tasks WHERE 'text'") }
        }

    /** An unknown function name is a planning error rather than a false predicate. */
    @Test
    fun unknownFunctionIsAnError() =
        runTest {
            val fx = tasks()
            assertFailsWith<SqlPlanningException> { fx.query("SELECT title FROM tasks WHERE NO_SUCH_FN(tags)") }
        }

    /** Combined AND/OR predicates over document paths still work end to end. */
    @Test
    fun combinedPredicates() =
        runTest {
            val fx = tasks()
            val result =
                fx.query(
                    "SELECT title FROM tasks WHERE ARRAY_CONTAINS(tags, 'ops') AND meta.points < 5 OR title ILIKE 'write%'",
                )
            assertTrue(result.strings().containsAll(listOf("Deploy staging", "Write docs")))
        }
}
