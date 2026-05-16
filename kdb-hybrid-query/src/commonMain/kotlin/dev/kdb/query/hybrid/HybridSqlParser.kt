package dev.kdb.query.hybrid

import dev.kdb.sql.SqlParser
import dev.kdb.sql.SqlStatement
import dev.kdb.sql.defaultSqlParser

public interface HybridSqlParser {
    public fun parseWithVersion(sql: String): ParsedHybridStatement
    public fun parse(sql: String): SqlStatement
}

public fun hybridSqlParser(base: SqlParser = defaultSqlParser()): HybridSqlParser =
    DefaultHybridSqlParser(base)

public class DefaultHybridSqlParser(
    private val base: SqlParser,
) : HybridSqlParser {
    override fun parseWithVersion(sql: String): ParsedHybridStatement {
        val (stripped, version) = stripVersionClause(sql.trim())
        base.parse(stripped)
        return ParsedHybridStatement(stripped, version)
    }

    override fun parse(sql: String): SqlStatement = base.parse(sql)

    internal companion object {
        internal fun stripVersionClause(sql: String): Pair<String, VersionClause?> {
            val keywords =
                listOf(
                    "AT VERSION" to { lit: String -> VersionClause.AtTag(lit) },
                    "AT COMMIT" to { lit: String -> VersionClause.AtCommit(lit) },
                    "AT TIME" to { lit: String -> VersionClause.AtTime(lit) },
                )
            for ((keyword, factory) in keywords) {
                val idx = sql.lastIndexOf(keyword, ignoreCase = true)
                if (idx < 0) continue
                val tail = sql.substring(idx + keyword.length).trim()
                val literal = readQuotedLiteral(tail) ?: continue
                val stripped = sql.substring(0, idx).trim()
                return stripped to factory(literal)
            }
            return sql to null
        }

        private fun readQuotedLiteral(tail: String): String? {
            if (tail.isEmpty() || tail[0] != '\'') return null
            val end = tail.indexOf('\'', 1)
            if (end < 0) return null
            return tail.substring(1, end)
        }
    }
}
