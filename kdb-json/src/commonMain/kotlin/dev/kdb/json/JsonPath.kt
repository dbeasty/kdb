package dev.kdb.json

import dev.kdb.error.JsonPathException

internal sealed class PathSeg {
    data object Root : PathSeg()

    data class Field(
        val name: String,
    ) : PathSeg()

    data class Idx(
        val index: Int,
    ) : PathSeg()

    data object WildcardElem : PathSeg()

    data object WildcardField : PathSeg()
}

/**
 * Compiled JSONPath (`$.field`, `$.a[0]`, wildcards for [kdbJsonGetAll] only).
 */
public class JsonPath private constructor(
    public val expression: String,
    internal val segments: List<PathSeg>,
) {
    public companion object {
        private val cache = LinkedHashMap<String, JsonPath>()

        public fun compile(expression: String): JsonPath =
            JsonPathCacheLock.withLock {
                cache[expression]?.let { return@withLock it }
                val p = parse(expression)
                val jp = JsonPath(expression, p)
                if (cache.size >= 256) {
                    val k = cache.keys.iterator().next()
                    cache.remove(k)
                }
                cache[expression] = jp
                jp
            }

        public fun compileOrNull(expression: String): JsonPath? =
            try {
                compile(expression)
            } catch (_: JsonPathException) {
                null
            }

        private fun parse(expr: String): List<PathSeg> {
            if (expr.isEmpty() || expr[0] != '$') {
                throw JsonPathException("path must start with \$", expr)
            }
            if (expr.length == 1) {
                return listOf(PathSeg.Root)
            }
            if (expr[1] != '.') {
                throw JsonPathException("expected . after \$", expr)
            }
            val out = mutableListOf<PathSeg>(PathSeg.Root)
            var i = 2

            fun parseNameOrStar() {
                if (i < expr.length && expr[i] == '*') {
                    i++
                    out.add(PathSeg.WildcardField)
                } else {
                    val startI = i
                    while (i < expr.length && expr[i] != '.' && expr[i] != '[') {
                        i++
                    }
                    if (i == startI) {
                        throw JsonPathException("field name expected", expr)
                    }
                    out.add(PathSeg.Field(expr.substring(startI, i)))
                }
            }

            fun parseBracket() {
                i++
                if (i < expr.length && expr[i] == '*') {
                    i++
                    if (i >= expr.length || expr[i] != ']') {
                        throw JsonPathException("expected ] after *", expr)
                    }
                    i++
                    out.add(PathSeg.WildcardElem)
                } else {
                    var neg = false
                    if (i < expr.length && expr[i] == '-') {
                        neg = true
                        i++
                    }
                    val ds = i
                    while (i < expr.length && expr[i] in '0'..'9') {
                        i++
                    }
                    if (i == ds) {
                        throw JsonPathException("index expected", expr)
                    }
                    var idx = expr.substring(ds, i).toInt()
                    if (neg) {
                        idx = -idx
                    }
                    if (i >= expr.length || expr[i] != ']') {
                        throw JsonPathException("expected ]", expr)
                    }
                    i++
                    out.add(PathSeg.Idx(idx))
                }
            }

            parseNameOrStar()
            while (i < expr.length) {
                when (expr[i]) {
                    '.' -> {
                        i++
                        parseNameOrStar()
                    }
                    '[' -> parseBracket()
                    else -> throw JsonPathException("unexpected '${expr[i]}'", expr)
                }
            }
            return out
        }
    }

    override fun toString(): String = expression

    override fun equals(other: Any?): Boolean = other is JsonPath && expression == other.expression

    override fun hashCode(): Int = expression.hashCode()
}

internal fun JsonPath.hasWildcards(): Boolean =
    segments.any {
        it is PathSeg.WildcardElem || it is PathSeg.WildcardField
    }
