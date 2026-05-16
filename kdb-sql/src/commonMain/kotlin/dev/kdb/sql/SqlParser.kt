package dev.kdb.sql

public interface SqlParser {
    public fun parse(sql: String): SqlStatement
}

public fun defaultSqlParser(): SqlParser = RecursiveDescentSqlParser()

internal class RecursiveDescentSqlParser : SqlParser {
    private lateinit var input: String
    private var pos = 0

    override fun parse(sql: String): SqlStatement {
        input = sql.trim()
        pos = 0
        skipWs()
        return when {
            matchKeyword("CREATE") -> parseCreateVirtualView()
            matchKeyword("DROP") -> parseDropVirtualView()
            matchKeyword("SELECT") -> SqlStatement.Select(parseSelectQuery())
            else -> throw parseError("expected SELECT or CREATE VIRTUAL VIEW")
        }
    }

    private fun parseCreateVirtualView(): SqlStatement.CreateVirtualView {
        expectKeyword("CREATE")
        expectKeyword("VIRTUAL")
        expectKeyword("VIEW")
        val name = readIdentifier()
        expectKeyword("AS")
        expectKeyword("SELECT")
        val query = parseSelectQuery()
        return SqlStatement.CreateVirtualView(name, query)
    }

    private fun parseDropVirtualView(): SqlStatement.DropVirtualView {
        expectKeyword("DROP")
        expectKeyword("VIRTUAL")
        expectKeyword("VIEW")
        return SqlStatement.DropVirtualView(readIdentifier())
    }

    private fun parseSelectQuery(): SelectQuery {
        val distinct = matchKeyword("DISTINCT")
        val projections = parseProjections()
        expectKeyword("FROM")
        val table = TableRef(readIdentifier(), null)
        skipWs()
        var where: SqlExpr? = null
        if (matchKeyword("WHERE")) {
            where = parseExpr()
        }
        val orderBy = mutableListOf<OrderItem>()
        if (matchKeyword("ORDER")) {
            expectKeyword("BY")
            do {
                val expr = parseExpr()
                val asc = !matchKeyword("DESC")
                if (!asc) {
                    // consumed DESC
                } else {
                    matchKeyword("ASC")
                }
                orderBy += OrderItem(expr, asc)
            } while (matchChar(','))
        }
        var limit: Int? = null
        var offset = 0
        if (matchKeyword("LIMIT")) {
            limit = readInt()
            if (matchKeyword("OFFSET")) {
                offset = readInt()
            }
        }
        return SelectQuery(distinct, projections, table, where, orderBy, limit, offset)
    }

    private fun parseProjections(): List<SelectProjection> {
        val out = mutableListOf<SelectProjection>()
        do {
            skipWs()
            if (peek() == '*') {
                consume()
                out += SelectProjection.Star()
            } else if (peekKeyword("kdb_json_") || peekKeyword("MATCH")) {
                val expr = parseExpr()
                val alias = parseOptionalAlias()
                out += SelectProjection.Expression(expr, alias)
            } else {
                val name = readIdentifier()
                val alias = parseOptionalAlias()
                if (alias != null || peek() != '.') {
                    out += SelectProjection.Column(name, alias)
                } else {
                    out += SelectProjection.Column(name, null)
                }
            }
        } while (matchChar(','))
        return out
    }

    private fun parseOptionalAlias(): String? {
        skipWs()
        if (matchKeyword("AS")) {
            return readIdentifier()
        }
        return null
    }

    private fun parseExpr(): SqlExpr {
        return parseOr()
    }

    private fun parseOr(): SqlExpr {
        var left = parseAnd()
        while (matchKeyword("OR")) {
            left = SqlExpr.Binary(BinaryOp.OR, left, parseAnd())
        }
        return left
    }

    private fun parseAnd(): SqlExpr {
        var left = parseComparison()
        while (matchKeyword("AND")) {
            left = SqlExpr.Binary(BinaryOp.AND, left, parseComparison())
        }
        return left
    }

    private fun parseComparison(): SqlExpr {
        if (matchKeyword("NOT")) {
            return SqlExpr.Unary(UnaryOp.NOT, parseComparison())
        }
        if (matchKeyword("MATCH")) {
            expectChar('(')
            val col = readIdentifier()
            expectChar(',')
            val q = readStringLiteral()
            expectChar(')')
            return SqlExpr.Match(col, q)
        }
        var left = parsePrimary()
        skipWs()
        val op =
            when {
                matchOp("=") -> BinaryOp.EQ
                matchOp("<>") || matchOp("!=") -> BinaryOp.NE
                matchOp("<=") -> BinaryOp.LE
                matchOp(">=") -> BinaryOp.GE
                matchOp("<") -> BinaryOp.LT
                matchOp(">") -> BinaryOp.GT
                matchKeyword("LIKE") -> BinaryOp.LIKE
                else -> return left
            }
        val right = parsePrimary()
        return SqlExpr.Binary(op, left, right)
    }

    private fun parsePrimary(): SqlExpr {
        skipWs()
        when {
            peek() == '\'' -> return SqlExpr.Literal(SqlCell.StringVal(readStringLiteral()))
            peek() == '?' -> {
                consume()
                return SqlExpr.Parameter(0)
            }
            peek().isDigit() -> {
                val num = readNumber()
                return if (num.contains('.')) {
                    SqlExpr.Literal(SqlCell.DoubleVal(num.toDouble()))
                } else {
                    SqlExpr.Literal(SqlCell.LongVal(num.toLong()))
                }
            }
            matchKeyword("NULL") -> return SqlExpr.Literal(SqlCell.Null)
            peek().isLetter() || peek() == '_' -> {
                val name = readIdentifier()
                if (peek() == '(') {
                    expectChar('(')
                    val args = mutableListOf<SqlExpr>()
                    if (peek() != ')') {
                        do {
                            args += parseExpr()
                        } while (matchChar(','))
                    }
                    expectChar(')')
                    return SqlExpr.FunctionCall(name.lowercase(), args)
                }
                return SqlExpr.ColumnRef(name)
            }
            peek() == '(' -> {
                expectChar('(')
                val e = parseExpr()
                expectChar(')')
                return e
            }
            else -> throw parseError("expected expression")
        }
    }

    private fun readStringLiteral(): String {
        expectChar('\'')
        val sb = StringBuilder()
        while (pos < input.length) {
            val c = input[pos++]
            if (c == '\'') {
                if (pos < input.length && input[pos] == '\'') {
                    sb.append('\'')
                    pos++
                } else {
                    break
                }
            } else {
                sb.append(c)
            }
        }
        return sb.toString()
    }

    private fun readNumber(): String {
        val start = pos
        while (pos < input.length && (input[pos].isDigit() || input[pos] == '.')) pos++
        return input.substring(start, pos)
    }

    private fun readInt(): Int = readNumber().toInt()

    private fun readIdentifier(): String {
        skipWs()
        val start = pos
        if (pos < input.length && (input[pos] == '_' || input[pos].isLetter())) {
            pos++
            while (pos < input.length && (input[pos].isLetterOrDigit() || input[pos] == '_')) pos++
        }
        if (start == pos) throw parseError("expected identifier")
        return input.substring(start, pos)
    }

    private fun expectKeyword(kw: String) {
        if (!matchKeyword(kw)) throw parseError("expected $kw")
    }

    private fun matchKeyword(kw: String): Boolean {
        skipWs()
        if (!input.regionMatches(pos, kw, 0, kw.length, ignoreCase = true)) return false
        if (pos + kw.length < input.length && input[pos + kw.length].isLetterOrDigit()) return false
        pos += kw.length
        return true
    }

    private fun peekKeyword(prefix: String): Boolean =
        input.regionMatches(pos, prefix, 0, prefix.length, ignoreCase = true)

    private fun matchOp(op: String): Boolean {
        skipWs()
        if (!input.regionMatches(pos, op, 0, op.length)) return false
        pos += op.length
        return true
    }

    private fun matchChar(c: Char): Boolean {
        skipWs()
        if (peek() != c) return false
        pos++
        return true
    }

    private fun expectChar(c: Char) {
        if (!matchChar(c)) throw parseError("expected '$c'")
    }

    private fun peek(): Char = if (pos < input.length) input[pos] else '\u0000'

    private fun consume() {
        pos++
    }

    private fun skipWs() {
        while (pos < input.length && input[pos].isWhitespace()) pos++
    }

    private fun parseError(msg: String): SqlParseException = SqlParseException(msg, input, pos)
}
