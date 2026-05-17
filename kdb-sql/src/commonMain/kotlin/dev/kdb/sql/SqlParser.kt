package dev.kdb.sql

public interface SqlParser {
    public fun parse(sql: String): SqlStatement
}

public fun defaultSqlParser(): SqlParser = RecursiveDescentSqlParser()

internal class RecursiveDescentSqlParser : SqlParser {
    private lateinit var input: String
    private var pos = 0
    private var nextParamIndex = 0

    override fun parse(sql: String): SqlStatement {
        input = sql.trim()
        pos = 0
        nextParamIndex = 0
        skipWs()
        return when {
            matchKeyword("BEGIN") -> parseBegin()
            matchKeyword("START") -> parseStartTransaction()
            matchKeyword("COMMIT") -> parseCommit()
            matchKeyword("ROLLBACK") -> parseRollback()
            matchKeyword("CREATE") -> parseCreate()
            matchKeyword("DROP") -> parseDrop()
            matchKeyword("INSERT") -> SqlStatement.Insert(parseInsert())
            matchKeyword("UPDATE") -> SqlStatement.Update(parseUpdate())
            matchKeyword("DELETE") -> SqlStatement.Delete(parseDelete())
            matchKeyword("SELECT") -> SqlStatement.Select(parseSelectQuery())
            else -> throw parseError("expected SQL statement")
        }
    }

    private fun parseBegin(): SqlStatement {
        if (matchKeyword("WORK")) { /* optional */ }
        return SqlStatement.BeginTransaction
    }

    private fun parseStartTransaction(): SqlStatement {
        expectKeyword("TRANSACTION")
        return SqlStatement.BeginTransaction
    }

    private fun parseCommit(): SqlStatement {
        if (matchKeyword("WORK")) { /* optional */ }
        return SqlStatement.Commit
    }

    private fun parseRollback(): SqlStatement {
        if (matchKeyword("WORK")) { /* optional */ }
        return SqlStatement.Rollback
    }

    private fun parseCreate(): SqlStatement {
        expectKeyword("CREATE")
        return when {
            matchKeyword("VIRTUAL") -> {
                expectKeyword("VIEW")
                val name = readIdentifier()
                expectKeyword("AS")
                expectKeyword("SELECT")
                SqlStatement.CreateVirtualView(name, parseSelectQuery())
            }
            matchKeyword("INDEX") -> {
                expectKeyword("INDEX")
                SqlStatement.CreateIndex(parseCreateIndexBody())
            }
            else -> throw parseError("expected VIRTUAL VIEW or INDEX")
        }
    }

    private fun parseCreateIndexBody(): CreateIndexStatement {
        val indexName = readIdentifier()
        expectKeyword("ON")
        val table = readIdentifier()
        expectChar('(')
        val fields = mutableListOf<String>()
        do {
            fields += readIdentifier()
        } while (matchChar(','))
        expectChar(')')
        var type = dev.kdb.index.IndexType.BTREE
        if (matchKeyword("USING")) {
            type =
                when (readIdentifier().uppercase()) {
                    "HASH" -> dev.kdb.index.IndexType.HASH
                    "BTREE" -> dev.kdb.index.IndexType.BTREE
                    "FULLTEXT" -> dev.kdb.index.IndexType.FULLTEXT
                    "VECTOR" -> dev.kdb.index.IndexType.VECTOR
                    else -> throw parseError("unknown index type")
                }
        }
        val unique = matchKeyword("UNIQUE")
        return CreateIndexStatement(indexName, table, fields, type, unique)
    }

    private fun parseDropIndexBody(): DropIndexStatement {
        val indexName = readIdentifier()
        expectKeyword("ON")
        val table = readIdentifier()
        return DropIndexStatement(indexName, table)
    }

    private fun parseDrop(): SqlStatement {
        expectKeyword("DROP")
        return when {
            matchKeyword("VIRTUAL") -> {
                expectKeyword("VIEW")
                SqlStatement.DropVirtualView(readIdentifier())
            }
            matchKeyword("INDEX") -> {
                expectKeyword("INDEX")
                SqlStatement.DropIndex(parseDropIndexBody())
            }
            else -> throw parseError("expected VIRTUAL VIEW or INDEX")
        }
    }

    private fun parseUpdate(): UpdateStatement {
        val table = TableRef(readIdentifier(), parseOptionalTableAlias())
        expectKeyword("SET")
        val assignments = mutableListOf<Assignment>()
        do {
            val col = readIdentifier()
            expectChar('=')
            assignments += Assignment(col, parseExpr())
        } while (matchChar(','))
        var where: SqlExpr? = null
        if (matchKeyword("WHERE")) {
            where = parseExpr()
        }
        return UpdateStatement(table, assignments, where)
    }

    private fun parseInsert(): InsertStatement {
        expectKeyword("INTO")
        val table = TableRef(readIdentifier(), parseOptionalTableAlias())
        expectKeyword("(")
        val columns = mutableListOf<String>()
        do {
            columns += readIdentifier()
        } while (matchChar(','))
        expectChar(')')
        expectKeyword("VALUES")
        expectChar('(')
        val values = mutableListOf<SqlExpr>()
        do {
            values += parseExpr()
        } while (matchChar(','))
        expectChar(')')
        return InsertStatement(table, columns, values)
    }

    private fun parseDelete(): DeleteStatement {
        expectKeyword("FROM")
        val table = TableRef(readIdentifier(), parseOptionalTableAlias())
        var where: SqlExpr? = null
        if (matchKeyword("WHERE")) {
            where = parseExpr()
        }
        return DeleteStatement(table, where)
    }

    private fun parseSelectQuery(): SelectQuery {
        val distinct = matchKeyword("DISTINCT")
        val projections = parseProjections()
        expectKeyword("FROM")
        val table = TableRef(readIdentifier(), parseOptionalTableAlias())
        val joins = mutableListOf<JoinClause>()
        while (matchKeyword("INNER")) {
            expectKeyword("JOIN")
            val joinTable = TableRef(readIdentifier(), parseOptionalTableAlias())
            expectKeyword("ON")
            val on = parseExpr()
            joins += JoinClause(JoinType.INNER, joinTable, on)
        }
        skipWs()
        var where: SqlExpr? = null
        if (matchKeyword("WHERE")) {
            where = parseExpr()
        }
        val groupBy = mutableListOf<SqlExpr>()
        if (matchKeyword("GROUP")) {
            expectKeyword("BY")
            do {
                groupBy += parseExpr()
            } while (matchChar(','))
        }
        val orderBy = mutableListOf<OrderItem>()
        if (matchKeyword("ORDER")) {
            expectKeyword("BY")
            do {
                val expr = parseOrderExpr()
                skipWs()
                val ascending =
                    when {
                        matchKeyword("DESC") -> false
                        matchKeyword("ASC") -> true
                        else -> true
                    }
                orderBy += OrderItem(expr, ascending)
            } while (matchChar(','))
        }
        var limit: Int? = null
        var offset = 0
        if (matchKeyword("LIMIT")) {
            limit = readInt()
        }
        if (matchKeyword("OFFSET")) {
            offset = readInt()
        }
        return SelectQuery(distinct, projections, table, joins, where, groupBy, orderBy, limit, offset)
    }

    private fun parseOrderExpr(): SqlExpr {
        if (matchKeyword("SIMILARITY")) {
            expectChar('(')
            val col = readIdentifier()
            expectChar(',')
            if (peek() != '\'') {
                throw parseError("similarity text query requires embedding (not yet available)")
            }
            val q = readStringLiteral()
            expectChar(')')
            return SqlExpr.Similarity(col, q, null)
        }
        return parseExpr()
    }

    private fun parseOptionalTableAlias(): String? {
        skipWs()
        if (!isTableAliasStart()) return null
        val mark = pos
        val id = readIdentifier()
        if (isReservedTableKeyword(id)) {
            pos = mark
            return null
        }
        return id
    }

    private fun isTableAliasStart(): Boolean {
        val c = peek()
        return c.isLetter() || c == '_'
    }

    private fun isReservedTableKeyword(id: String): Boolean =
        id.equals("WHERE", ignoreCase = true) ||
            id.equals("SET", ignoreCase = true) ||
            id.equals("ORDER", ignoreCase = true) ||
            id.equals("BY", ignoreCase = true) ||
            id.equals("LIMIT", ignoreCase = true) ||
            id.equals("OFFSET", ignoreCase = true) ||
            id.equals("VALUES", ignoreCase = true) ||
            id.equals("AND", ignoreCase = true) ||
            id.equals("OR", ignoreCase = true) ||
            id.equals("NOT", ignoreCase = true) ||
            id.equals("BETWEEN", ignoreCase = true) ||
            id.equals("IS", ignoreCase = true) ||
            id.equals("NULL", ignoreCase = true) ||
            id.equals("INTO", ignoreCase = true) ||
            id.equals("FROM", ignoreCase = true) ||
            id.equals("SELECT", ignoreCase = true) ||
            id.equals("AS", ignoreCase = true) ||
            id.equals("ON", ignoreCase = true) ||
            id.equals("USING", ignoreCase = true) ||
            id.equals("UNIQUE", ignoreCase = true) ||
            id.equals("GROUP", ignoreCase = true) ||
            id.equals("INNER", ignoreCase = true) ||
            id.equals("JOIN", ignoreCase = true) ||
            id.equals("IN", ignoreCase = true) ||
            id.equals("HAVING", ignoreCase = true)

    private fun parseProjections(): List<SelectProjection> {
        val out = mutableListOf<SelectProjection>()
        do {
            skipWs()
            if (peek() == '*') {
                consume()
                out += SelectProjection.Star()
            } else if (matchKeyword("COUNT")) {
                expectChar('(')
                skipWs()
                val arg =
                    if (peek() == '*') {
                        consume()
                        SqlExpr.ColumnRef("*")
                    } else {
                        parseExpr()
                    }
                expectChar(')')
                out += SelectProjection.Expression(SqlExpr.FunctionCall("count", listOf(arg)), parseOptionalAlias())
            } else if (peekKeyword("kdb_json_") || peekKeyword("MATCH")) {
                val expr = parseExpr()
                val alias = parseOptionalAlias()
                out += SelectProjection.Expression(expr, alias)
            } else {
                val mark = pos
                val name = readIdentifier()
                if (peek() == '.') {
                    expectChar('.')
                    val field = readIdentifier()
                    out += SelectProjection.Expression(SqlExpr.QualifiedColumn(name, field), parseOptionalAlias())
                } else if (peek() == '(') {
                    pos = mark
                    val expr = parseExpr()
                    out += SelectProjection.Expression(expr, parseOptionalAlias())
                } else {
                    val alias = parseOptionalAlias()
                    out += SelectProjection.Column(name, alias)
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

    private fun parseExpr(): SqlExpr = parseOr()

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
        if (left is SqlExpr.ColumnRef && matchKeyword("IN")) {
            expectChar('(')
            if (matchKeyword("SELECT")) {
                throw parseError("subquery IN is not supported in v1")
            }
            val values = mutableListOf<SqlExpr>()
            do {
                values += parseExpr()
            } while (matchChar(','))
            expectChar(')')
            return SqlExpr.InList(left.name, values)
        }
        if (left is SqlExpr.ColumnRef && matchKeyword("BETWEEN")) {
            val low = parsePrimary()
            expectKeyword("AND")
            val high = parsePrimary()
            return SqlExpr.Between(left.name, low, high)
        }
        if (matchKeyword("IS")) {
            if (matchKeyword("NOT")) {
                expectKeyword("NULL")
                return SqlExpr.Unary(UnaryOp.NOT, SqlExpr.Unary(UnaryOp.IS_NULL, left))
            }
            expectKeyword("NULL")
            return SqlExpr.Unary(UnaryOp.IS_NULL, left)
        }
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
                return SqlExpr.Parameter(nextParamIndex++)
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
                if (peek() == '.') {
                    expectChar('.')
                    val field = readIdentifier()
                    return SqlExpr.QualifiedColumn(name, field)
                }
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
        skipWs()
        val start = pos
        while (pos < input.length && (input[pos].isDigit() || input[pos] == '.')) pos++
        return input.substring(start, pos)
    }

    private fun readInt(): Int {
        val n = readNumber()
        if (n.isEmpty()) throw parseError("expected integer")
        return n.toInt()
    }

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
