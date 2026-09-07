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
        // Malformed input must surface as SqlParseException, never as an IndexOutOfBounds /
        // NumberFormat / IllegalState from inside the descent (Layer 16 §3 item 4).
        val stmt =
            try {
                parseStatement()
            } catch (e: SqlParseException) {
                throw e
            } catch (e: SqlPlanningException) {
                throw e
            } catch (e: Exception) {
                throw SqlParseException(e.message ?: "malformed SQL", input, pos)
            }
        skipWs()
        if (matchChar(';')) skipWs()
        if (pos < input.length) throw parseError("unexpected input after statement")
        return stmt
    }

    private fun parseStatement(): SqlStatement {
        return when {
            matchKeyword("BEGIN") -> parseBegin()
            matchKeyword("START") -> parseStartTransaction()
            matchKeyword("COMMIT") -> parseCommit()
            matchKeyword("ROLLBACK") -> parseRollback()
            matchKeyword("CREATE") -> parseCreate()
            matchKeyword("ALTER") -> parseAlter()
            matchKeyword("DROP") -> parseDrop()
            matchKeyword("GRANT") -> SqlStatement.Grant(parseGrantSpec("TO"))
            matchKeyword("REVOKE") -> SqlStatement.Revoke(parseGrantSpec("FROM"))
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
        return when {
            matchKeyword("VIRTUAL") -> {
                expectKeyword("VIEW")
                val name = readIdentifier()
                expectKeyword("AS")
                expectKeyword("SELECT")
                SqlStatement.CreateVirtualView(name, parseSelectQuery())
            }
            matchKeyword("UNIQUE") -> {
                expectKeyword("INDEX")
                SqlStatement.CreateIndex(parseCreateIndexBody(uniquePrefix = true))
            }
            matchKeyword("INDEX") -> SqlStatement.CreateIndex(parseCreateIndexBody(uniquePrefix = false))
            matchKeyword("TABLE") -> SqlStatement.CreateTable(parseCreateTableBody())
            matchKeyword("ROLE") -> SqlStatement.CreateRole(readIdentifier())
            matchKeyword("USER") -> parseCreateUserBody()
            else -> throw parseError("expected VIRTUAL VIEW, [UNIQUE] INDEX, TABLE, ROLE, or USER")
        }
    }

    private fun parseCreateUserBody(): SqlStatement {
        val id = readIdentifier()
        expectKeyword("WITH")
        expectKeyword("PASSWORD")
        val password = readStringLiteral()
        val roles = mutableListOf<String>()
        if (matchKeyword("ROLES")) {
            expectChar('(')
            do {
                roles += readIdentifier()
            } while (matchChar(','))
            expectChar(')')
        }
        return SqlStatement.CreateUser(id, password, roles)
    }

    /** `<kind> ON DATABASE|COLLECTION|DOCUMENT <db>[.<collection>[.<documentId>]] TO|FROM <role>`
     * — [terminator] is `"TO"` for GRANT, `"FROM"` for REVOKE. */
    private fun parseGrantSpec(terminator: String): GrantSpec {
        val kind = readIdentifier().lowercase()
        expectKeyword("ON")
        val expectedSegments =
            when {
                matchKeyword("DATABASE") -> 1
                matchKeyword("COLLECTION") -> 2
                matchKeyword("DOCUMENT") -> 3
                else -> throw parseError("expected DATABASE, COLLECTION, or DOCUMENT")
            }
        val segments = mutableListOf<String>()
        segments += readResourcePathSegment()
        while (matchChar('.')) {
            segments += readResourcePathSegment()
        }
        if (segments.size != expectedSegments) {
            throw parseError(
                "expected $expectedSegments dot-separated path segment(s), got ${segments.size}",
            )
        }
        expectKeyword(terminator)
        val role = readIdentifier()
        return GrantSpec(
            kind = kind,
            database = segments[0],
            collection = segments.getOrNull(1),
            documentId = segments.getOrNull(2),
            role = role,
        )
    }

    /** Like [readIdentifier] but also accepts `-` so a document id (typically a UUID) parses. */
    private fun readResourcePathSegment(): String {
        skipWs()
        val start = pos
        while (pos < input.length && (input[pos].isLetterOrDigit() || input[pos] == '_' || input[pos] == '-')) pos++
        if (start == pos) throw parseError("expected resource path segment")
        return input.substring(start, pos)
    }

    private fun parseCreateTableBody(): CreateTableStatement {
        val table = TableRef(readIdentifier())
        expectChar('(')
        val columns = mutableListOf<ColumnDefinition>()
        val constraints = mutableListOf<List<String>>()
        do {
            if (matchKeyword("UNIQUE")) {
                expectChar('(')
                val fields = mutableListOf<String>()
                do {
                    fields += readIdentifier()
                } while (matchChar(','))
                expectChar(')')
                constraints += fields
            } else {
                columns += parseColumnDefinition()
            }
        } while (matchChar(','))
        expectChar(')')
        if (columns.isEmpty()) throw parseError("CREATE TABLE needs at least one column")
        return CreateTableStatement(table, columns, constraints)
    }

    private fun parseColumnDefinition(): ColumnDefinition {
        val name = readIdentifier()
        val type = parseColumnType()
        var required = false
        var unique = false
        while (true) {
            when {
                matchKeyword("NOT") -> {
                    expectKeyword("NULL")
                    required = true
                }
                matchKeyword("UNIQUE") -> unique = true
                else -> break
            }
        }
        return ColumnDefinition(name, type, required, indexed = true, unique = unique)
    }

    private fun parseColumnType(): dev.kdb.schema.KdbFieldType {
        val typeName = readIdentifier().uppercase()
        if (peek() == '(') {
            expectChar('(')
            readNumber()
            expectChar(')')
        }
        return when (typeName) {
            "VARCHAR", "TEXT", "STRING", "CHAR" -> dev.kdb.schema.KdbFieldType.StringType
            "INT", "INTEGER" -> dev.kdb.schema.KdbFieldType.Int32Type
            "BIGINT", "LONG" -> dev.kdb.schema.KdbFieldType.Int64Type
            "DOUBLE", "FLOAT", "REAL" -> dev.kdb.schema.KdbFieldType.Float64Type
            "BOOLEAN", "BOOL" -> dev.kdb.schema.KdbFieldType.BoolType
            "TIMESTAMP", "DATETIME" -> dev.kdb.schema.KdbFieldType.TimestampType
            "UUID" -> dev.kdb.schema.KdbFieldType.UuidType
            else -> throw parseError("unsupported column type: $typeName")
        }
    }

    /**
     * `name ON t (f1 [WEIGHT n] {, f2 [WEIGHT n]}) [USING HASH|BTREE|FULLTEXT|VECTOR]
     * [WITH (k = v {, k = v})] [UNIQUE]` (Layer 16 §9.2). Fields are dotted JSON paths.
     */
    private fun parseCreateIndexBody(uniquePrefix: Boolean): CreateIndexStatement {
        val indexName = readIdentifier()
        expectKeyword("ON")
        val table = readIdentifier()
        expectChar('(')
        val fields = mutableListOf<String>()
        val weights = LinkedHashMap<String, Int>()
        do {
            val field = readDottedIdentifier()
            if (field in fields) throw parseError("duplicate index field: $field")
            fields += field
            if (matchKeyword("WEIGHT")) {
                val w = readInt()
                if (w < 1) throw parseError("WEIGHT must be >= 1")
                weights[field] = w
            }
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
        val options = LinkedHashMap<String, String>()
        if (matchKeyword("WITH")) {
            expectChar('(')
            do {
                val key = readIdentifier().lowercase()
                expectChar('=')
                skipWs()
                val value =
                    when {
                        peek() == '\'' -> readStringLiteral()
                        peek().isDigit() || peek() == '-' -> readNumber()
                        else -> readIdentifier()
                    }
                if (key in options) throw parseError("duplicate option: $key")
                options[key] = value
            } while (matchChar(','))
            expectChar(')')
        }
        val unique = matchKeyword("UNIQUE") || uniquePrefix
        if (weights.isNotEmpty() && type != dev.kdb.index.IndexType.FULLTEXT) {
            throw parseError("WEIGHT is only valid for FULLTEXT indexes")
        }
        return CreateIndexStatement(indexName, table, fields, type, unique, weights, options)
    }

    private fun parseDropIndexBody(): DropIndexStatement {
        val indexName = readIdentifier()
        expectKeyword("ON")
        val table = readIdentifier()
        return DropIndexStatement(indexName, table)
    }

    private fun parseDrop(): SqlStatement {
        return when {
            matchKeyword("VIRTUAL") -> {
                expectKeyword("VIEW")
                SqlStatement.DropVirtualView(readIdentifier())
            }
            matchKeyword("INDEX") -> SqlStatement.DropIndex(parseDropIndexBody())
            matchKeyword("TABLE") -> SqlStatement.DropTable(TableRef(readIdentifier()))
            matchKeyword("ROLE") -> SqlStatement.DropRole(readIdentifier())
            matchKeyword("USER") -> SqlStatement.DropUser(readIdentifier())
            else -> throw parseError("expected VIRTUAL VIEW, INDEX, TABLE, ROLE, or USER")
        }
    }

    private fun parseAlter(): SqlStatement {
        expectKeyword("TABLE")
        val table = TableRef(readIdentifier())
        expectKeyword("ADD")
        expectKeyword("COLUMN")
        val column = parseColumnDefinition()
        return SqlStatement.AlterTableAddColumn(AlterTableAddColumnStatement(table, column))
    }

    private fun parseUpdate(): UpdateStatement {
        val table = TableRef(readIdentifier(), parseOptionalTableAlias())
        expectKeyword("SET")
        val assignments = mutableListOf<Assignment>()
        do {
            val col = readDottedIdentifier()
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
        expectChar('(')
        val columns = mutableListOf<String>()
        do {
            columns += readIdentifier()
        } while (matchChar(','))
        expectChar(')')
        expectKeyword("VALUES")
        val rows = mutableListOf<List<SqlExpr>>()
        do {
            expectChar('(')
            val values = mutableListOf<SqlExpr>()
            do {
                values += parseExpr()
            } while (matchChar(','))
            expectChar(')')
            rows += values
        } while (matchChar(','))
        return InsertStatement(table, columns, rows)
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

    private fun parseOrderExpr(): SqlExpr = parseExpr()

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
            id.equals("LIKE", ignoreCase = true) ||
            id.equals("ILIKE", ignoreCase = true) ||
            id.equals("WITH", ignoreCase = true) ||
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
            } else if (peekKeyword("kdb_json_")) {
                val expr = parseExpr()
                val alias = parseOptionalAlias()
                out += SelectProjection.Expression(expr, alias)
            } else {
                val mark = pos
                val name = readIdentifier()
                if (peek() == '.') {
                    expectChar('.')
                    val rest = readDottedIdentifier()
                    out += SelectProjection.Expression(SqlExpr.QualifiedColumn(name, rest), parseOptionalAlias())
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

    /**
     * `cmp := operand ( ( = | <> | != | < | <= | > | >= ) operand | [NOT] LIKE s | [NOT] ILIKE s
     * | [NOT] IN (…) | [NOT] BETWEEN a AND b | IS [NOT] NULL )?` (Layer 16 §4).
     */
    private fun parseComparison(): SqlExpr {
        if (matchKeyword("NOT")) {
            return SqlExpr.Unary(UnaryOp.NOT, parseComparison())
        }
        val left = parsePrimary()
        skipWs()
        val negated = matchKeyword("NOT")
        val columnPath = columnPathOf(left)
        if (matchKeyword("IN")) {
            if (columnPath == null) throw parseError("IN requires a column on the left")
            expectChar('(')
            if (matchKeyword("SELECT")) {
                throw parseError("subquery IN is not supported in v1")
            }
            val values = mutableListOf<SqlExpr>()
            do {
                values += parseExpr()
            } while (matchChar(','))
            expectChar(')')
            return SqlExpr.InList(columnPath, values, negated)
        }
        if (matchKeyword("BETWEEN")) {
            if (columnPath == null) throw parseError("BETWEEN requires a column on the left")
            val low = parsePrimary()
            expectKeyword("AND")
            val high = parsePrimary()
            return SqlExpr.Between(columnPath, low, high, negated)
        }
        if (matchKeyword("LIKE")) {
            return SqlExpr.Binary(if (negated) BinaryOp.NOT_LIKE else BinaryOp.LIKE, left, parsePrimary())
        }
        if (matchKeyword("ILIKE")) {
            return SqlExpr.Binary(if (negated) BinaryOp.NOT_ILIKE else BinaryOp.ILIKE, left, parsePrimary())
        }
        if (negated) throw parseError("expected LIKE, ILIKE, IN, or BETWEEN after NOT")
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
                else -> return left
            }
        val right = parsePrimary()
        return SqlExpr.Binary(op, left, right)
    }

    /** The dotted path of a column operand (`a` or `t.a.b`), or null for any other expression. */
    private fun columnPathOf(expr: SqlExpr): String? =
        when (expr) {
            is SqlExpr.ColumnRef -> expr.name
            is SqlExpr.QualifiedColumn -> "${expr.qualifier}.${expr.name}"
            else -> null
        }

    private fun parsePrimary(): SqlExpr {
        skipWs()
        when {
            peek() == '\'' -> return SqlExpr.Literal(SqlCell.StringVal(readStringLiteral()))
            peek() == '?' -> {
                consume()
                return SqlExpr.Parameter(nextParamIndex++)
            }
            peek().isDigit() || (peek() == '-' && peekAt(1).isDigit()) -> return SqlExpr.Literal(numberLiteral(readNumber()))
            peek() == '[' -> return parseVectorLiteral()
            matchKeyword("NULL") -> return SqlExpr.Literal(SqlCell.Null)
            matchKeyword("TRUE") -> return SqlExpr.Literal(SqlCell.BoolVal(true))
            matchKeyword("FALSE") -> return SqlExpr.Literal(SqlCell.BoolVal(false))
            peek().isLetter() || peek() == '_' -> {
                val name = readIdentifier()
                if (peek() == '(') {
                    when (name.uppercase()) {
                        "MATCH" -> return parseMatchCall()
                        "SIMILARITY" -> return parseSimilarityCall()
                        "FUSE" -> return parseFuseCall()
                    }
                    expectChar('(')
                    val args = mutableListOf<SqlExpr>()
                    skipWs()
                    if (peek() != ')') {
                        do {
                            skipWs()
                            if (peek() == '*' && name.equals("count", ignoreCase = true)) {
                                consume()
                                args += SqlExpr.ColumnRef("*")
                            } else {
                                args += parseExpr()
                            }
                        } while (matchChar(','))
                    }
                    expectChar(')')
                    return SqlExpr.FunctionCall(name.lowercase(), args)
                }
                if (peek() == '.') {
                    expectChar('.')
                    val rest = readDottedIdentifier()
                    return SqlExpr.QualifiedColumn(name, rest)
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

    /** `MATCH(index_or_field, 'query' | ?)` (Layer 16 §9.1). */
    private fun parseMatchCall(): SqlExpr {
        expectChar('(')
        val target = readDottedIdentifier()
        expectChar(',')
        val query = parsePrimary()
        if (query !is SqlExpr.Parameter && !(query is SqlExpr.Literal && query.cell is SqlCell.StringVal)) {
            throw parseError("MATCH query must be a string literal or a parameter")
        }
        expectChar(')')
        return SqlExpr.Match(target, query)
    }

    /** `SIMILARITY(field, ? | [v1, v2, …])` (Layer 16 §9.1). */
    private fun parseSimilarityCall(): SqlExpr {
        expectChar('(')
        val field = readDottedIdentifier()
        expectChar(',')
        val vector = parsePrimary()
        if (vector !is SqlExpr.Parameter && vector !is SqlExpr.VectorLiteral) {
            throw parseError("SIMILARITY vector must be a parameter or a vector literal")
        }
        expectChar(')')
        return SqlExpr.Similarity(field, vector)
    }

    /** `FUSE(arm, arm {, arm} [, 'rrf' | 'weighted'])` (Layer 16 §9.1). */
    private fun parseFuseCall(): SqlExpr {
        expectChar('(')
        val arms = mutableListOf<SqlExpr>()
        var mode = "rrf"
        do {
            skipWs()
            if (peek() == '\'') {
                mode = readStringLiteral()
                if (matchChar(',')) throw parseError("FUSE mode must be the last argument")
                break
            }
            val arm = parsePrimary()
            if (arm !is SqlExpr.Match && arm !is SqlExpr.Similarity) {
                throw parseError("FUSE arms must be MATCH or SIMILARITY calls")
            }
            arms += arm
        } while (matchChar(','))
        expectChar(')')
        if (arms.size < 2) throw parseError("FUSE needs at least two arms")
        return SqlExpr.Fuse(arms, mode.lowercase())
    }

    private fun parseVectorLiteral(): SqlExpr {
        expectChar('[')
        val values = mutableListOf<Double>()
        skipWs()
        if (peek() != ']') {
            do {
                skipWs()
                if (!(peek().isDigit() || (peek() == '-' && peekAt(1).isDigit()))) {
                    throw parseError("expected number in vector literal")
                }
                values += readNumber().toDouble()
            } while (matchChar(','))
        }
        expectChar(']')
        if (values.isEmpty()) throw parseError("vector literal must not be empty")
        return SqlExpr.VectorLiteral(values)
    }

    private fun numberLiteral(text: String): SqlCell {
        val isDouble = text.any { it == '.' || it == 'e' || it == 'E' }
        if (!isDouble) {
            text.toLongOrNull()?.let { return SqlCell.LongVal(it) }
        }
        return SqlCell.DoubleVal(text.toDoubleOrNull() ?: throw parseError("malformed number: $text"))
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
                    return sb.toString()
                }
            } else {
                sb.append(c)
            }
        }
        throw parseError("unterminated string literal")
    }

    /** `[-]digits[.digits][(e|E)[+-]digits]`; the caller decides integer vs double. */
    private fun readNumber(): String {
        skipWs()
        val start = pos
        if (peek() == '-') pos++
        val intStart = pos
        while (pos < input.length && input[pos].isDigit()) pos++
        if (pos == intStart) throw parseError("expected number")
        if (peek() == '.' && peekAt(1).isDigit()) {
            pos++
            while (pos < input.length && input[pos].isDigit()) pos++
        } else if (peek() == '.') {
            throw parseError("malformed number")
        }
        if (peek() == 'e' || peek() == 'E') {
            pos++
            if (peek() == '+' || peek() == '-') pos++
            val expStart = pos
            while (pos < input.length && input[pos].isDigit()) pos++
            if (pos == expStart) throw parseError("malformed exponent")
        }
        return input.substring(start, pos)
    }

    private fun readInt(): Int {
        val n = readNumber()
        return n.toIntOrNull() ?: throw parseError("expected integer, got $n")
    }

    /** `a` or `a.b.c` — a column path (Layer 16 §2) or a dotted index field. */
    private fun readDottedIdentifier(): String {
        val sb = StringBuilder(readIdentifier())
        while (peek() == '.') {
            pos++
            sb.append('.').append(readIdentifier())
        }
        return sb.toString()
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

    private fun peekAt(offset: Int): Char = if (pos + offset < input.length) input[pos + offset] else '\u0000'

    private fun consume() {
        pos++
    }

    private fun skipWs() {
        while (pos < input.length && input[pos].isWhitespace()) pos++
    }

    private fun parseError(msg: String): SqlParseException = SqlParseException(msg, input, pos)
}
