package dev.kdb.sql

import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbDocument
import dev.kdb.json.JsonValue
import dev.kdb.json.KdbJsonFunctionRegistry
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.isNone

/**
 * Everything one SELECT/UPDATE/DELETE needs to evaluate expressions against documents
 * (Layer 16 §2): the schema (Rule 1 vs Rule 2), bound parameters, the FROM table's name and
 * alias (stripped from qualified column paths), projection aliases (for ORDER BY / GROUP BY),
 * and the rankings behind every `MATCH`/`SIMILARITY`/`FUSE` the statement contains.
 */
internal class EvalEnv(
    val schema: KdbSchema,
    val parameters: List<SqlParameter> = emptyList(),
    tableQualifiers: Collection<String> = emptyList(),
    val aliases: Map<String, SqlExpr> = emptyMap(),
    val scores: Map<SqlExpr, Map<KdbUuid, Float>> = emptyMap(),
) {
    private val qualifiers = tableQualifiers.map { it.lowercase() }.toSet()
    private val parsed = HashMap<KdbUuid, JsonValue?>()

    fun root(doc: KdbDocument): JsonValue? =
        parsed.getOrPut(doc.id) {
            try {
                JsonValue.fromJsonString(doc.json)
            } catch (_: Exception) {
                null
            }
        }

    /** Strips a leading table/alias qualifier: `t.a.b` → `a.b` when `t` names the FROM table. */
    fun resolvePath(name: String): String {
        val dot = name.indexOf('.')
        if (dot <= 0) return name
        val head = name.substring(0, dot).lowercase()
        return if (head in qualifiers) name.substring(dot + 1) else name
    }

    fun withScores(scores: Map<SqlExpr, Map<KdbUuid, Float>>): EvalEnv =
        EvalEnv(schema, parameters, qualifiers, aliases, scores)
}

/** Cell/predicate evaluation over one document (Layer 16 §2, §4). */
internal object SqlPredicate {
    const val KDB_ID: String = "kdb_id"
    const val DOC: String = "_doc"

    fun isReserved(name: String): Boolean = name == KDB_ID || name == DOC

    // ---------------------------------------------------------------- predicates

    fun eval(
        expr: SqlExpr,
        doc: KdbDocument,
        env: EvalEnv,
    ): Boolean =
        when (expr) {
            is SqlExpr.Binary -> evalBinary(expr, doc, env)
            is SqlExpr.Unary ->
                when (expr.op) {
                    UnaryOp.NOT -> !eval(expr.expr, doc, env)
                    UnaryOp.IS_NULL -> evalCell(expr.expr, doc, env) is SqlCell.Null
                }
            is SqlExpr.Match -> scoreOf(expr, doc, env) != null
            is SqlExpr.Between -> evalBetween(expr, doc, env)
            is SqlExpr.InList -> evalInList(expr, doc, env)
            is SqlExpr.ColumnRef -> candidatesOf(expr.name, doc, env).any { it is JsonValue.JBool && it.value }
            is SqlExpr.QualifiedColumn ->
                candidatesOf("${expr.qualifier}.${expr.name}", doc, env).any { it is JsonValue.JBool && it.value }
            is SqlExpr.FunctionCall ->
                when (val cell = evalFunction(expr, doc, env)) {
                    is SqlCell.BoolVal -> cell.value
                    is SqlCell.Null -> false
                    else -> throw SqlPlanningException("function ${expr.name} is not a boolean predicate", "")
                }
            is SqlExpr.Literal ->
                (expr.cell as? SqlCell.BoolVal)?.value
                    ?: throw SqlPlanningException("literal ${expr.cell} is not a boolean predicate", "")
            is SqlExpr.Parameter ->
                (parameterToCell(env.parameters.getOrNull(expr.index)) as? SqlCell.BoolVal)?.value
                    ?: throw SqlPlanningException("parameter ${expr.index} is not a boolean predicate", "")
            is SqlExpr.Similarity, is SqlExpr.Fuse, is SqlExpr.VectorLiteral ->
                throw SqlPlanningException("unsupported predicate expression: ${expr::class.simpleName}", "")
        }

    private fun evalBinary(
        expr: SqlExpr.Binary,
        doc: KdbDocument,
        env: EvalEnv,
    ): Boolean =
        when (expr.op) {
            BinaryOp.AND -> eval(expr.left, doc, env) && eval(expr.right, doc, env)
            BinaryOp.OR -> eval(expr.left, doc, env) || eval(expr.right, doc, env)
            BinaryOp.LIKE, BinaryOp.NOT_LIKE, BinaryOp.ILIKE, BinaryOp.NOT_ILIKE -> evalLike(expr, doc, env)
            BinaryOp.EQ, BinaryOp.NE, BinaryOp.LT, BinaryOp.LE, BinaryOp.GT, BinaryOp.GE -> {
                val lefts = candidateCells(expr.left, doc, env)
                val rights = candidateCells(expr.right, doc, env)
                if (expr.op == BinaryOp.NE) {
                    // `<>` is true when no candidate pair is equal, incomparable pairs included;
                    // a NULL side is unknown → false.
                    if (lefts.all { it is SqlCell.Null } || rights.all { it is SqlCell.Null }) {
                        false
                    } else {
                        lefts.none { l -> rights.any { r -> compareForPredicate(l, r) == 0 } }
                    }
                } else {
                    lefts.any { l -> rights.any { r -> compareOp(expr.op, l, r) } }
                }
            }
        }

    /** Ordering/equality over two cells per §2: null → unknown → false; incomparable → false. */
    fun compareOp(
        op: BinaryOp,
        left: SqlCell?,
        right: SqlCell?,
    ): Boolean {
        if (op == BinaryOp.NE) {
            if (left == null || right == null || left is SqlCell.Null || right is SqlCell.Null) return false
            return compareForPredicate(left, right) != 0
        }
        val cmp = compareForPredicate(left, right) ?: return false
        return when (op) {
            BinaryOp.EQ -> cmp == 0
            BinaryOp.LT -> cmp < 0
            BinaryOp.LE -> cmp <= 0
            BinaryOp.GT -> cmp > 0
            BinaryOp.GE -> cmp >= 0
            else -> false
        }
    }

    private fun evalBetween(
        expr: SqlExpr.Between,
        doc: KdbDocument,
        env: EvalEnv,
    ): Boolean {
        val low = evalCell(expr.low, doc, env)
        val high = evalCell(expr.high, doc, env)
        val hit =
            candidateCells(SqlExpr.ColumnRef(expr.column), doc, env).any { c ->
                compareOp(BinaryOp.GE, c, low) && compareOp(BinaryOp.LE, c, high)
            }
        return if (expr.negated) !hit else hit
    }

    private fun evalInList(
        expr: SqlExpr.InList,
        doc: KdbDocument,
        env: EvalEnv,
    ): Boolean {
        val cells = candidateCells(SqlExpr.ColumnRef(expr.column), doc, env)
        val hit =
            expr.values.any { valueExpr ->
                val expected = evalCell(valueExpr, doc, env)
                cells.any { compareForPredicate(it, expected) == 0 }
            }
        return if (expr.negated) !hit else hit
    }

    private fun evalLike(
        expr: SqlExpr.Binary,
        doc: KdbDocument,
        env: EvalEnv,
    ): Boolean {
        val pattern = (evalCell(expr.right, doc, env) as? SqlCell.StringVal)?.value ?: return false
        val ignoreCase = expr.op == BinaryOp.ILIKE || expr.op == BinaryOp.NOT_ILIKE
        val regex = likeRegex(pattern, ignoreCase)
        val hit =
            candidateCells(expr.left, doc, env).any { c -> c is SqlCell.StringVal && regex.matches(c.value) }
        return if (expr.op == BinaryOp.NOT_LIKE || expr.op == BinaryOp.NOT_ILIKE) !hit else hit
    }

    /** `%` → any run, `_` → one character, everything else literal (Layer 16 §4). */
    fun likeRegex(
        pattern: String,
        ignoreCase: Boolean,
    ): Regex {
        val sb = StringBuilder("^")
        for (ch in pattern) {
            when (ch) {
                '%' -> sb.append("[\\s\\S]*")
                '_' -> sb.append("[\\s\\S]")
                // Escaped by hand rather than with Regex.escape: that emits \Q...\E, which the
                // JS regex engine does not understand, so the same pattern would behave
                // differently per target.
                in REGEX_META -> sb.append('\\').append(ch)
                else -> sb.append(ch)
            }
        }
        sb.append("$")
        return if (ignoreCase) Regex(sb.toString(), RegexOption.IGNORE_CASE) else Regex(sb.toString())
    }

    private val REGEX_META = setOf('\\', '.', '[', ']', '{', '}', '(', ')', '<', '>', '*', '+', '-', '=', '!', '?', '^', '$', '|', '/')

    // ---------------------------------------------------------------- cells

    /** First-candidate value of [expr] for projection / ORDER BY / GROUP BY (§2). */
    fun evalCell(
        expr: SqlExpr,
        doc: KdbDocument,
        env: EvalEnv,
    ): SqlCell =
        when (expr) {
            is SqlExpr.Literal -> expr.cell
            is SqlExpr.ColumnRef -> cellForColumn(expr.name, doc, env)
            is SqlExpr.QualifiedColumn -> cellForColumn("${expr.qualifier}.${expr.name}", doc, env)
            is SqlExpr.Parameter -> parameterToCell(env.parameters.getOrNull(expr.index)) ?: SqlCell.Null
            is SqlExpr.FunctionCall -> evalFunction(expr, doc, env)
            is SqlExpr.Match, is SqlExpr.Similarity, is SqlExpr.Fuse ->
                SqlCell.DoubleVal(scoreOf(expr, doc, env)?.let { widenScore(it) } ?: 0.0)
            is SqlExpr.Binary, is SqlExpr.Unary, is SqlExpr.Between, is SqlExpr.InList ->
                SqlCell.BoolVal(eval(expr, doc, env))
            is SqlExpr.VectorLiteral -> SqlCell.JsonVal(JsonValue.JArray(expr.values.map { JsonValue.JNumber(it) }).toJsonString())
        }

    /** Every candidate of a column expression (ANY semantics); a single cell for anything else. */
    fun candidateCells(
        expr: SqlExpr,
        doc: KdbDocument,
        env: EvalEnv,
    ): List<SqlCell> =
        when (expr) {
            is SqlExpr.ColumnRef -> columnCandidates(expr.name, doc, env)
            is SqlExpr.QualifiedColumn -> columnCandidates("${expr.qualifier}.${expr.name}", doc, env)
            else -> listOf(evalCell(expr, doc, env))
        }

    private fun columnCandidates(
        name: String,
        doc: KdbDocument,
        env: EvalEnv,
    ): List<SqlCell> {
        val path = env.resolvePath(name)
        return when (path) {
            KDB_ID -> listOf(SqlCell.StringVal(doc.id.toString()))
            DOC -> listOf(SqlCell.JsonVal(doc.json))
            else -> {
                val cs = candidatesOf(name, doc, env)
                if (cs.isEmpty()) listOf(SqlCell.Null) else cs.map { SqlPaths.toCell(it) }
            }
        }
    }

    fun cellForColumn(
        name: String,
        doc: KdbDocument,
        env: EvalEnv,
    ): SqlCell = columnCandidates(name, doc, env).first()

    /** Raw JSON candidates at a column path (after alias stripping); empty when absent. */
    fun candidatesOf(
        name: String,
        doc: KdbDocument,
        env: EvalEnv,
    ): List<JsonValue> {
        val path = env.resolvePath(name)
        val root = env.root(doc) ?: return emptyList()
        return SqlPaths.candidates(root, SqlPaths.splitPath(path))
    }

    private fun rawValueOf(
        name: String,
        doc: KdbDocument,
        env: EvalEnv,
    ): JsonValue? {
        val path = env.resolvePath(name)
        val root = env.root(doc) ?: return null
        return SqlPaths.rawValues(root, SqlPaths.splitPath(path)).firstOrNull()
    }

    /**
     * Widens a float32 score to a double through its shortest 32-bit round-trip (Layer 16 §5), so
     * `0.9f` becomes `0.9` and not `0.8999999761581421`, and Go and Kotlin print the same cell.
     */
    fun widenScore(score: Float): Double = score.toString().toDouble()

    fun scoreOf(
        expr: SqlExpr,
        doc: KdbDocument,
        env: EvalEnv,
    ): Float? {
        val ranking =
            env.scores[expr]
                ?: throw SqlPlanningException(
                    when (expr) {
                        is SqlExpr.Match -> "no FULLTEXT index for ${expr.column}"
                        is SqlExpr.Similarity -> "no VECTOR index for ${expr.column}"
                        else -> "score expression was not planned: $expr"
                    },
                    "",
                )
        return ranking[doc.id]
    }

    // ---------------------------------------------------------------- functions

    private fun evalFunction(
        call: SqlExpr.FunctionCall,
        doc: KdbDocument,
        env: EvalEnv,
    ): SqlCell {
        when (call.name.lowercase()) {
            "array_contains" -> return arrayContains(call, doc, env, all = true)
            "array_contains_any" -> return arrayContains(call, doc, env, all = false)
            "array_length" -> {
                val path = pathArg(call, 1)
                val raw = rawValueOf(path, doc, env)
                return if (raw is JsonValue.JArray) SqlCell.LongVal(raw.elements.size.toLong()) else SqlCell.Null
            }
        }
        if (SqlAggregates.isAggregateFunction(call.name)) {
            return SqlAggregates.evalAggregate(call, listOf(doc), env)
        }
        val desc =
            KdbJsonFunctionRegistry.get(call.name)
                ?: throw SqlPlanningException("unknown function: ${call.name}", "")
        if (call.args.size < desc.minArgs || call.args.size > desc.maxArgs) {
            throw SqlPlanningException("${call.name} expects ${desc.minArgs}..${desc.maxArgs} arguments", "")
        }
        val jsonArgs = call.args.map { arg -> exprToJsonValue(arg, doc, env) }
        val result = desc.evaluate(jsonArgs) ?: return SqlCell.Null
        return SqlCell.StringVal(result.toJsonString())
    }

    private fun pathArg(
        call: SqlExpr.FunctionCall,
        minArgs: Int,
    ): String {
        if (call.args.size < minArgs) throw SqlPlanningException("${call.name} needs a path argument", "")
        return when (val a = call.args[0]) {
            is SqlExpr.ColumnRef -> a.name
            is SqlExpr.QualifiedColumn -> "${a.qualifier}.${a.name}"
            else -> throw SqlPlanningException("${call.name} first argument must be a column path", "")
        }
    }

    private fun arrayContains(
        call: SqlExpr.FunctionCall,
        doc: KdbDocument,
        env: EvalEnv,
        all: Boolean,
    ): SqlCell {
        val path = pathArg(call, 2)
        val raw = rawValueOf(path, doc, env) as? JsonValue.JArray ?: return SqlCell.BoolVal(false)
        val wanted = call.args.drop(1).map { SqlPaths.toJson(evalCell(it, doc, env)) }
        val hit =
            if (all) {
                wanted.all { w -> raw.elements.any { SqlPaths.jsonEquals(it, w) } }
            } else {
                wanted.any { w -> raw.elements.any { SqlPaths.jsonEquals(it, w) } }
            }
        return SqlCell.BoolVal(hit)
    }

    private fun exprToJsonValue(
        expr: SqlExpr,
        doc: KdbDocument,
        env: EvalEnv,
    ): JsonValue? =
        when (expr) {
            is SqlExpr.ColumnRef, is SqlExpr.QualifiedColumn -> {
                val name = if (expr is SqlExpr.ColumnRef) expr.name else "${(expr as SqlExpr.QualifiedColumn).qualifier}.${expr.name}"
                when (env.resolvePath(name)) {
                    DOC -> JsonValue.JString(doc.json)
                    KDB_ID -> JsonValue.JString(doc.id.toString())
                    else -> rawValueOf(name, doc, env)
                }
            }
            else -> SqlPaths.toJson(evalCell(expr, doc, env))
        }

    fun parameterToCell(param: SqlParameter?): SqlCell? =
        when (param) {
            null -> null
            is SqlParameter.StringParam -> SqlCell.StringVal(param.value)
            is SqlParameter.IntParam -> SqlCell.LongVal(param.value)
            is SqlParameter.DoubleParam -> SqlCell.DoubleVal(param.value)
            is SqlParameter.BoolParam -> SqlCell.BoolVal(param.value)
            SqlParameter.NullParam -> SqlCell.Null
            is SqlParameter.VectorParam ->
                SqlCell.JsonVal(JsonValue.JArray(param.asFloatArray().map { JsonValue.JNumber(it.toDouble()) }).toJsonString())
        }

    // ---------------------------------------------------------------- comparison (§2)

    private fun typeRank(c: SqlCell): Int =
        when (c) {
            SqlCell.Null -> 0
            is SqlCell.BoolVal -> 1
            is SqlCell.LongVal, is SqlCell.DoubleVal -> 2
            is SqlCell.StringVal -> 3
            is SqlCell.JsonVal -> 4
        }

    /** Same-type / numeric comparison; null when either side is NULL or the types are incomparable. */
    fun compareForPredicate(
        a: SqlCell?,
        b: SqlCell?,
    ): Int? {
        if (a == null || b == null || a is SqlCell.Null || b is SqlCell.Null) return null
        if (typeRank(a) != typeRank(b)) return null
        return compareSameType(a, b)
    }

    /** Total order for sorting: NULL first, then by type rank, then natural (Layer 16 §2). */
    fun compareTotal(
        a: SqlCell?,
        b: SqlCell?,
    ): Int {
        val ca = a ?: SqlCell.Null
        val cb = b ?: SqlCell.Null
        val r = typeRank(ca).compareTo(typeRank(cb))
        if (r != 0) return r
        if (ca is SqlCell.Null) return 0
        return compareSameType(ca, cb)
    }

    private fun compareSameType(
        a: SqlCell,
        b: SqlCell,
    ): Int =
        when {
            a is SqlCell.StringVal && b is SqlCell.StringVal -> a.value.compareTo(b.value)
            a is SqlCell.LongVal && b is SqlCell.LongVal -> a.value.compareTo(b.value)
            a is SqlCell.BoolVal && b is SqlCell.BoolVal -> a.value.compareTo(b.value)
            a is SqlCell.JsonVal && b is SqlCell.JsonVal ->
                if (SqlPaths.jsonEquals(SqlPaths.toJson(a), SqlPaths.toJson(b))) 0 else a.json.compareTo(b.json)
            else -> numeric(a).compareTo(numeric(b))
        }

    private fun numeric(c: SqlCell): Double =
        when (c) {
            is SqlCell.LongVal -> c.value.toDouble()
            is SqlCell.DoubleVal -> c.value
            else -> 0.0
        }

    /** Kept for callers that only need the total order (same contract as [compareTotal]). */
    fun compareCells(
        a: SqlCell?,
        b: SqlCell?,
    ): Int = compareTotal(a, b)

    // ---------------------------------------------------------------- joins

    fun evalJoin(
        expr: SqlExpr,
        joinedDocs: Map<String, KdbDocument>,
        envs: Map<String, EvalEnv>,
    ): Boolean =
        when (expr) {
            is SqlExpr.Binary ->
                when (expr.op) {
                    BinaryOp.AND -> evalJoin(expr.left, joinedDocs, envs) && evalJoin(expr.right, joinedDocs, envs)
                    BinaryOp.OR -> evalJoin(expr.left, joinedDocs, envs) || evalJoin(expr.right, joinedDocs, envs)
                    BinaryOp.LIKE, BinaryOp.NOT_LIKE, BinaryOp.ILIKE, BinaryOp.NOT_ILIKE -> {
                        val (doc, env) = joinTarget(expr.left, joinedDocs, envs)
                        evalLike(expr, doc, env)
                    }
                    else ->
                        compareOp(
                            expr.op,
                            evalJoinCell(expr.left, joinedDocs, envs),
                            evalJoinCell(expr.right, joinedDocs, envs),
                        )
                }
            is SqlExpr.Unary ->
                when (expr.op) {
                    UnaryOp.NOT -> !evalJoin(expr.expr, joinedDocs, envs)
                    UnaryOp.IS_NULL -> evalJoinCell(expr.expr, joinedDocs, envs) is SqlCell.Null
                }
            is SqlExpr.InList -> {
                val (doc, env) = joinTarget(SqlExpr.ColumnRef(expr.column), joinedDocs, envs)
                evalInList(expr, doc, env)
            }
            is SqlExpr.Between -> {
                val (doc, env) = joinTarget(SqlExpr.ColumnRef(expr.column), joinedDocs, envs)
                evalBetween(expr, doc, env)
            }
            else -> {
                val (doc, env) = joinTarget(expr, joinedDocs, envs)
                eval(expr, doc, env)
            }
        }

    fun evalJoinCell(
        expr: SqlExpr,
        joinedDocs: Map<String, KdbDocument>,
        envs: Map<String, EvalEnv>,
    ): SqlCell {
        val (doc, env) = joinTarget(expr, joinedDocs, envs)
        return evalCell(expr, doc, env)
    }

    /** The joined document an expression addresses: by qualifier, else the left (first) table. */
    private fun joinTarget(
        expr: SqlExpr,
        joinedDocs: Map<String, KdbDocument>,
        envs: Map<String, EvalEnv>,
    ): Pair<KdbDocument, EvalEnv> {
        val qualifier =
            when (expr) {
                is SqlExpr.QualifiedColumn -> expr.qualifier
                is SqlExpr.ColumnRef -> expr.name.substringBefore('.', "").takeIf { it.isNotEmpty() && it in joinedDocs }
                else -> null
            }
        val key = qualifier?.takeIf { it in joinedDocs } ?: joinedDocs.keys.first()
        return joinedDocs.getValue(key) to envs.getValue(key)
    }
}
