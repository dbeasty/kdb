package dev.kdb.jdbc

import dev.kdb.sql.QueryResult
import dev.kdb.sql.ResultColumn
import dev.kdb.sql.SqlCell
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Statement
import java.math.BigDecimal
import java.sql.Timestamp
import java.sql.Types

public fun queryResultSet(result: QueryResult): ResultSet {
    val state = ResultSetState(result)
    val handler =
        InvocationHandler { _, method, args ->
            when (method.name) {
                "next" -> state.next()
                "close" -> {
                    state.closed = true
                    null
                }
                "isClosed" -> state.closed
                "wasNull" -> state.wasNull
                "getRow" -> if (state.rowIndex < 0) 0 else state.rowIndex + 1
                "getMetaData" -> state.metaData()
                "getStatement" -> null
                "getType" -> ResultSet.TYPE_FORWARD_ONLY
                "getConcurrency" -> ResultSet.CONCUR_READ_ONLY
                "getFetchDirection" -> ResultSet.FETCH_FORWARD
                "getHoldability" -> ResultSet.HOLD_CURSORS_OVER_COMMIT
                "isBeforeFirst" -> state.rowIndex < 0
                "isAfterLast" -> state.rowIndex >= result.rows.size
                "isFirst" -> state.rowIndex == 0
                "isLast" -> state.rowIndex == result.rows.size - 1
                "beforeFirst" -> {
                    state.rowIndex = -1
                    null
                }
                "afterLast" -> {
                    state.rowIndex = result.rows.size
                    null
                }
                "first" -> {
                    state.rowIndex = if (result.rows.isEmpty()) -1 else 0
                    state.rowIndex == 0
                }
                "last" -> {
                    if (result.rows.isEmpty()) {
                        state.rowIndex = -1
                        false
                    } else {
                        state.rowIndex = result.rows.lastIndex
                        true
                    }
                }
                "findColumn" -> {
                    val label = args[0] as String
                    val ix = result.columns.indexOfFirst { it.name.equals(label, ignoreCase = true) }
                    if (ix < 0) throw SQLException("column not found: $label")
                    ix + 1
                }
                "getString" ->
                    when (args.size) {
                        1 ->
                            when (val a0 = args[0]) {
                                is Int -> state.getString(a0)
                                is String -> state.getString(state.findColumn(a0 as String))
                                else -> unsupported(method)
                            }
                        else -> unsupported(method)
                    }
                "getLong" ->
                    when (args[0]) {
                        is Int -> state.getLong(args[0] as Int)
                        is String -> state.getLong(state.findColumn(args[0] as String))
                        else -> unsupported(method)
                    }
                "getInt" ->
                    when (args[0]) {
                        is Int -> state.getLong(args[0] as Int).toInt()
                        is String -> state.getLong(state.findColumn(args[0] as String)).toInt()
                        else -> unsupported(method)
                    }
                "getBoolean" ->
                    when (args[0]) {
                        is Int -> state.getBoolean(args[0] as Int)
                        is String -> state.getBoolean(state.findColumn(args[0] as String))
                        else -> unsupported(method)
                    }
                "getDouble" ->
                    when (args[0]) {
                        is Int -> state.getDouble(args[0] as Int)
                        is String -> state.getDouble(state.findColumn(args[0] as String))
                        else -> unsupported(method)
                    }
                "getObject" -> {
                    when (args.size) {
                        1 ->
                            when (val a0 = args[0]) {
                                is Int -> state.getObject(a0)
                                is String -> state.getObject(state.findColumn(a0 as String))
                                else -> unsupported(method)
                            }
                        2 -> state.getObject(args[0] as Int)
                        else -> unsupported(method)
                    }
                }
                "getBigDecimal" ->
                    when (args[0]) {
                        is Int -> state.getBigDecimal(args[0] as Int)
                        is String -> state.getBigDecimal(state.findColumn(args[0] as String))
                        else -> unsupported(method)
                    }
                "getTimestamp" ->
                    when (args[0]) {
                        is Int -> state.getTimestamp(args[0] as Int)
                        is String -> state.getTimestamp(state.findColumn(args[0] as String))
                        else -> unsupported(method)
                    }
                "unwrap" -> {
                    val iface = args[0] as Class<*>
                    if (iface.isInstance(state)) iface.cast(state) else throw SQLFeatureNotSupportedException()
                }
                "isWrapperFor" -> args[0] is Class<*> && (args[0] as Class<*>).isInstance(state)
                "hashCode" -> System.identityHashCode(state)
                else -> unsupported(method)
            }
        }
    @Suppress("UNCHECKED_CAST")
    return Proxy.newProxyInstance(
        ResultSet::class.java.classLoader,
        arrayOf(ResultSet::class.java),
        handler,
    ) as ResultSet
}

private class ResultSetState(
    private val result: QueryResult,
) {
    var rowIndex = -1
    var closed = false
    var wasNull = false

    fun next(): Boolean {
        if (closed) throw SQLException("ResultSet is closed")
        rowIndex++
        return rowIndex < result.rows.size
    }

    fun findColumn(label: String): Int {
        val ix = result.columns.indexOfFirst { it.name.equals(label, ignoreCase = true) }
        if (ix < 0) throw SQLException("column not found: $label")
        return ix + 1
    }

    private fun cell(columnIndex: Int): SqlCell {
        if (rowIndex < 0 || rowIndex >= result.rows.size) throw SQLException("no current row")
        return result.rows[rowIndex].values[columnIndex - 1]
    }

    fun getString(columnIndex: Int): String? {
        val c = cell(columnIndex)
        wasNull = c is SqlCell.Null
        return when (c) {
            SqlCell.Null -> null
            is SqlCell.StringVal -> c.value
            is SqlCell.JsonVal -> c.json
            is SqlCell.LongVal -> c.value.toString()
            is SqlCell.DoubleVal -> c.value.toString()
            is SqlCell.BoolVal -> c.value.toString()
        }
    }

    fun getLong(columnIndex: Int): Long {
        val c = cell(columnIndex)
        wasNull = c is SqlCell.Null
        return when (c) {
            is SqlCell.LongVal -> c.value
            is SqlCell.StringVal -> c.value.toLong()
            else -> throw SQLException("not a long column")
        }
    }

    fun getBoolean(columnIndex: Int): Boolean {
        val c = cell(columnIndex)
        wasNull = c is SqlCell.Null
        return when (c) {
            is SqlCell.BoolVal -> c.value
            is SqlCell.StringVal -> c.value.toBooleanStrict()
            else -> throw SQLException("not a boolean column")
        }
    }

    fun getDouble(columnIndex: Int): Double {
        val c = cell(columnIndex)
        wasNull = c is SqlCell.Null
        return when (c) {
            is SqlCell.DoubleVal -> c.value
            is SqlCell.LongVal -> c.value.toDouble()
            is SqlCell.StringVal -> c.value.toDouble()
            else -> throw SQLException("not a double column")
        }
    }

    fun getObject(columnIndex: Int): Any? {
        val c = cell(columnIndex)
        wasNull = c is SqlCell.Null
        return when (c) {
            SqlCell.Null -> null
            is SqlCell.StringVal -> c.value
            is SqlCell.JsonVal -> c.json
            is SqlCell.LongVal -> c.value
            is SqlCell.DoubleVal -> c.value
            is SqlCell.BoolVal -> c.value
        }
    }

    fun getBigDecimal(columnIndex: Int): BigDecimal? {
        val c = cell(columnIndex)
        wasNull = c is SqlCell.Null
        return when (c) {
            SqlCell.Null -> null
            is SqlCell.LongVal -> BigDecimal.valueOf(c.value)
            is SqlCell.DoubleVal -> BigDecimal.valueOf(c.value)
            is SqlCell.StringVal -> c.value.toBigDecimalOrNull()
            else -> throw SQLException("not a decimal column")
        }
    }

    fun getTimestamp(columnIndex: Int): Timestamp? {
        val c = cell(columnIndex)
        wasNull = c is SqlCell.Null
        return when (c) {
            SqlCell.Null -> null
            is SqlCell.LongVal -> Timestamp(c.value)
            is SqlCell.StringVal -> Timestamp.valueOf(c.value.replace('T', ' ').take(19))
            else -> throw SQLException("not a timestamp column")
        }
    }

    fun metaData(): ResultSetMetaData = KdbResultSetMetaData(result)
}

private class KdbResultSetMetaData(
    private val result: QueryResult,
) : ResultSetMetaData {
    override fun getColumnCount(): Int = result.columns.size

    override fun getColumnName(column: Int): String = result.columns[column - 1].name

    override fun getColumnLabel(column: Int): String = getColumnName(column)

    override fun getColumnType(column: Int): Int =
        when (result.columns[column - 1].sqlType.uppercase()) {
            "INTEGER", "BIGINT" -> Types.BIGINT
            "BOOLEAN" -> Types.BOOLEAN
            "DOUBLE" -> Types.DOUBLE
            "DECIMAL" -> Types.DECIMAL
            "TIMESTAMP" -> Types.TIMESTAMP
            "JSON" -> Types.LONGVARCHAR
            else -> Types.VARCHAR
        }

    override fun getColumnTypeName(column: Int): String = result.columns[column - 1].sqlType

    override fun <T> unwrap(iface: Class<T>): T = throw SQLFeatureNotSupportedException()

    override fun isWrapperFor(iface: Class<*>): Boolean = false

    override fun isAutoIncrement(column: Int): Boolean = false

    override fun isCaseSensitive(column: Int): Boolean = true

    override fun isSearchable(column: Int): Boolean = true

    override fun isCurrency(column: Int): Boolean = false

    override fun isNullable(column: Int): Int = ResultSetMetaData.columnNullable

    override fun isSigned(column: Int): Boolean = true

    override fun getColumnDisplaySize(column: Int): Int = 255

    override fun getPrecision(column: Int): Int = 0

    override fun getScale(column: Int): Int = 0

    override fun getTableName(column: Int): String = ""

    override fun getSchemaName(column: Int): String = ""

    override fun getCatalogName(column: Int): String = ""

    override fun isReadOnly(column: Int): Boolean = true

    override fun isWritable(column: Int): Boolean = false

    override fun isDefinitelyWritable(column: Int): Boolean = false

    override fun getColumnClassName(column: Int): String = "java.lang.String"
}

private fun unsupported(method: Method): Nothing =
    throw SQLFeatureNotSupportedException("ResultSet.${method.name} is not supported")
