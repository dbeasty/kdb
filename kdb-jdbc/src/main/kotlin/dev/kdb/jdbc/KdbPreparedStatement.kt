package dev.kdb.jdbc

import dev.kdb.sql.SqlParameter
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.util.Calendar

public class KdbPreparedStatement(
    connection: KdbSqlConnection,
    private val sql: String,
) : KdbStatement(connection), PreparedStatement {
    private val bindingSlots = mutableListOf<SqlParameter?>()
    private val batchBindings = mutableListOf<List<SqlParameter>>()

    override fun executeQuery(): ResultSet {
        checkOpen()
        return executeQuery(sql, resolvedBindings())
    }

    override fun execute(): Boolean = execute(sql, resolvedBindings())

    override fun executeUpdate(): Int {
        val (keys, count) = executeUpdateReturningKeys(sql, resolvedBindings())
        lastGeneratedKeys = keys
        return count
    }

    private fun executeUpdateReturningKeys(
        sql: String,
        parameters: List<SqlParameter>,
    ): Pair<List<String>, Int> {
        connection.checkWritable()
        val hybridResult =
            connection.blocking { connection.executeHybrid(qualifyTable(sql), parameters) }
        return hybridResult.result.generatedIds to hybridResult.result.rowsAffected
    }

    override fun setString(
        parameterIndex: Int,
        x: String?,
    ) {
        bind(parameterIndex, if (x == null) SqlParameter.NullParam else SqlParameter.StringParam(x))
    }

    override fun setLong(
        parameterIndex: Int,
        x: Long,
    ) {
        bind(parameterIndex, SqlParameter.IntParam(x))
    }

    override fun setNull(
        parameterIndex: Int,
        sqlType: Int,
    ) {
        bind(parameterIndex, SqlParameter.NullParam)
    }

    override fun clearParameters() {
        bindingSlots.clear()
    }

    override fun getMetaData(): ResultSetMetaData? = null

    override fun getParameterMetaData(): ParameterMetaData? = null

    override fun setBoolean(
        parameterIndex: Int,
        x: Boolean,
    ) {
        bind(parameterIndex, SqlParameter.BoolParam(x))
    }

    override fun setByte(
        parameterIndex: Int,
        x: Byte,
    ) {
        bind(parameterIndex, SqlParameter.IntParam(x.toLong()))
    }

    override fun setShort(
        parameterIndex: Int,
        x: Short,
    ) {
        bind(parameterIndex, SqlParameter.IntParam(x.toLong()))
    }

    override fun setInt(
        parameterIndex: Int,
        x: Int,
    ) {
        bind(parameterIndex, SqlParameter.IntParam(x.toLong()))
    }

    override fun setFloat(
        parameterIndex: Int,
        x: Float,
    ) {
        bind(parameterIndex, SqlParameter.DoubleParam(x.toDouble()))
    }

    override fun setDouble(
        parameterIndex: Int,
        x: Double,
    ) {
        bind(parameterIndex, SqlParameter.DoubleParam(x))
    }

    override fun setBigDecimal(
        parameterIndex: Int,
        x: BigDecimal?,
    ) {
        bind(
            parameterIndex,
            if (x == null) SqlParameter.NullParam else SqlParameter.StringParam(x.toPlainString()),
        )
    }

    override fun setBytes(
        parameterIndex: Int,
        x: ByteArray?,
    ) = unsupported()

    override fun setDate(
        parameterIndex: Int,
        x: Date?,
    ) = unsupported()

    override fun setTime(
        parameterIndex: Int,
        x: Time?,
    ) = unsupported()

    override fun setTimestamp(
        parameterIndex: Int,
        x: Timestamp?,
    ) = unsupported()

    override fun setAsciiStream(
        parameterIndex: Int,
        x: InputStream?,
    ) = unsupported()

    override fun setAsciiStream(
        parameterIndex: Int,
        x: InputStream?,
        length: Int,
    ) = unsupported()

    override fun setAsciiStream(
        parameterIndex: Int,
        x: InputStream?,
        length: Long,
    ) = unsupported()

    @Deprecated("Deprecated in Java")
    override fun setUnicodeStream(
        parameterIndex: Int,
        x: InputStream?,
        length: Int,
    ) = unsupported()

    override fun setBinaryStream(
        parameterIndex: Int,
        x: InputStream?,
    ) = unsupported()

    override fun setBinaryStream(
        parameterIndex: Int,
        x: InputStream?,
        length: Int,
    ) = unsupported()

    override fun setBinaryStream(
        parameterIndex: Int,
        x: InputStream?,
        length: Long,
    ) = unsupported()

    override fun setObject(
        parameterIndex: Int,
        x: Any?,
        targetSqlType: Int,
    ) {
        when (x) {
            null -> setNull(parameterIndex, targetSqlType)
            is String -> setString(parameterIndex, x)
            is Long -> setLong(parameterIndex, x)
            is Int -> setInt(parameterIndex, x)
            is Boolean -> setBoolean(parameterIndex, x)
            else -> setString(parameterIndex, x.toString())
        }
    }

    override fun setObject(
        parameterIndex: Int,
        x: Any?,
    ) {
        setObject(parameterIndex, x, Types.VARCHAR)
    }

    override fun setCharacterStream(
        parameterIndex: Int,
        reader: Reader?,
    ) = unsupported()

    override fun setCharacterStream(
        parameterIndex: Int,
        reader: Reader?,
        length: Int,
    ) = unsupported()

    override fun setCharacterStream(
        parameterIndex: Int,
        reader: Reader?,
        length: Long,
    ) = unsupported()

    override fun setRef(
        parameterIndex: Int,
        x: Ref?,
    ) = unsupported()

    override fun setBlob(
        parameterIndex: Int,
        x: Blob?,
    ) = unsupported()

    override fun setBlob(
        parameterIndex: Int,
        inputStream: InputStream?,
    ) = unsupported()

    override fun setClob(
        parameterIndex: Int,
        x: Clob?,
    ) = unsupported()

    override fun setClob(
        parameterIndex: Int,
        reader: Reader?,
    ) = unsupported()

    override fun setArray(
        parameterIndex: Int,
        x: java.sql.Array?,
    ) = unsupported()

    override fun setDate(
        parameterIndex: Int,
        x: Date?,
        cal: Calendar?,
    ) = unsupported()

    override fun setTime(
        parameterIndex: Int,
        x: Time?,
        cal: Calendar?,
    ) = unsupported()

    override fun setTimestamp(
        parameterIndex: Int,
        x: Timestamp?,
        cal: Calendar?,
    ) = unsupported()

    override fun setNull(
        parameterIndex: Int,
        sqlType: Int,
        typeName: String?,
    ) {
        setNull(parameterIndex, sqlType)
    }

    override fun setURL(
        parameterIndex: Int,
        x: URL?,
    ) = unsupported()

    override fun setRowId(
        parameterIndex: Int,
        x: RowId?,
    ) = unsupported()

    override fun setNString(
        parameterIndex: Int,
        value: String?,
    ) {
        setString(parameterIndex, value)
    }

    override fun setNCharacterStream(
        parameterIndex: Int,
        value: Reader?,
    ) = unsupported()

    override fun setNCharacterStream(
        parameterIndex: Int,
        value: Reader?,
        length: Long,
    ) = unsupported()

    override fun setNClob(
        parameterIndex: Int,
        value: NClob?,
    ) = unsupported()

    override fun setNClob(
        parameterIndex: Int,
        reader: Reader?,
    ) = unsupported()

    override fun setClob(
        parameterIndex: Int,
        reader: Reader?,
        length: Long,
    ) = unsupported()

    override fun setBlob(
        parameterIndex: Int,
        inputStream: InputStream?,
        length: Long,
    ) = unsupported()

    override fun setNClob(
        parameterIndex: Int,
        reader: Reader?,
        length: Long,
    ) = unsupported()

    override fun setSQLXML(
        parameterIndex: Int,
        xmlObject: SQLXML?,
    ) = unsupported()

    override fun setObject(
        parameterIndex: Int,
        x: Any?,
        targetSqlType: Int,
        scaleOrLength: Int,
    ) {
        setObject(parameterIndex, x, targetSqlType)
    }

    override fun addBatch() {
        batchBindings += resolvedBindings()
        bindingSlots.clear()
    }

    override fun clearBatch() {
        batchBindings.clear()
    }

    override fun executeBatch(): IntArray {
        checkOpen()
        connection.checkWritable()
        if (batchBindings.isEmpty()) return IntArray(0)
        val counts = IntArray(batchBindings.size)
        val keys = mutableListOf<String>()
        for (i in batchBindings.indices) {
            val (generated, count) = executeUpdateReturningKeys(sql, batchBindings[i])
            counts[i] = count
            keys += generated
        }
        batchBindings.clear()
        bindingSlots.clear()
        lastGeneratedKeys = keys
        return counts
    }

    private fun bind(
        parameterIndex: Int,
        value: SqlParameter,
    ) {
        if (parameterIndex < 1) {
            throw SQLException("parameter index must be >= 1")
        }
        while (bindingSlots.size < parameterIndex) {
            bindingSlots.add(null)
        }
        bindingSlots[parameterIndex - 1] = value
    }

    private fun resolvedBindings(): List<SqlParameter> {
        if (bindingSlots.isEmpty()) {
            return emptyList()
        }
        if (bindingSlots.any { it == null }) {
            throw SQLException("not all parameters were set")
        }
        return bindingSlots.filterNotNull()
    }

    private fun unsupported(): Nothing = throw SQLFeatureNotSupportedException()
}
