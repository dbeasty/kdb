package dev.kdb.jdbc

import dev.kdb.sql.QueryResult
import dev.kdb.sql.ResultColumn
import dev.kdb.sql.ColumnSource
import dev.kdb.sql.QueryRow
import dev.kdb.sql.SqlCell
import java.sql.ResultSet

internal fun generatedKeysResultSet(ids: List<String>): ResultSet {
    val rows = ids.map { QueryRow(listOf(SqlCell.StringVal(it))) }
    val result =
        QueryResult(
            columns = listOf(ResultColumn("kdb_id", "VARCHAR", ColumnSource.KDB_ID)),
            rows = rows,
        )
    return queryResultSet(result)
}
