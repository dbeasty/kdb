package dev.kdb.jdbc

import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.RowIdLifetime
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types

public class KdbDatabaseMetaData(
    private val connection: KdbConnection,
) : DatabaseMetaData {
    override fun allProceduresAreCallable(): Boolean = false

    override fun allTablesAreSelectable(): Boolean = true

    override fun getURL(): String = "${KdbDriver.URL_PREFIX}memory:///${connection.catalog}"

    override fun getUserName(): String = ""

    override fun isReadOnly(): Boolean = connection.isReadOnly

    override fun nullsAreSortedHigh(): Boolean = false

    override fun nullsAreSortedLow(): Boolean = true

    override fun nullsAreSortedAtStart(): Boolean = false

    override fun nullsAreSortedAtEnd(): Boolean = true

    override fun getDatabaseProductName(): String = "KDB"

    override fun getDatabaseProductVersion(): String = "0.9"

    override fun getDriverName(): String = "KDB JDBC"

    override fun getDriverVersion(): String = "0.9"

    override fun getDriverMajorVersion(): Int = 0

    override fun getDriverMinorVersion(): Int = 9

    override fun usesLocalFiles(): Boolean = true

    override fun usesLocalFilePerTable(): Boolean = false

    override fun supportsMixedCaseIdentifiers(): Boolean = true

    override fun storesUpperCaseIdentifiers(): Boolean = false

    override fun storesLowerCaseIdentifiers(): Boolean = false

    override fun storesMixedCaseIdentifiers(): Boolean = true

    override fun supportsMixedCaseQuotedIdentifiers(): Boolean = true

    override fun storesUpperCaseQuotedIdentifiers(): Boolean = false

    override fun storesLowerCaseQuotedIdentifiers(): Boolean = false

    override fun storesMixedCaseQuotedIdentifiers(): Boolean = true

    override fun getIdentifierQuoteString(): String = "\""

    override fun getSQLKeywords(): String = "AT,BEGIN,ROLLBACK,START,TRANSACTION,VERSION,COMMIT,TIME,WORK"

    override fun getNumericFunctions(): String = ""

    override fun getStringFunctions(): String = "kdb_json_get,kdb_json_set"

    override fun getSystemFunctions(): String = ""

    override fun getTimeDateFunctions(): String = ""

    override fun getSearchStringEscape(): String = "\\"

    override fun getExtraNameCharacters(): String = ""

    override fun supportsAlterTableWithAddColumn(): Boolean = true

    override fun supportsAlterTableWithDropColumn(): Boolean = false

    override fun supportsColumnAliasing(): Boolean = true

    override fun nullPlusNonNullIsNull(): Boolean = true

    override fun supportsConvert(): Boolean = false

    override fun supportsConvert(
        fromType: Int,
        toType: Int,
    ): Boolean = false

    override fun supportsTableCorrelationNames(): Boolean = true

    override fun supportsDifferentTableCorrelationNames(): Boolean = false

    override fun supportsExpressionsInOrderBy(): Boolean = true

    override fun supportsOrderByUnrelated(): Boolean = true

    override fun supportsGroupBy(): Boolean = true

    override fun supportsGroupByUnrelated(): Boolean = false

    override fun supportsGroupByBeyondSelect(): Boolean = false

    override fun supportsLikeEscapeClause(): Boolean = true

    override fun supportsMultipleResultSets(): Boolean = false

    override fun supportsMultipleTransactions(): Boolean = false

    override fun supportsNonNullableColumns(): Boolean = true

    override fun supportsMinimumSQLGrammar(): Boolean = true

    override fun supportsCoreSQLGrammar(): Boolean = false

    override fun supportsExtendedSQLGrammar(): Boolean = false

    override fun supportsANSI92EntryLevelSQL(): Boolean = false

    override fun supportsANSI92IntermediateSQL(): Boolean = false

    override fun supportsANSI92FullSQL(): Boolean = false

    override fun supportsIntegrityEnhancementFacility(): Boolean = false

    override fun supportsOuterJoins(): Boolean = false

    override fun supportsFullOuterJoins(): Boolean = false

    override fun supportsLimitedOuterJoins(): Boolean = false

    override fun getSchemaTerm(): String = "namespace"

    override fun getProcedureTerm(): String = "procedure"

    override fun getCatalogTerm(): String = "catalog"

    override fun isCatalogAtStart(): Boolean = true

    override fun getCatalogSeparator(): String = "/"

    override fun supportsSchemasInDataManipulation(): Boolean = true

    override fun supportsSchemasInProcedureCalls(): Boolean = false

    override fun supportsSchemasInTableDefinitions(): Boolean = true

    override fun supportsSchemasInIndexDefinitions(): Boolean = true

    override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false

    override fun supportsCatalogsInDataManipulation(): Boolean = true

    override fun supportsCatalogsInProcedureCalls(): Boolean = false

    override fun supportsCatalogsInTableDefinitions(): Boolean = true

    override fun supportsCatalogsInIndexDefinitions(): Boolean = true

    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false

    override fun supportsPositionedDelete(): Boolean = false

    override fun supportsPositionedUpdate(): Boolean = false

    override fun supportsSelectForUpdate(): Boolean = false

    override fun supportsStoredProcedures(): Boolean = false

    override fun supportsSubqueriesInComparisons(): Boolean = false

    override fun supportsSubqueriesInExists(): Boolean = false

    override fun supportsSubqueriesInIns(): Boolean = false

    override fun supportsSubqueriesInQuantifieds(): Boolean = false

    override fun supportsCorrelatedSubqueries(): Boolean = false

    override fun supportsUnion(): Boolean = false

    override fun supportsUnionAll(): Boolean = false

    override fun supportsOpenCursorsAcrossCommit(): Boolean = false

    override fun supportsOpenCursorsAcrossRollback(): Boolean = false

    override fun supportsOpenStatementsAcrossCommit(): Boolean = false

    override fun supportsOpenStatementsAcrossRollback(): Boolean = false

    override fun getMaxBinaryLiteralLength(): Int = 0

    override fun getMaxCharLiteralLength(): Int = 0

    override fun getMaxColumnNameLength(): Int = 128

    override fun getMaxColumnsInGroupBy(): Int = 0

    override fun getMaxColumnsInIndex(): Int = 16

    override fun getMaxColumnsInOrderBy(): Int = 16

    override fun getMaxColumnsInSelect(): Int = 256

    override fun getMaxColumnsInTable(): Int = 256

    override fun getMaxConnections(): Int = 0

    override fun getMaxCursorNameLength(): Int = 0

    override fun getMaxIndexLength(): Int = 0

    override fun getMaxSchemaNameLength(): Int = 128

    override fun getMaxProcedureNameLength(): Int = 0

    override fun getMaxCatalogNameLength(): Int = 128

    override fun getMaxRowSize(): Int = 0

    override fun doesMaxRowSizeIncludeBlobs(): Boolean = true

    override fun getMaxStatementLength(): Int = 0

    override fun getMaxStatements(): Int = 0

    override fun getMaxTableNameLength(): Int = 128

    override fun getMaxTablesInSelect(): Int = 1

    override fun getMaxUserNameLength(): Int = 0

    override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_READ_COMMITTED

    override fun supportsTransactions(): Boolean = true

    override fun supportsTransactionIsolationLevel(level: Int): Boolean =
        level == Connection.TRANSACTION_READ_COMMITTED

    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false

    override fun supportsDataManipulationTransactionsOnly(): Boolean = true

    override fun dataDefinitionCausesTransactionCommit(): Boolean = false

    override fun dataDefinitionIgnoredInTransactions(): Boolean = true

    override fun getProcedures(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?,
    ): ResultSet {
        val table = connection.embedded.defaultNamespace.substringAfterLast('/')
        return singleRowResultSet(
            arrayOf("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE"),
            arrayOf(connection.catalog, schemaPattern ?: "", table, "TABLE"),
        )
    }

    override fun getSchemas(): ResultSet = getSchemas(null, null)

    override fun getSchemas(
        catalog: String?,
        schemaPattern: String?,
    ): ResultSet =
        singleRowResultSet(
            arrayOf("TABLE_SCHEM"),
            arrayOf(schemaPattern ?: connection.schema ?: "main"),
        )

    override fun getCatalogs(): ResultSet =
        singleRowResultSet(
            arrayOf("TABLE_CAT"),
            arrayOf(connection.catalog),
        )

    override fun getTableTypes(): ResultSet =
        singleRowResultSet(
            arrayOf("TABLE_TYPE"),
            arrayOf("TABLE"),
        )

    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?,
    ): ResultSet {
        val cols =
            listOf(
                arrayOf(connection.catalog, schemaPattern ?: "", tableNamePattern ?: "users", "kdb_id", Types.VARCHAR, "VARCHAR"),
                arrayOf(connection.catalog, schemaPattern ?: "", tableNamePattern ?: "users", "_doc", Types.LONGVARCHAR, "JSON"),
            )
        return multiRowResultSet(
            arrayOf("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME"),
            cols.map { it.map { v -> v as Any? }.toTypedArray() },
        )
    }

    override fun <T> unwrap(iface: Class<T>): T = throw SQLFeatureNotSupportedException()

    override fun isWrapperFor(iface: Class<*>): Boolean = false

    override fun getColumnPrivileges(
        catalog: String?,
        schema: String?,
        table: String?,
        columnNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getTablePrivileges(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getBestRowIdentifier(
        catalog: String?,
        schema: String?,
        table: String?,
        scope: Int,
        nullable: Boolean,
    ): ResultSet = emptyResultSet()

    override fun getVersionColumns(
        catalog: String?,
        schema: String?,
        table: String?,
    ): ResultSet = emptyResultSet()

    override fun getPrimaryKeys(
        catalog: String?,
        schema: String?,
        table: String?,
    ): ResultSet = emptyResultSet()

    override fun getImportedKeys(
        catalog: String?,
        schema: String?,
        table: String?,
    ): ResultSet = emptyResultSet()

    override fun getExportedKeys(
        catalog: String?,
        schema: String?,
        table: String?,
    ): ResultSet = emptyResultSet()

    override fun getCrossReference(
        parentCatalog: String?,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?,
    ): ResultSet = emptyResultSet()

    override fun getTypeInfo(): ResultSet = emptyResultSet()

    override fun getIndexInfo(
        catalog: String?,
        schema: String?,
        table: String?,
        unique: Boolean,
        approximate: Boolean,
    ): ResultSet = emptyResultSet()

    override fun supportsResultSetType(type: Int): Boolean = type == ResultSet.TYPE_FORWARD_ONLY

    override fun supportsResultSetConcurrency(
        type: Int,
        concurrency: Int,
    ): Boolean = type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY

    override fun ownUpdatesAreVisible(type: Int): Boolean = false

    override fun ownDeletesAreVisible(type: Int): Boolean = false

    override fun ownInsertsAreVisible(type: Int): Boolean = false

    override fun othersUpdatesAreVisible(type: Int): Boolean = false

    override fun othersDeletesAreVisible(type: Int): Boolean = false

    override fun othersInsertsAreVisible(type: Int): Boolean = false

    override fun updatesAreDetected(type: Int): Boolean = false

    override fun deletesAreDetected(type: Int): Boolean = false

    override fun insertsAreDetected(type: Int): Boolean = false

    override fun supportsBatchUpdates(): Boolean = false

    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?,
    ): ResultSet = emptyResultSet()

    override fun getConnection(): Connection = connection

    override fun supportsSavepoints(): Boolean = false

    override fun supportsNamedParameters(): Boolean = true

    override fun supportsMultipleOpenResults(): Boolean = false

    override fun supportsGetGeneratedKeys(): Boolean = false

    override fun getSuperTypes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getSuperTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun supportsResultSetHoldability(holdability: Int): Boolean =
        holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT

    override fun getResultSetHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT

    override fun getDatabaseMajorVersion(): Int = 0

    override fun getDatabaseMinorVersion(): Int = 9

    override fun getJDBCMajorVersion(): Int = 4

    override fun getJDBCMinorVersion(): Int = 2

    override fun getSQLStateType(): Int = DatabaseMetaData.sqlStateSQL

    override fun locatorsUpdateCopy(): Boolean = true

    override fun supportsStatementPooling(): Boolean = false

    override fun getRowIdLifetime(): RowIdLifetime = RowIdLifetime.ROWID_UNSUPPORTED

    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = false

    override fun autoCommitFailureClosesAllResultSets(): Boolean = false

    override fun getClientInfoProperties(): ResultSet = emptyResultSet()

    override fun getFunctions(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getFunctionColumns(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
        columnNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun getPseudoColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?,
    ): ResultSet = emptyResultSet()

    override fun generatedKeyAlwaysReturned(): Boolean = false

    override fun supportsRefCursors(): Boolean = false

    private fun emptyResultSet(): ResultSet = multiRowResultSet(arrayOf("x"), emptyList())

    private fun singleRowResultSet(
        columns: Array<String>,
        row: Array<Any?>,
    ): ResultSet = multiRowResultSet(columns, listOf(row))

    private fun multiRowResultSet(
        columns: Array<String>,
        rows: List<Array<Any?>>,
    ): ResultSet {
        val colMeta =
            columns.map { name ->
                dev.kdb.sql.ResultColumn(name, "VARCHAR", dev.kdb.sql.ColumnSource.SCHEMA_FIELD)
            }
        val queryRows =
            rows.map { row ->
                dev.kdb.sql.QueryRow(row.map { v -> dev.kdb.sql.SqlCell.StringVal(v?.toString() ?: "") })
            }
        return queryResultSet(dev.kdb.sql.QueryResult(colMeta, queryRows))
    }
}
