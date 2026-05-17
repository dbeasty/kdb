package dev.kdb.jdbc

import dev.kdb.jdbc.file.openFileRuntime
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.sql.SQLFeatureNotSupportedException
import java.util.Properties
import java.util.logging.Logger

public class KdbDriver : Driver {
    override fun connect(
        url: String,
        info: Properties?,
    ): Connection? {
        if (!acceptsURL(url)) return null
        val parsed = KdbJdbcUrlParser.parse(url, info)
        return when (parsed.mode) {
            JdbcMode.MEMORY ->
                KdbConnection(
                    openMemoryRuntime(parsed.catalog, parsed.namespaceId),
                    parsed,
                )
            JdbcMode.FILE -> {
                val root =
                    parsed.dataRoot
                        ?: throw SQLException("file JDBC URL missing data root")
                KdbConnection(
                    openFileRuntime(root, parsed.catalog, parsed.namespaceId),
                    parsed,
                )
            }
            JdbcMode.NETWORK ->
                throw SQLFeatureNotSupportedException("KDB JDBC mode ${parsed.mode} is not implemented in v1")
        }
    }

    override fun acceptsURL(url: String): Boolean = KdbJdbcUrlParser.accepts(url)

    override fun getPropertyInfo(
        url: String,
        info: Properties?,
    ): Array<DriverPropertyInfo> = emptyArray()

    override fun getMajorVersion(): Int = 0

    override fun getMinorVersion(): Int = 9

    override fun jdbcCompliant(): Boolean = false

    override fun getParentLogger(): Logger = Logger.getLogger(KdbDriver::class.java.name)

    public companion object {
        public const val URL_PREFIX: String = "jdbc:kdb:"

        init {
            try {
                DriverManager.registerDriver(KdbDriver())
            } catch (_: SQLException) {
            }
        }
    }
}
