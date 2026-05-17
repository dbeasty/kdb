package dev.kdb.hibernate

import org.hibernate.dialect.Dialect
import org.hibernate.dialect.pagination.LimitHandler
import org.hibernate.dialect.pagination.LimitOffsetLimitHandler

/**
 * Minimal Hibernate 6 dialect for KDB embedded/network JDBC.
 *
 * KDB uses document storage with an optional schema lens; SQL is a query interface.
 */
public class KdbDialect : Dialect() {
    override fun getLimitHandler(): LimitHandler = LimitOffsetLimitHandler.INSTANCE

    override fun supportsIfExistsBeforeTableName(): Boolean = true

    override fun supportsIfExistsBeforeConstraintName(): Boolean = true
}
