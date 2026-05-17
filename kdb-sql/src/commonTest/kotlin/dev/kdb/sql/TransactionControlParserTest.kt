package dev.kdb.sql

import kotlin.test.Test
import kotlin.test.assertIs

class TransactionControlParserTest {
  private val parser = defaultSqlParser()

  @Test
  fun beginAndStartTransaction() {
    assertIs<SqlStatement.BeginTransaction>(parser.parse("BEGIN"))
    assertIs<SqlStatement.BeginTransaction>(parser.parse("BEGIN WORK"))
    assertIs<SqlStatement.BeginTransaction>(parser.parse("START TRANSACTION"))
  }

  @Test
  fun commitAndRollback() {
    assertIs<SqlStatement.Commit>(parser.parse("COMMIT"))
    assertIs<SqlStatement.Commit>(parser.parse("COMMIT WORK"))
    assertIs<SqlStatement.Rollback>(parser.parse("ROLLBACK"))
    assertIs<SqlStatement.Rollback>(parser.parse("ROLLBACK WORK"))
  }
}
