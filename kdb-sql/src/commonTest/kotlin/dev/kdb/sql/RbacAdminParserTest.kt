package dev.kdb.sql

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

class RbacAdminParserTest {
    private val parser = defaultSqlParser()

    @Test
    fun createAndDropRole() {
        val create = parser.parse("CREATE ROLE analyst")
        assertIs<SqlStatement.CreateRole>(create)
        assertEquals("analyst", create.name)

        val drop = parser.parse("DROP ROLE analyst")
        assertIs<SqlStatement.DropRole>(drop)
        assertEquals("analyst", drop.name)
    }

    @Test
    fun createUserWithRoles() {
        val stmt = parser.parse("CREATE USER alice WITH PASSWORD 'hunter2' ROLES (analyst, auditor)")
        assertIs<SqlStatement.CreateUser>(stmt)
        assertEquals("alice", stmt.id)
        assertEquals("hunter2", stmt.password)
        assertEquals(listOf("analyst", "auditor"), stmt.roles)
    }

    @Test
    fun createUserWithoutRoles() {
        val stmt = parser.parse("CREATE USER bob WITH PASSWORD 'pw'")
        assertIs<SqlStatement.CreateUser>(stmt)
        assertEquals(emptyList(), stmt.roles)
    }

    @Test
    fun dropUser() {
        val stmt = parser.parse("DROP USER alice")
        assertIs<SqlStatement.DropUser>(stmt)
        assertEquals("alice", stmt.id)
    }

    @Test
    fun grantOnDatabase() {
        val stmt = parser.parse("GRANT write ON DATABASE orders TO analyst")
        assertIs<SqlStatement.Grant>(stmt)
        assertEquals(GrantSpec("write", "orders", null, null, "analyst"), stmt.grant)
    }

    @Test
    fun grantOnCollection() {
        val stmt = parser.parse("GRANT read ON COLLECTION orders.invoices TO analyst")
        assertIs<SqlStatement.Grant>(stmt)
        assertEquals(GrantSpec("read", "orders", "invoices", null, "analyst"), stmt.grant)
    }

    @Test
    fun grantOnDocumentWithHyphenatedId() {
        val docId = "9f8b7c6d-1111-2222-3333-444455556666"
        val stmt = parser.parse("GRANT write ON DOCUMENT orders.invoices.$docId TO analyst")
        assertIs<SqlStatement.Grant>(stmt)
        assertEquals(GrantSpec("write", "orders", "invoices", docId, "analyst"), stmt.grant)
    }

    @Test
    fun revokeFromRole() {
        val stmt = parser.parse("REVOKE write ON COLLECTION orders.invoices FROM analyst")
        assertIs<SqlStatement.Revoke>(stmt)
        assertEquals(GrantSpec("write", "orders", "invoices", null, "analyst"), stmt.grant)
    }

    @Test
    fun rejectsScopeSegmentMismatch() {
        assertFailsWith<SqlParseException> {
            parser.parse("GRANT write ON DATABASE orders.invoices TO analyst")
        }
    }

    @Test
    fun isAdminStatementCoversAllSixKinds() {
        val statements =
            listOf(
                "CREATE ROLE r",
                "DROP ROLE r",
                "GRANT write ON DATABASE d TO r",
                "REVOKE write ON DATABASE d FROM r",
                "CREATE USER u WITH PASSWORD 'p'",
                "DROP USER u",
            )
        for (sql in statements) {
            assertTrue(isAdminStatement(parser.parse(sql)), "expected admin statement: $sql")
        }
        assertTrue(!isAdminStatement(parser.parse("SELECT * FROM t")))
    }
}
