package dev.kdb.integration

import dev.kdb.auth.ConnectionContext
import dev.kdb.auth.store.RegistryAuthStore
import dev.kdb.auth.store.dynamicAuthEngine
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.query.hybrid.ReadConsistency
import dev.kdb.server.KdbServerRuntime
import dev.kdb.server.SqlWireHost
import dev.kdb.wire.HandshakePayload
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireCapabilitySet
import dev.kdb.wire.WireClientMode
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** End-to-end coverage for docs/kdb-rbac-plan.md phase 4: `CREATE ROLE`/`GRANT`/`REVOKE`/
 * `CREATE USER` executed over the real wire protocol against a live [RegistryAuthStore], with
 * [dynamicAuthEngine] resolving every subsequent request from that same store. */
class RbacAdminSqlIntegrationTest {
    private val ns = "orders/invoices"

    private suspend fun seedStore(): RegistryAuthStore {
        val store = RegistryAuthStore.inMemory()
        // "admin" also needs ordinary read access to the namespace it connects to — the wire
        // handshake/session-begin check (AuthAction.SessionBegin) is a separate "read" check,
        // distinct from the "admin" kind gating CREATE ROLE/GRANT/etc.
        store.createRole("admin", setOf("admin:_system", "read:orders/*"))
        store.createUser("root", "root-secret", roles = setOf("admin"))
        return store
    }

    private suspend fun openServer(): KdbServerRuntime = KdbServerRuntime(openMemoryRuntime("orders", ns))

    private suspend fun sqlHost(
        ctx: ConnectionContext,
        store: RegistryAuthStore,
        server: KdbServerRuntime? = null,
    ): SqlWireHost =
        SqlWireHost(
            wire = defaultWireCodec(),
            server = server ?: openServer(),
            defaultNamespace = ns,
            auth = dynamicAuthEngine(store),
            connectionContext = ctx,
            userStore = store,
            roleStore = store,
        )

    @Test
    fun adminCanCreateRoleGrantAndUser() =
        runTest {
            val store = seedStore()
            val server = openServer()
            val host = sqlHost(ConnectionContext(user = "root", password = "root-secret"), store, server)
            assertTrue(doHandshake(host).response.accepted)
            val begin = doSessionBegin(host, "admin-session")

            val createRole = doSqlExec(host, begin.sessionId, "CREATE ROLE analyst")
            assertNull(createRole.error)

            val grant = doSqlExec(host, begin.sessionId, "GRANT write ON DATABASE orders TO analyst")
            assertNull(grant.error)

            val createUser =
                doSqlExec(
                    host,
                    begin.sessionId,
                    "CREATE USER alice WITH PASSWORD 'hunter2' ROLES (analyst)",
                )
            assertNull(createUser.error)

            assertEquals(setOf("write:orders"), store.getRole("analyst")?.grants)
            assertEquals(setOf("analyst"), store.getUser("alice")?.roles)
            assertTrue(store.verifyPassword("alice", "hunter2"))
        }

    @Test
    fun revokeRemovesTheGrant() =
        runTest {
            val store = seedStore()
            store.createRole("analyst", setOf("write:orders"))
            val host = sqlHost(ConnectionContext(user = "root", password = "root-secret"), store)
            assertTrue(doHandshake(host).response.accepted)
            val begin = doSessionBegin(host, "admin-session-2")

            val revoke = doSqlExec(host, begin.sessionId, "REVOKE write ON DATABASE orders FROM analyst")
            assertNull(revoke.error)
            assertEquals(emptySet(), store.getRole("analyst")?.grants)
        }

    @Test
    fun nonAdminCannotManageRoles() =
        runTest {
            val store = seedStore()
            store.createRole("analyst", setOf("read:orders/*"))
            store.createUser("bob", "bob-secret", roles = setOf("analyst"))
            val host = sqlHost(ConnectionContext(user = "bob", password = "bob-secret"), store)
            assertTrue(doHandshake(host).response.accepted)
            val begin = doSessionBegin(host, "bob-session")

            val result = doSqlExec(host, begin.sessionId, "CREATE ROLE sneaky")
            assertNotNull(result.error)
            assertTrue(result.error!!.contains("forbidden", ignoreCase = true))
            assertNull(store.getRole("sneaky"))
        }

    @Test
    fun grantedUserGainsWriteAccessImmediatelyAfterGrant() =
        runTest {
            val store = seedStore()
            val server = openServer()
            val adminHost = sqlHost(ConnectionContext(user = "root", password = "root-secret"), store, server)
            assertTrue(doHandshake(adminHost).response.accepted)
            val adminSession = doSessionBegin(adminHost, "admin-session-3")
            assertNull(doSqlExec(adminHost, adminSession.sessionId, "CREATE ROLE analyst").error)
            // Session-begin needs "read" and the INSERT needs "write" — grant both, same as a
            // real admin would for a role meant to actually connect and write.
            assertNull(
                doSqlExec(adminHost, adminSession.sessionId, "GRANT read ON DATABASE orders TO analyst").error,
            )
            assertNull(
                doSqlExec(adminHost, adminSession.sessionId, "GRANT write ON DATABASE orders TO analyst").error,
            )
            assertNull(
                doSqlExec(
                    adminHost,
                    adminSession.sessionId,
                    "CREATE USER alice WITH PASSWORD 'hunter2' ROLES (analyst)",
                ).error,
            )

            val aliceHost = sqlHost(ConnectionContext(user = "alice", password = "hunter2"), store, server)
            assertTrue(doHandshake(aliceHost).response.accepted)
            val aliceSession = doSessionBegin(aliceHost, "alice-session")
            val insert =
                doSqlExec(
                    aliceHost,
                    aliceSession.sessionId,
                    "INSERT INTO invoices (invoiceId) VALUES ('i1')",
                )
            assertNull(insert.error)
        }

    private suspend fun doHandshake(host: SqlWireHost): WireMessage.HandshakeAck {
        val wire = defaultWireCodec()
        return wire.decode(
            host.handleFrame(
                wire.encode(
                    WireMessage.Handshake(
                        WireHeader(WireMessageType.HANDSHAKE, KDB_WIRE_PROTOCOL_VERSION, 1, 0),
                        HandshakePayload(
                            nodeId = "c1",
                            namespaces = listOf(ns),
                            localHeads = emptyMap(),
                            capabilities = WireCapabilitySet(),
                            clientMode = WireClientMode.SQL_CLIENT,
                        ),
                    ),
                ),
            )!!,
        ) as WireMessage.HandshakeAck
    }

    private suspend fun doSessionBegin(
        host: SqlWireHost,
        sessionId: String,
    ): WireMessage.SessionBeginAck {
        val wire = defaultWireCodec()
        return wire.decode(
            host.handleFrame(
                wire.encode(
                    WireMessage.SessionBegin(
                        WireHeader(WireMessageType.SESSION_BEGIN, KDB_WIRE_PROTOCOL_VERSION, 2, 0),
                        namespace = ns,
                        sessionId = sessionId,
                        readConsistency = ReadConsistency.READ_COMMITTED.name,
                        baseVersionHex = null,
                    ),
                ),
            )!!,
        ) as WireMessage.SessionBeginAck
    }

    private suspend fun doSqlExec(
        host: SqlWireHost,
        sessionId: String,
        sql: String,
    ): WireMessage.SqlResult {
        val wire = defaultWireCodec()
        return wire.decode(
            host.handleFrame(
                wire.encode(
                    WireMessage.SqlExec(
                        WireHeader(WireMessageType.SQL_EXEC, KDB_WIRE_PROTOCOL_VERSION, 3, 0),
                        namespace = ns,
                        sessionId = sessionId,
                        sql = sql,
                        parametersJson = null,
                    ),
                ),
            )!!,
        ) as WireMessage.SqlResult
    }
}
