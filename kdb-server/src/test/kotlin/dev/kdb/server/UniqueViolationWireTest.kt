package dev.kdb.server

import dev.kdb.auth.ConnectionContext
import dev.kdb.codec.KdbUuid
import dev.kdb.embed.openMemoryRuntime
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.schema.UniqueConstraint
import dev.kdb.stream.WireConnection
import dev.kdb.transaction.UniqueConstraintViolationException
import dev.kdb.wire.KDB_WIRE_PROTOCOL_VERSION
import dev.kdb.wire.WireHeader
import dev.kdb.wire.WireMessage
import dev.kdb.wire.WireMessageType
import dev.kdb.wire.defaultWireCodec
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Layer 16 §9.6 on the wire: a compound-unique collision is refused with errorCode UNIQUE_VIOLATION,
 * distinct from a malformed payload's SCHEMA_VIOLATION, and the registry the server enforces against
 * is built from existing data at open.
 */
class UniqueViolationWireTest {
    private val wire = defaultWireCodec()
    private val ns = "demo/unique"

    private val schema =
        KdbSchema.build(
            listOf(
                SchemaField("tenant", KdbFieldType.StringType, required = true, indexed = true),
                SchemaField("code", KdbFieldType.StringType, required = true, indexed = true),
            ),
            uniqueConstraints = listOf(UniqueConstraint("tenant", "code")),
        )

    /** Guards: the second document claiming the same (tenant, code) is refused on the wire with the
     * UNIQUE_VIOLATION code a client needs to tell "that value is taken" from "your payload is bad". */
    @Test
    fun aCollidingUpsertIsRefusedWithUniqueViolationOnTheWire() =
        runTest {
            val runtime = openMemoryRuntime("demo", ns, schema)
            val server = KdbServerRuntime(runtime)
            server.start(ns)
            val host = sqlWireHostFactory(wire, server, ns)(ConnectionContext.EMPTY)
            val conn = FakeWireConnection()
            val loop = launch { pipelinedPerConnection(conn, host) }

            val first = upsert(conn, KdbUuid.random(), """{"tenant":"t1","code":"c1"}""", 1)
            assertNull(first.error, "first write failed: ${first.error}")

            val clash = upsert(conn, KdbUuid.random(), """{"tenant":"t1","code":"c1"}""", 2)
            assertNotNull(clash.error, "a colliding write must be refused")
            assertEquals("UNIQUE_VIOLATION", clash.errorCode)
            assertTrue(clash.error!!.contains("unique constraint"), "unexpected message: ${clash.error}")

            // Only part of the tuple matching is not a collision.
            assertNull(upsert(conn, KdbUuid.random(), """{"tenant":"t2","code":"c1"}""", 3).error)

            conn.close()
            loop.join()
            server.close()
        }

    /** Guards: the registry is rebuilt from existing data at open, so a runtime opened over documents
     * that already hold a value enforces against them rather than starting empty. */
    @Test
    fun theRegistryIsRebuiltFromExistingDataAtOpen() =
        runTest {
            val runtime = openMemoryRuntime("demo", ns, schema)
            val first = KdbServerRuntime(runtime)
            first.upsert(ns, KdbUuid.random(), """{"tenant":"t1","code":"c1"}""")

            // A second server over the same storage/DAG - as a restart would be.
            val reopened = KdbServerRuntime(runtime)
            reopened.start(ns)
            assertEquals(1, reopened.uniqueKeysFor(ns).size(), "open must rebuild the registry from head")
            assertFailsWith<UniqueConstraintViolationException> {
                reopened.upsert(ns, KdbUuid.random(), """{"tenant":"t1","code":"c1"}""")
            }
            reopened.close()
        }

    private suspend fun upsert(
        conn: FakeWireConnection,
        docId: KdbUuid,
        json: String,
        corr: Int,
    ): WireMessage.UpsertResult {
        val frame =
            wire.encode(
                WireMessage.Upsert(
                    WireHeader(WireMessageType.UPSERT, KDB_WIRE_PROTOCOL_VERSION, corr, 0),
                    namespace = ns,
                    docId = docId.toString(),
                    json = json,
                ),
            )
        return wire.decode(conn.roundTrip(frame)) as WireMessage.UpsertResult
    }

    /** See SqlWireDisconnectCleanupTest's identically-named class for the rationale. */
    private class FakeWireConnection : WireConnection {
        private val inbound = Channel<ByteArray>(Channel.UNLIMITED)
        private val outbound = Channel<ByteArray>(Channel.UNLIMITED)

        suspend fun roundTrip(frame: ByteArray): ByteArray {
            inbound.send(frame)
            return outbound.receive()
        }

        override suspend fun send(frame: ByteArray) {
            outbound.send(frame)
        }

        override fun incoming(): Flow<ByteArray> = inbound.receiveAsFlow()

        override suspend fun close() {
            inbound.close()
        }
    }
}
