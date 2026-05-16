package dev.kdb.policy

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NamespacePolicyTest {
    @Test
    fun putRoundTrip() =
        runTest {
            val reg = inMemoryNamespacePolicyRegistry()
            val policy = defaultMutable("app/users")
            reg.put(policy)
            assertEquals(1L, reg.get("app/users").revision)
        }

    @Test
    fun defaultWhenMissing() =
        runTest {
            val reg = inMemoryNamespacePolicyRegistry()
            assertEquals(HistoryMode.FULL, reg.get("new/ns").history)
        }

    @Test
    fun parseEventsNeverSquash() {
        val p =
            defaultNamespacePolicyParser().parse(
                """{"namespaceId":"app/events","mode":"APPEND_ONLY","compaction":{"squashAfter":"NEVER"}}""",
            )
        assertEquals(SquashMode.NEVER, p.compaction.squashAfter)
    }

    @Test
    fun validateRetainOrdering() {
        val bad =
            NamespacePolicy(
                namespaceId = "x",
                schema = null,
                mode = NamespaceMode.MUTABLE,
                history = HistoryMode.FULL,
                conflict = dev.kdb.transaction.ConflictPolicy.STRICT,
                compaction =
                    CompactionPolicy(
                        retainGranularity =
                            listOf(
                                RetainRule(30, RetainStrategy.DAILY_SNAPSHOTS),
                                RetainRule(7, RetainStrategy.FULL_HISTORY),
                            ),
                    ),
            )
        val r = DefaultPolicyValidator.validate(bad)
        assertFalse(r.ok)
    }

    @Test
    fun evaluatorNeverSquash() {
        val plans =
            DefaultCompactionPolicyEvaluator.boundaryCandidates(
                CompactionPolicy(squashAfter = SquashMode.NEVER),
                emptyMap(),
                emptySet(),
                emptySet(),
                KdbHash.fromBytes(ByteArray(32)),
            ) { null }
        assertTrue(plans.isEmpty())
    }

    @Test
    fun jsonRoundTrip() {
        val policy = cacheNoHistory("app/cache")
        val bytes = encodePolicy(policy)
        val decoded = decodePolicy(bytes, schema = null)
        assertEquals(policy.history, decoded.history)
        assertEquals(policy.compaction.squashAfter, decoded.compaction.squashAfter)
    }

    @Test
    fun cachePolicyHistoryNone() {
        val p = cacheNoHistory("c")
        assertEquals(HistoryMode.NONE, p.history)
    }

    @Test
    fun appendOnlyPreset() {
        val schema =
            KdbSchema.build(
                listOf(SchemaField("eventType", KdbFieldType.StringType, required = true, indexed = true)),
            )
        val p = appendOnlyEvents("app/events", schema)
        assertEquals(NamespaceMode.APPEND_ONLY, p.mode)
        assertEquals(SquashMode.NEVER, p.compaction.squashAfter)
    }
}
