package dev.kdb.policy

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/** Layer 16 §9.5: the expiry block on NamespacePolicy. */
class DocumentExpiryPolicyTest {

    /** Guards: documentExpiry round-trips through the JSON codec with its defaults. */
    @Test
    fun expiryRoundTripsThroughJson() {
        val p =
            defaultNamespacePolicyParser().parse(
                """{"namespaceId":"app/t","documentExpiry":{"fieldPath":"expiresAt","graceMillis":5000}}""",
            )
        assertEquals(DocumentExpiryPolicy("expiresAt", 5000, 60_000), p.documentExpiry)
        val back = decodePolicy(encodePolicy(p), null)
        assertEquals(p.documentExpiry, back.documentExpiry)
    }

    /** Guards: a policy without the block decodes to null (default off). */
    @Test
    fun absentExpiryIsNull() {
        val p = defaultNamespacePolicyParser().parse("""{"namespaceId":"app/t"}""")
        assertNull(p.documentExpiry)
    }

    /** Guards: a blank field path or a non-positive sweep interval is rejected by the validator. */
    @Test
    fun invalidExpiryIsRejectedOnPut() =
        runTest {
            val reg = inMemoryNamespacePolicyRegistry()
            assertFailsWith<PolicyValidationException> {
                reg.put(defaultMutable("app/t").copy(documentExpiry = DocumentExpiryPolicy(" ", 0, 1000)))
            }
            assertFailsWith<PolicyValidationException> {
                reg.put(defaultMutable("app/t").copy(documentExpiry = DocumentExpiryPolicy("ttl", 0, 0)))
            }
            reg.put(defaultMutable("app/t").copy(documentExpiry = DocumentExpiryPolicy("ttl", 0, 1000)))
            assertEquals("ttl", reg.get("app/t").documentExpiry?.fieldPath)
        }
}
