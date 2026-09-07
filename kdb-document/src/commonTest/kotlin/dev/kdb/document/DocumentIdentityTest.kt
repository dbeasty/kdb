package dev.kdb.document

import dev.kdb.codec.KdbUuid
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Component 72 identity rules (kdb-spec-layer16 §9.4). The cross-tree fixture parity check lives in
 * kdb-integration's DerivedIdVectorsTest; these pin the rules themselves. */
class DocumentIdentityTest {

    /** Guards: the derived id is the spec's uuid8(sha256(namespace ‖ utf8(s))) - one vector copied from
     * the shared fixture so a silent change to the namespace bytes or bit-forcing shows up here. */
    @Test
    fun derivedIdMatchesTheSharedFixtureVector() {
        assertEquals("54d100db-b8d0-8b38-8755-264670b3fc47", derivedDocumentId("order-1").toString())
        assertEquals("84994230-081b-8414-9065-14f4f0cf226e", derivedDocumentId("a").toString())
    }

    /** Guards: derived ids always carry version nibble 8 and variant bits 10, and are stable. */
    @Test
    fun derivedIdHasVersion8AndVariant10AndIsDeterministic() {
        val a = derivedDocumentId("user:42").toString()
        assertEquals(a, derivedDocumentId("user:42").toString())
        assertEquals('8', a[14], "version nibble must be 8 in $a")
        assertTrue(a[19] in "89ab", "variant bits must be 10 in $a")
    }

    /** Guards: a UUID string in `id` is the identity itself, not re-derived. */
    @Test
    fun uuidStringIdIsTheIdentity() {
        val id = KdbUuid.random()
        val resolved = resolveDocumentId("""{"name":"x","id":"$id"}""")
        assertEquals(id, resolved.id)
        assertTrue(resolved.supplied)
    }

    /** Guards: a non-UUID string maps through the derived id. */
    @Test
    fun naturalKeyIdMapsToTheDerivedId() {
        val resolved = resolveDocumentId("""{"id":"order-1"}""")
        assertEquals(derivedDocumentId("order-1"), resolved.id)
        assertTrue(resolved.supplied)
    }

    /** Guards: no `id` means a fresh random id is minted and reported as not supplied. */
    @Test
    fun absentIdMintsARandomOne() {
        val a = resolveDocumentId("""{"v":1}""")
        val b = resolveDocumentId("""{"v":1}""")
        assertFalse(a.supplied)
        assertTrue(a.id != b.id)
    }

    /** Guards: a non-string or empty `id` is rejected rather than silently replaced. */
    @Test
    fun nonStringOrEmptyIdIsRejected() {
        assertFailsWith<DocumentDecodeException> { resolveDocumentId("""{"id":42}""") }
        assertFailsWith<DocumentDecodeException> { resolveDocumentId("""{"id":""}""") }
        assertFailsWith<DocumentDecodeException> { resolveDocumentId("""{"id":null}""") }
        assertFailsWith<DocumentDecodeException> { resolveDocumentId("""[1]""") }
    }
}
