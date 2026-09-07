package dev.kdb.integration

import dev.kdb.document.derivedDocumentId
import dev.kdb.document.resolveDocumentId
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.File
import java.nio.file.Paths
import kotlin.io.path.isDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Layer 16 §9.4 parity: the derived document id must agree with the Go tree on every vector in the
 * shared fixture go/testdata/golden/search/derived_id_vectors.json. The fixture is authored by the Go
 * side; if it is not present yet this test reports that and skips rather than failing the build.
 */
class DerivedIdVectorsTest {

    private val fixture: File? by lazy {
        val root = Paths.get(System.getProperty("user.dir"))
        val repo =
            generateSequence(root) { it.parent }
                .firstOrNull { it.resolve("go").isDirectory() }
                ?: root
        repo.resolve("go/testdata/golden/search/derived_id_vectors.json").toFile().takeIf { it.isFile }
    }

    @Test
    fun derivedIdsMatchTheGoFixture() {
        val file = fixture
        if (file == null) {
            println("SKIP: go/testdata/golden/search/derived_id_vectors.json is not present yet")
            return
        }
        val root = Json.parseToJsonElement(file.readText()).jsonObject
        val vectors = root["vectors"]!!.jsonArray
        assertTrue(vectors.isNotEmpty(), "fixture carries no vectors")
        for (v in vectors) {
            val obj = v.jsonObject
            val id = obj["id"]!!.jsonPrimitive.content
            val expected = obj["uuid"]!!.jsonPrimitive.content
            assertEquals(expected, derivedDocumentId(id).toString(), "derived id mismatch for input '$id'")
            // The same string arriving as a document body's `id` must resolve to the same identity.
            val body = """{"id":${kotlinx.serialization.json.JsonPrimitive(id)}}"""
            assertEquals(expected, resolveDocumentId(body).id.toString(), "resolveDocumentId mismatch for '$id'")
        }
    }
}
