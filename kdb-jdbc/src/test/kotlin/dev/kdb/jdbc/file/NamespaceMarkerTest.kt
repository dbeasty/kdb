package dev.kdb.jdbc.file

import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class NamespaceMarkerTest {
    private fun writeMarker(
        dir: String,
        ns: String,
        json: String,
    ) {
        NamespacePaths.ensureDirs(dir, ns)
        NamespacePaths.metaFile(dir, ns).toFile().writeText(json)
    }

    @Test
    fun shallowRootsInGosMarker_refuseTheOpen() {
        val dir = createTempDirectory("kdb-shallow").toString()
        val root = "ab".repeat(32)
        writeMarker(dir, "app/data", """{"namespaceId":"app/data","shallowRoots":["$root"],"bodiesExternal":true}""")
        val e = assertFailsWith<ShallowNamespaceException> { openFileRuntime(dir, "app", "app/data", acquireDirectoryLock = false) }
        assertEquals(listOf(root), e.shallowRoots)
        assertTrue(e.message!!.contains("shallow root"), e.message)
    }

    @Test
    fun markersWithoutShallowRoots_open() {
        val dir = createTempDirectory("kdb-deep").toString()
        // Go's marker once a deepen has emptied the roots, and the CLI's own marker.
        writeMarker(dir, "app/deepened", """{"namespaceId":"app/deepened","bodiesExternal":true}""")
        writeMarker(dir, "app/cli", """{"namespaceId":"app/cli","createdAt":"1"}""")
        writeMarker(dir, "app/garbled", "not json")
        for (ns in listOf("app/deepened", "app/cli", "app/garbled", "app/none")) {
            assertEquals(emptyList(), NamespaceMarker.shallowRoots(dir, ns))
            openFileRuntime(dir, "app", ns, acquireDirectoryLock = false)
        }
    }
}
