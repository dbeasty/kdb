package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class IndexCatalogTest {

    private fun descriptor(
        field: String,
        type: IndexType,
        options: Map<String, String> = emptyMap(),
        namespaceId: String = "ns",
    ) = IndexDescriptor(
        indexId = KdbUuid.random(),
        namespaceId = namespaceId,
        fieldName = field,
        fields = listOf(field),
        type = type,
        unique = false,
        schemaVersion = 3,
        createdAtHash = KdbHash.fromBytes(ByteArray(32) { 7 }),
        options = options,
    )

    /** Guards §9.2: a catalog round-trips through its bytes with every descriptor detail intact. */
    @Test
    fun catalogRoundTripsThroughBytes() {
        val vector =
            descriptor(
                "embedding",
                IndexType.VECTOR,
                mapOf("dimensions" to "768", "metric" to "cosine", "m" to "16"),
            )
        val fulltext =
            descriptor("title", IndexType.FULLTEXT, mapOf("weights" to "title=3,description=1"))
                .copy(fields = listOf("title", "description"))
        val catalog =
            IndexCatalog(
                "ns",
                listOf(
                    IndexCatalogEntry(vector, "tasks_vec"),
                    IndexCatalogEntry(fulltext, "tasks_text"),
                    IndexCatalogEntry(descriptor("email", IndexType.HASH), null),
                ),
            )

        val decoded = IndexCatalog.decode(catalog.encode())

        assertEquals(catalog, decoded)
        assertEquals(listOf("title", "description"), decoded.entries[1].descriptor.fields)
        assertEquals("768", decoded.entries[0].descriptor.options["dimensions"])
        assertNull(decoded.entries[2].sqlIndexName)
    }

    /** Guards the escaping: field paths and option values holding separators survive the round trip. */
    @Test
    fun escapesSeparatorsInPathsAndOptions() {
        val awkward =
            descriptor(
                "steps.text",
                IndexType.FULLTEXT,
                mapOf("weights" to "a|b=2,c=3", "note" to "x=y,z"),
                namespaceId = "ns|weird,name",
            ).copy(fields = listOf("steps.text", "a,b", "c|d"))
        val catalog = IndexCatalog("ns|weird,name", listOf(IndexCatalogEntry(awkward, "idx,name|1")))

        assertEquals(catalog, IndexCatalog.decode(catalog.encode()))
    }

    /** Guards §9.2 persistence: a saved catalog is found again under the namespace's blob key. */
    @Test
    fun catalogSavesAndLoadsThroughTheBlobStore() =
        runTest {
            val blobs = InMemoryIndexBlobStore()
            val catalog = IndexCatalog("ns", listOf(IndexCatalogEntry(descriptor("email", IndexType.HASH), "by_email")))

            catalog.save(blobs)

            assertEquals(catalog, IndexCatalog.load(blobs, "ns"))
            assertNull(IndexCatalog.load(blobs, "other"), "an unsaved namespace has no catalog")
        }

    /** Guards the blob key shape both trees agree on, so a catalog is findable after a restart. */
    @Test
    fun blobKeysAreDerivedFromTheIdentity() {
        val id = KdbUuid.fromString("11111111-1111-4111-8111-111111111111")
        assertEquals("index/$id/snapshot", indexSnapshotBlobKey(id))
        assertEquals("index/catalog/ns", indexCatalogBlobKey("ns"))
    }

    /** Guards the storage-backed blob store resolving its keyed pointer to the right bytes. */
    @Test
    fun storageBackedBlobStoreResolvesKeysToBytes() =
        runTest {
            val storage = dev.kdb.storage.mem.InMemoryStorageAdapter()
            val blobs = storageAdapterIndexBlobStore(storage)

            blobs.write("index/a/snapshot", "first".encodeToByteArray())
            blobs.write("index/b/snapshot", "second".encodeToByteArray())

            assertEquals("first", blobs.read("index/a/snapshot")?.decodeToString())
            assertEquals("second", blobs.read("index/b/snapshot")?.decodeToString())
            assertNull(blobs.read("index/c/snapshot"))

            // A rewrite under the same key replaces what the key resolves to.
            blobs.write("index/a/snapshot", "third".encodeToByteArray())
            assertEquals("third", blobs.read("index/a/snapshot")?.decodeToString())

            blobs.delete("index/a/snapshot")
            assertNull(blobs.read("index/a/snapshot"))
        }

    /** Guards the namespace invariant: a foreign descriptor is refused, never silently re-homed. */
    @Test
    fun rejectsADescriptorFromAnotherNamespace() {
        assertTrue(
            runCatching {
                IndexCatalog("ns", listOf(IndexCatalogEntry(descriptor("f", IndexType.HASH, namespaceId = "other"), null)))
            }.isFailure,
        )
    }

    /** Guards a corrupt or foreign blob being rejected rather than decoded into nonsense. */
    @Test
    fun rejectsBytesThatAreNotACatalog() {
        assertTrue(
            runCatching { IndexCatalog.decode("something else entirely".encodeToByteArray()) }.isFailure,
        )
    }
}
