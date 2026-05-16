package dev.kdb.index.hash

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexEntry
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType
import dev.kdb.index.UniqueIndexViolationException
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class HashIndexTest {
    @Test
    fun putAndLookup_exact() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val head = dag.head()
            val store =
                DefaultHashIndexStore(
                    descriptor("email"),
                    dag,
                    InMemoryStorageAdapter(),
                )
            val doc = KdbUuid.random()
            store.put(IndexEntry(doc, IndexKey.StringKey("alice"), head))
            assertEquals(listOf(doc), store.lookup(IndexKey.StringKey("alice"), null))
        }

    @Test
    fun unique_rejectsDuplicate() =
        runTest {
            val dag = inMemoryCommitDag("ns")
            val head = dag.head()
            val store =
                DefaultHashIndexStore(
                    descriptor("email", unique = true),
                    dag,
                    InMemoryStorageAdapter(),
                )
            val d1 = KdbUuid.random()
            val d2 = KdbUuid.random()
            store.put(IndexEntry(d1, IndexKey.StringKey("x"), head))
            assertFailsWith<UniqueIndexViolationException> {
                store.put(IndexEntry(d2, IndexKey.StringKey("x"), head))
            }
        }

    private fun descriptor(
        field: String,
        unique: Boolean = false,
    ) = IndexDescriptor(
        indexId = KdbUuid.random(),
        namespaceId = "ns",
        fieldName = field,
        fields = listOf(field),
        type = IndexType.HASH,
        unique = unique,
        schemaVersion = 1,
        createdAtHash = KdbHash.fromBytes(ByteArray(32)),
    )
}
