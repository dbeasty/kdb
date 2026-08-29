package dev.kdb.index.btree

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.index.IndexDescriptor
import dev.kdb.index.IndexEntry
import dev.kdb.index.IndexKey
import dev.kdb.index.IndexType
import dev.kdb.index.IndexTypeMismatchException
import dev.kdb.index.UniqueIndexViolationException
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** This module had no tests. */
class DefaultBtreeIndexStoreTest {
    private suspend fun store(
        ns: String,
        unique: Boolean = false,
        type: IndexType = IndexType.BTREE,
    ): DefaultBtreeIndexStore {
        val dag = inMemoryCommitDag(ns)
        val descriptor =
            IndexDescriptor(
                indexId = KdbUuid.random(),
                namespaceId = ns,
                fieldName = "rank",
                fields = listOf("rank"),
                type = type,
                unique = unique,
                schemaVersion = 1,
                createdAtHash = dag.head(),
            )
        return DefaultBtreeIndexStore(descriptor, dag, InMemoryStorageAdapter())
    }

    @Test
    fun putThenLookup() =
        runTest {
            val s = store("ns/btree-basic")
            val dag = inMemoryCommitDag("ns/btree-basic")
            val docId = KdbUuid.random()
            val key = IndexKey.Int64Key(5)
            s.put(IndexEntry(docId, key, dag.head()))

            assertEquals(listOf(docId), s.lookup(key, null))
            assertTrue(s.lookup(IndexKey.Int64Key(6), null).isEmpty())
        }

    /** A BTREE index is ordered, which is the point of it: range queries must come back sorted. */
    @Test
    fun rangeReturnsKeysInOrder() =
        runTest {
            val s = store("ns/btree-range")
            val dag = inMemoryCommitDag("ns/btree-range")
            val head = dag.head()
            val byRank = (1..5).associateWith { KdbUuid.random() }
            // Inserted out of order, so a store that simply preserved insertion order would fail.
            for (rank in listOf(3, 1, 5, 2, 4)) {
                s.put(IndexEntry(byRank.getValue(rank), IndexKey.Int64Key(rank.toLong()), head))
            }

            assertEquals(
                (1..5).map { byRank.getValue(it) },
                s.range(null, null, null, 0, true),
            )
            assertEquals(
                (5 downTo 1).map { byRank.getValue(it) },
                s.range(null, null, null, 0, false),
            )
            assertEquals(
                (2..4).map { byRank.getValue(it) },
                s.range(IndexKey.Int64Key(2), IndexKey.Int64Key(4), null, 0, true),
                "range bounds should be inclusive",
            )
        }

    @Test
    fun uniqueIndexRejectsASecondDocumentUnderTheSameKey() =
        runTest {
            val s = store("ns/btree-unique", unique = true)
            val dag = inMemoryCommitDag("ns/btree-unique")
            val head = dag.head()
            val key = IndexKey.Int64Key(1)
            val first = KdbUuid.random()
            s.put(IndexEntry(first, key, head))

            assertFailsWith<UniqueIndexViolationException> {
                s.put(IndexEntry(KdbUuid.random(), key, head))
            }
            // The original entry is untouched by the rejected write.
            assertEquals(listOf(first), s.lookup(key, null))
        }

    /** Re-putting the same document under the same key is a no-op, not a violation of itself. */
    @Test
    fun uniqueIndexAllowsRewritingTheSameDocument() =
        runTest {
            val s = store("ns/btree-unique-rewrite", unique = true)
            val dag = inMemoryCommitDag("ns/btree-unique-rewrite")
            val head = dag.head()
            val key = IndexKey.Int64Key(1)
            val docId = KdbUuid.random()
            s.put(IndexEntry(docId, key, head))
            s.put(IndexEntry(docId, key, head))

            assertEquals(listOf(docId), s.lookup(key, null))
        }

    @Test
    fun nonUniqueIndexHoldsManyDocumentsUnderOneKey() =
        runTest {
            val s = store("ns/btree-multi")
            val dag = inMemoryCommitDag("ns/btree-multi")
            val head = dag.head()
            val key = IndexKey.Int64Key(1)
            val ids = (0 until 3).map { KdbUuid.random() }
            for (id in ids) s.put(IndexEntry(id, key, head))

            assertEquals(ids.toSet(), s.lookup(key, null).toSet())
        }

    @Test
    fun deleteRemovesTheDocument() =
        runTest {
            val s = store("ns/btree-delete")
            val dag = inMemoryCommitDag("ns/btree-delete")
            val head = dag.head()
            val key = IndexKey.Int64Key(1)
            val kept = KdbUuid.random()
            val removed = KdbUuid.random()
            s.put(IndexEntry(kept, key, head))
            s.put(IndexEntry(removed, key, head))

            s.delete(removed, head)

            assertEquals(listOf(kept), s.lookup(key, null))
        }

    @Test
    fun bulkLoadAndRebuildFileEveryEntry() =
        runTest {
            val s = store("ns/btree-bulk")
            val dag = inMemoryCommitDag("ns/btree-bulk")
            val head = dag.head()
            val entries = (1..4).map { IndexEntry(KdbUuid.random(), IndexKey.Int64Key(it.toLong()), head) }

            s.bulkLoad(entries)
            assertEquals(4, s.range(null, null, null, 0, true).size)

            s.clear()
            assertTrue(s.range(null, null, null, 0, true).isEmpty())

            s.rebuild(entries)
            assertEquals(4, s.range(null, null, null, 0, true).size)
        }

    /**
     * A BTREE index cannot answer full-text or vector queries, and says so with a typed error
     * naming both the type asked for and the type it is - a caller routing a query needs to tell
     * "wrong index type" apart from "no results".
     */
    @Test
    fun unsupportedQueryKindsRaiseATypeMismatch() =
        runTest {
            val s = store("ns/btree-unsupported")
            val searchError = assertFailsWith<IndexTypeMismatchException> { s.search("anything", null, 10) }
            assertTrue(searchError.message!!.contains("SEARCH"), searchError.message!!)

            val vectorError =
                assertFailsWith<IndexTypeMismatchException> {
                    s.nearestNeighbours(floatArrayOf(1f, 2f), 3, null)
                }
            assertTrue(vectorError.message!!.contains("VECTOR"), vectorError.message!!)
        }

    @Test
    fun snapshotRestoresIntoAnotherStore() =
        runTest {
            val s = store("ns/btree-snapshot")
            val dag = inMemoryCommitDag("ns/btree-snapshot")
            val head = dag.head()
            val docId = KdbUuid.random()
            val key = IndexKey.Int64Key(9)
            s.put(IndexEntry(docId, key, head))

            val other = store("ns/btree-snapshot")
            other.restoreSnapshot(s.snapshot())

            assertEquals(listOf(docId), other.lookup(key, null))
        }

    /** The factory is type-checked: handing it a descriptor for another index type is a bug. */
    @Test
    fun factoryRefusesANonBtreeDescriptor() =
        runTest {
            val dag = inMemoryCommitDag("ns/btree-factory")
            val factory = btreeIndexStoreFactory(dag, InMemoryStorageAdapter())
            val hashDescriptor =
                IndexDescriptor(
                    indexId = KdbUuid.random(),
                    namespaceId = "ns/btree-factory",
                    fieldName = "rank",
                    fields = listOf("rank"),
                    type = IndexType.HASH,
                    unique = false,
                    schemaVersion = 1,
                    createdAtHash = dag.head(),
                )
            assertFailsWith<IllegalArgumentException> { factory.create(hashDescriptor) }
        }
}
