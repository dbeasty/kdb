package dev.kdb.index

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * kdb-index (1622 LOC) had no test source set at all. VersionedIndexEngine is what every index
 * store is built on - put, delete, lookup and range all go through it - so this covers its
 * semantics directly, and pins the ones where it has to agree with Go's VersionedEngine.
 */
class VersionedIndexEngineTest {
    private suspend fun engineWith(
        ns: String,
        key: IndexKey,
        count: Int,
    ): Pair<VersionedIndexEngine, List<KdbUuid>> {
        val dag = inMemoryCommitDag(ns)
        val head = dag.head()
        val engine = VersionedIndexEngine(dag)
        val ids = (0 until count).map { KdbUuid.random() }
        for (id in ids) {
            engine.put(IndexEntry(id, key, head))
        }
        return engine to ids
    }

    @Test
    fun putThenLookupReturnsTheDocument() =
        runTest {
            val dag = inMemoryCommitDag("ns/index-basic")
            val head = dag.head()
            val engine = VersionedIndexEngine(dag)
            val docId = KdbUuid.random()
            val key = IndexKey.StringKey("active")

            engine.put(IndexEntry(docId, key, head))

            assertEquals(listOf(docId), engine.lookup(key, null))
            // A key nothing was filed under is empty, not an error and not everything.
            assertTrue(engine.lookup(IndexKey.StringKey("inactive"), null).isEmpty())
        }

    @Test
    fun deleteRemovesTheDocumentFromItsKey() =
        runTest {
            val dag = inMemoryCommitDag("ns/index-delete")
            val head = dag.head()
            val engine = VersionedIndexEngine(dag)
            val key = IndexKey.StringKey("active")
            val kept = KdbUuid.random()
            val removed = KdbUuid.random()
            engine.put(IndexEntry(kept, key, head))
            engine.put(IndexEntry(removed, key, head))

            engine.delete(removed, head)

            assertEquals(listOf(kept), engine.lookup(key, null))
        }

    @Test
    fun clearEmptiesTheIndex() =
        runTest {
            val (engine, _) = engineWith("ns/index-clear", IndexKey.StringKey("k"), 3)
            engine.clear()
            assertTrue(engine.lookup(IndexKey.StringKey("k"), null).isEmpty())
        }

    @Test
    fun bulkLoadFilesEveryEntry() =
        runTest {
            val dag = inMemoryCommitDag("ns/index-bulk")
            val head = dag.head()
            val engine = VersionedIndexEngine(dag)
            val ids = (0 until 5).map { KdbUuid.random() }
            engine.bulkLoad(ids.map { IndexEntry(it, IndexKey.StringKey("bulk"), head) })

            assertEquals(ids.toSet(), engine.lookup(IndexKey.StringKey("bulk"), null).toSet())
        }

    /**
     * A non-positive limit means "no limit", matching Go's rangeScan. Taken literally the
     * size check was already satisfied after the first document, so a limit of 0 - which a
     * caller means as "unbounded" - returned exactly one row here and everything on Go's side.
     */
    @Test
    fun rangeTreatsNonPositiveLimitAsUnlimited() =
        runTest {
            val (engine, ids) = engineWith("ns/index-limit-zero", IndexKey.StringKey("same"), 6)
            for (limit in listOf(0, -1, -100)) {
                assertEquals(
                    ids.size,
                    engine.range(null, null, null, limit, true).size,
                    "limit $limit did not mean unlimited",
                )
            }
        }

    @Test
    fun rangeLimitBeyondTheDataReturnsEverything() =
        runTest {
            val (engine, ids) = engineWith("ns/index-limit-large", IndexKey.StringKey("same"), 4)
            assertEquals(ids.size, engine.range(null, null, null, 1000, true).size)
        }

    /** A limited range must return the same documents every time it runs. */
    @Test
    fun rangeWithLimitIsRepeatable() =
        runTest {
            val (engine, _) = engineWith("ns/index-repeatable", IndexKey.StringKey("same"), 10)
            val first = engine.range(null, null, null, 3, true)
            assertEquals(3, first.size)
            repeat(20) {
                assertEquals(first, engine.range(null, null, null, 3, true))
            }
        }

    /** Paging must partition rather than overlap: each limit extends the one before it. */
    @Test
    fun rangeLimitsAreNestedPrefixes() =
        runTest {
            val (engine, ids) = engineWith("ns/index-prefix", IndexKey.StringKey("same"), 8)
            var prev = engine.range(null, null, null, 1, true)
            for (k in 2..ids.size) {
                val got = engine.range(null, null, null, k, true)
                assertEquals(k, got.size, "limit $k returned ${got.size}")
                assertEquals(prev, got.take(prev.size), "limit $k is not an extension of limit ${prev.size}")
                prev = got
            }
        }

    @Test
    fun rangeKeyBoundsAreInclusiveAndDirectionReverses() =
        runTest {
            val dag = inMemoryCommitDag("ns/index-bounds")
            val head = dag.head()
            val engine = VersionedIndexEngine(dag)
            val byKey =
                listOf("a", "b", "c", "d", "e").associateWith { k ->
                    KdbUuid.random().also { engine.put(IndexEntry(it, IndexKey.StringKey(k), head)) }
                }

            assertEquals(
                listOf(byKey.getValue("b"), byKey.getValue("c"), byKey.getValue("d")),
                engine.range(IndexKey.StringKey("b"), IndexKey.StringKey("d"), null, 0, true),
                "bounds should be inclusive at both ends",
            )
            assertEquals(
                listOf(byKey.getValue("d"), byKey.getValue("c"), byKey.getValue("b")),
                engine.range(IndexKey.StringKey("b"), IndexKey.StringKey("d"), null, 0, false),
                "descending should reverse key order",
            )
            assertEquals(
                listOf(byKey.getValue("a"), byKey.getValue("b")),
                engine.range(null, IndexKey.StringKey("b"), null, 0, true),
                "an open lower bound should reach the first key",
            )
            assertEquals(
                listOf(byKey.getValue("d"), byKey.getValue("e")),
                engine.range(IndexKey.StringKey("d"), null, null, 0, true),
                "an open upper bound should reach the last key",
            )
            assertTrue(
                engine.range(IndexKey.StringKey("x"), IndexKey.StringKey("z"), null, 0, true).isEmpty(),
                "a range matching nothing should be empty, not everything",
            )
        }

    @Test
    fun snapshotRestoresIntoAnotherEngine() =
        runTest {
            val dag = inMemoryCommitDag("ns/index-snapshot")
            val head = dag.head()
            val engine = VersionedIndexEngine(dag)
            val key = IndexKey.StringKey("active")
            val docId = KdbUuid.random()
            engine.put(IndexEntry(docId, key, head))

            val restored = VersionedIndexEngine(dag)
            restored.restoreSnapshotBytes(engine.snapshotBytes())

            assertEquals(listOf(docId), restored.lookup(key, null))
        }

    @Test
    fun isValidReportsWhetherTheCommitIsKnownToTheIndex() =
        runTest {
            val dag = inMemoryCommitDag("ns/index-valid")
            val head = dag.head()
            val engine = VersionedIndexEngine(dag)
            engine.put(IndexEntry(KdbUuid.random(), IndexKey.StringKey("k"), head))

            assertTrue(engine.isValid(head))
            assertTrue(
                !engine.isValid(KdbHash.fromHex("ab".repeat(32))),
                "a commit the index has never seen should not be reported valid",
            )
        }
}

/**
 * The snapshot format writes one line per event. These pin the parts of it that are easy to get
 * wrong: the hash encoding (interpolating a KdbHash wrote an object identity, so a snapshot was
 * never restorable), and key values that contain the format's own separators.
 */
class VersionedIndexSnapshotTest {
    private suspend fun roundTrip(
        ns: String,
        entries: List<Pair<KdbUuid, IndexKey>>,
    ): Pair<VersionedIndexEngine, VersionedIndexEngine> {
        val dag = inMemoryCommitDag(ns)
        val head = dag.head()
        val source = VersionedIndexEngine(dag)
        for ((id, key) in entries) {
            source.put(IndexEntry(id, key, head))
        }
        val restored = VersionedIndexEngine(dag)
        restored.restoreSnapshotBytes(source.snapshotBytes())
        return source to restored
    }

    @Test
    fun snapshotBytesAreRestorable() =
        runTest {
            val docId = KdbUuid.random()
            val key = IndexKey.StringKey("active")
            val (_, restored) = roundTrip("ns/snap-basic", listOf(docId to key))
            assertEquals(listOf(docId), restored.lookup(key, null))
        }

    @Test
    fun snapshotCarriesEveryKeyType() =
        runTest {
            val entries =
                listOf(
                    KdbUuid.random() to IndexKey.StringKey("s"),
                    KdbUuid.random() to IndexKey.Int32Key(7),
                    KdbUuid.random() to IndexKey.Int64Key(1L shl 40),
                    KdbUuid.random() to IndexKey.Float64Key(1.5),
                    KdbUuid.random() to IndexKey.BoolKey(true),
                    KdbUuid.random() to IndexKey.TimestampKey(1_700_000_000_000L),
                    KdbUuid.random() to IndexKey.UuidKey(KdbUuid.random()),
                )
            val (_, restored) = roundTrip("ns/snap-keytypes", entries)
            for ((id, key) in entries) {
                assertEquals(listOf(id), restored.lookup(key, null), "key $key did not survive")
            }
        }

    @Test
    fun snapshotSurvivesADeleteEvent() =
        runTest {
            val dag = inMemoryCommitDag("ns/snap-delete")
            val head = dag.head()
            val source = VersionedIndexEngine(dag)
            val key = IndexKey.StringKey("k")
            val kept = KdbUuid.random()
            val removed = KdbUuid.random()
            source.put(IndexEntry(kept, key, head))
            source.put(IndexEntry(removed, key, head))
            source.delete(removed, head)

            val restored = VersionedIndexEngine(dag)
            restored.restoreSnapshotBytes(source.snapshotBytes())

            assertEquals(listOf(kept), restored.lookup(key, null))
        }

    @Test
    fun restoreReplacesWhateverWasThereBefore() =
        runTest {
            val dag = inMemoryCommitDag("ns/snap-replace")
            val head = dag.head()
            val source = VersionedIndexEngine(dag)
            val keptKey = IndexKey.StringKey("from-snapshot")
            val keptId = KdbUuid.random()
            source.put(IndexEntry(keptId, keptKey, head))

            val target = VersionedIndexEngine(dag)
            val staleKey = IndexKey.StringKey("stale")
            target.put(IndexEntry(KdbUuid.random(), staleKey, head))
            target.restoreSnapshotBytes(source.snapshotBytes())

            assertEquals(listOf(keptId), target.lookup(keptKey, null))
            assertTrue(
                target.lookup(staleKey, null).isEmpty(),
                "restore left an entry that was not in the snapshot",
            )
        }

    @Test
    fun snapshotOfAnEmptyIndexRestoresToAnEmptyIndex() =
        runTest {
            val dag = inMemoryCommitDag("ns/snap-empty")
            val source = VersionedIndexEngine(dag)
            val restored = VersionedIndexEngine(dag)
            restored.restoreSnapshotBytes(source.snapshotBytes())
            assertTrue(restored.lookup(IndexKey.StringKey("anything"), null).isEmpty())
        }

    /** The line format is pipe-separated, so a key value containing a pipe must still survive. */
    @Test
    fun snapshotSurvivesAKeyContainingTheFieldSeparator() =
        runTest {
            val docId = KdbUuid.random()
            val key = IndexKey.StringKey("a|b|c")
            val (_, restored) = roundTrip("ns/snap-pipe", listOf(docId to key))
            assertEquals(listOf(docId), restored.lookup(key, null))
        }
}
