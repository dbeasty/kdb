package dev.kdb.transaction

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.ViolationType
import dev.kdb.schema.KdbFieldType
import dev.kdb.schema.KdbSchema
import dev.kdb.schema.SchemaField
import dev.kdb.schema.UniqueConstraint
import dev.kdb.storage.mem.InMemoryStorageAdapter
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Layer 16 §9.6: compound unique constraints enforced at commit through the UniqueKeyRegistry. */
class UniqueKeyRegistryTest {
    private val ns = "app/orders"

    private fun schema(
        compound: Boolean = true,
        singleUnique: Boolean = false,
    ): KdbSchema =
        KdbSchema.build(
            listOf(
                SchemaField("tenant", KdbFieldType.StringType, required = false, indexed = true),
                SchemaField("code", KdbFieldType.StringType, required = false, indexed = true, unique = singleUnique),
            ),
            uniqueConstraints = if (compound) listOf(UniqueConstraint("tenant", "code")) else emptyList(),
        )

    private fun doc(json: String, id: KdbUuid = KdbUuid.random()) = KdbDocument(id, json)

    private fun tx(base: dev.kdb.codec.KdbHash, vararg ops: KdbOp) =
        KdbTransaction(KdbUuid.random(), base, ops.toList(), KdbTimestamp.now(), KdbUuid.random())

    /** Guards: the key is the ordered field tuple plus canonical values, so a compound constraint only
     * collides when every part matches - and `1` and `1.0` are the same value. */
    @Test
    fun uniqueKeysForBuildsTheCanonicalCompoundTuple() {
        val s = schema()
        val a = uniqueKeysFor(ns, s, doc("""{"tenant":"t1","code":"c1"}"""))
        val b = uniqueKeysFor(ns, s, doc("""{"code":"c1","tenant":"t1"}"""))
        assertEquals(1, a.size)
        assertEquals(listOf("tenant", "code"), a[0].fields)
        assertEquals(a[0], b[0], "key order must follow the constraint, not the document's key order")

        val numeric =
            KdbSchema.build(
                listOf(SchemaField("n", KdbFieldType.Float64Type, required = false, indexed = true)),
                uniqueConstraints = listOf(UniqueConstraint("n")),
            )
        assertEquals(
            uniqueKeysFor(ns, numeric, doc("""{"n":1}""")),
            uniqueKeysFor(ns, numeric, doc("""{"n":1.0}""")).map { it.copy() },
        )
    }

    /** Guards: sparse semantics - a document missing (or nulling) any part of a tuple claims nothing. */
    @Test
    fun aDocumentMissingAnyPartOfTheTupleClaimsNothing() {
        val s = schema()
        assertTrue(uniqueKeysFor(ns, s, doc("""{"tenant":"t1"}""")).isEmpty())
        assertTrue(uniqueKeysFor(ns, s, doc("""{"tenant":"t1","code":null}""")).isEmpty())
        assertEquals(1, uniqueKeysFor(ns, s, doc("""{"tenant":"t1","code":"c1"}""")).size)
    }

    /** Guards: apply() retracts before claiming, and a stale retraction never frees another owner's key. */
    @Test
    fun applyRetractsThenClaimsAndIgnoresStaleRetractions() =
        runTest {
            val registry = UniqueKeyRegistry()
            val key = UniqueKey(ns, listOf("tenant", "code"), listOf("\"t1\"", "\"c1\""))
            val a = KdbUuid.random()
            val b = KdbUuid.random()
            registry.apply(emptyMap(), mapOf(key to a))
            assertEquals(a, registry.owner(key))
            // A retraction naming the wrong owner must not free the key.
            registry.apply(mapOf(key to b), emptyMap())
            assertEquals(a, registry.owner(key))
            // Moving the value from a to b in one step leaves b holding it, never nobody.
            registry.apply(mapOf(key to a), mapOf(key to b))
            assertEquals(b, registry.owner(key))
        }

    /** Guards: a second document claiming the same compound tuple is rejected at commit. */
    @Test
    fun compoundCollisionIsRejectedAtCommit() =
        runTest {
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val registry = UniqueKeyRegistry()
            val engine = transactionEngine(ConflictPolicy.LAST_WRITE, uniqueKeys = registry)
            val s = schema()

            assertIs<TransactionResult.Success>(
                engine.commit(tx(dag.head(), KdbOp.Write(KdbUuid.random(), """{"tenant":"t1","code":"c1"}""")), dag, storage, s),
            )
            // Same code under a different tenant is fine - only the whole tuple is unique.
            assertIs<TransactionResult.Success>(
                engine.commit(tx(dag.head(), KdbOp.Write(KdbUuid.random(), """{"tenant":"t2","code":"c1"}""")), dag, storage, s),
            )
            val clash =
                engine.commit(tx(dag.head(), KdbOp.Write(KdbUuid.random(), """{"tenant":"t1","code":"c1"}""")), dag, storage, s)
            val err = assertIs<TransactionResult.SchemaError>(clash)
            assertEquals(ViolationType.UNIQUE_CONSTRAINT, err.violations.single().violations.single().violationType)
            assertEquals("tenant,code", err.violations.single().violations.single().fieldName)
        }

    /** Guards: rewriting the same document keeps its own claim, and a value freed in the same
     * transaction can be taken by another document (a swap must not self-collide). */
    @Test
    fun rewritingAndHandingOverAValueAreBothAllowed() =
        runTest {
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val registry = UniqueKeyRegistry()
            val engine = transactionEngine(ConflictPolicy.LAST_WRITE, uniqueKeys = registry)
            val s = schema()
            val a = KdbUuid.random()
            val b = KdbUuid.random()

            assertIs<TransactionResult.Success>(
                engine.commit(tx(dag.head(), KdbOp.Write(a, """{"tenant":"t1","code":"c1"}""")), dag, storage, s),
            )
            // Rewriting a with the same tuple is not a self-collision.
            assertIs<TransactionResult.Success>(
                engine.commit(tx(dag.head(), KdbOp.Write(a, """{"tenant":"t1","code":"c1"}""")), dag, storage, s),
            )
            // a releases the value and b takes it, atomically.
            assertIs<TransactionResult.Success>(
                engine.commit(
                    tx(
                        dag.head(),
                        KdbOp.Delete(a),
                        KdbOp.Write(b, """{"tenant":"t1","code":"c1"}"""),
                    ),
                    dag,
                    storage,
                    s,
                ),
            )
            assertEquals(b, registry.owner(UniqueKey(ns, listOf("tenant", "code"), listOf("\"t1\"", "\"c1\""))))
        }

    /** Guards: a single-field `UNIQUE` runs through the same mechanism as the 1-tuple case. */
    @Test
    fun singleFieldUniqueIsTheOneTupleCase() =
        runTest {
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val registry = UniqueKeyRegistry()
            val engine = transactionEngine(ConflictPolicy.LAST_WRITE, uniqueKeys = registry)
            val s = schema(compound = false, singleUnique = true)

            assertIs<TransactionResult.Success>(
                engine.commit(tx(dag.head(), KdbOp.Write(KdbUuid.random(), """{"code":"c1"}""")), dag, storage, s),
            )
            val clash = engine.commit(tx(dag.head(), KdbOp.Write(KdbUuid.random(), """{"code":"c1"}""")), dag, storage, s)
            assertIs<TransactionResult.SchemaError>(clash)
            assertEquals(1, registry.size())
        }

    /** Guards: rebuild repopulates from an existing document tree (the "rebuild at open" path), and
     * reports data that already violates the constraint instead of silently picking a winner. */
    @Test
    fun rebuildRepopulatesFromStorageAndReportsExistingDuplicates() =
        runTest {
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val s = schema()
            // Two colliding documents written without any registry in play (as if by an older build).
            val plain = transactionEngine(ConflictPolicy.LAST_WRITE)
            assertIs<TransactionResult.Success>(
                plain.commit(tx(dag.head(), KdbOp.Write(KdbUuid.random(), """{"tenant":"t1","code":"c1"}""")), dag, storage, s),
            )
            val head1 = dag.head()
            val tree1 = dag.getCommitOrThrow(head1).documentTreeHash
            val fresh = UniqueKeyRegistry()
            fresh.rebuild(ns, storage, tree1, s)
            assertEquals(1, fresh.size())
            assertNull(fresh.owner(UniqueKey(ns, listOf("tenant", "code"), listOf("\"t9\"", "\"c9\""))))

            assertIs<TransactionResult.Success>(
                plain.commit(tx(dag.head(), KdbOp.Write(KdbUuid.random(), """{"tenant":"t1","code":"c1"}""")), dag, storage, s),
            )
            val tree2 = dag.getCommitOrThrow(dag.head()).documentTreeHash
            assertFailsWith<UniqueConstraintViolationException> {
                UniqueKeyRegistry().rebuild(ns, storage, tree2, s)
            }
        }

    /** Guards: two ops in one transaction claiming the same tuple is a violation - atomicity does not
     * launder a self-collision. */
    @Test
    fun twoWritesInOneTransactionCannotClaimTheSameTuple() =
        runTest {
            val dag = inMemoryCommitDag(ns)
            val storage = InMemoryStorageAdapter()
            val engine = transactionEngine(ConflictPolicy.LAST_WRITE, uniqueKeys = UniqueKeyRegistry())
            val result =
                engine.commit(
                    tx(
                        dag.head(),
                        KdbOp.Write(KdbUuid.random(), """{"tenant":"t1","code":"c1"}"""),
                        KdbOp.Write(KdbUuid.random(), """{"tenant":"t1","code":"c1"}"""),
                    ),
                    dag,
                    storage,
                    schema(),
                )
            assertIs<TransactionResult.SchemaError>(result)
        }
}
