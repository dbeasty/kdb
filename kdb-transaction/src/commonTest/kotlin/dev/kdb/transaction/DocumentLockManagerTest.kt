package dev.kdb.transaction

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.document.KdbOp
import dev.kdb.document.KdbTransaction
import dev.kdb.error.DocumentLockedException
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class DocumentLockManagerTest {
  private val ns = "demo/users"
  private val doc = KdbUuid.random()
  private val locks = DocumentLockManager()

  @Test
  fun acquireAndRelease() =
      runTest {
        locks.tryAcquire(ns, doc, "sess-a")
        locks.release(ns, doc, "sess-a")
        locks.tryAcquire(ns, doc, "sess-b")
      }

  @Test
  fun reentrantSameSession() =
      runTest {
        locks.tryAcquire(ns, doc, "sess-a")
        locks.tryAcquire(ns, doc, "sess-a")
        locks.release(ns, doc, "sess-a")
        locks.tryAcquire(ns, doc, "sess-b")
      }

  @Test
  fun conflictDifferentSession() =
      runTest {
        locks.tryAcquire(ns, doc, "sess-a")
        assertFailsWith<DocumentLockedException> { locks.tryAcquire(ns, doc, "sess-b") }
      }

  @Test
  fun releaseAllClearsSession() =
      runTest {
        val doc2 = KdbUuid.random()
        locks.tryAcquire(ns, doc, "sess-a")
        locks.tryAcquire(ns, doc2, "sess-a")
        locks.releaseAll("sess-a")
        locks.tryAcquire(ns, doc, "sess-b")
        locks.tryAcquire(ns, doc2, "sess-b")
      }

  @Test
  fun acquireAllForTransaction() =
      runTest {
        val doc2 = KdbUuid.random()
        val tx =
            KdbTransaction(
                id = KdbUuid.random(),
                baseVersion =
                    KdbHash.fromHex(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    ),
                operations =
                    listOf(
                        KdbOp.Write(doc, """{"id":"${doc.toString()}"}"""),
                        KdbOp.Delete(doc2),
                    ),
                timestamp = KdbTimestamp.now(),
                authorNodeId = KdbUuid.random(),
            )
        locks.acquireAllForTransaction(ns, "sess-a", tx)
        assertFailsWith<DocumentLockedException> { locks.tryAcquire(ns, doc, "sess-b") }
        assertFailsWith<DocumentLockedException> { locks.tryAcquire(ns, doc2, "sess-b") }
        locks.releaseAll("sess-a")
        assertEquals(2, documentIdsIn(tx).size)
      }
}
