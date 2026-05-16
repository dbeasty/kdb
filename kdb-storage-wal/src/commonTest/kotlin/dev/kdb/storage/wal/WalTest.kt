package dev.kdb.storage.wal

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.document.kdbSha256
import dev.kdb.storage.StorageEngineConfig
import dev.kdb.storage.mem.InMemoryPlatformIoShim
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

class WalTest {
  @Test
  fun appendAndRecover_roundTrip() = runTest {
        val shim = InMemoryPlatformIoShim()
        val config = StorageEngineConfig(globalMemoryBudgetBytes = 1_000_000, ioShim = shim)
        val wal = DefaultWriteAheadLogFactory().openOrCreate("ns1", config, shim)
        val hash = KdbHash.fromBytes(kdbSha256(byteArrayOf(1, 2, 3)))
        wal.append(
            WalRecord(0, KdbTimestamp.now(), WalRecordKind.PutBlob, WalPutBlob(hash, byteArrayOf(1, 2, 3)).encode()),
        )
        wal.sync()
        var count = 0
        val summary = wal.recover { count++ }
        assertEquals(1, summary.recordsReplayed)
        assertEquals(1, count)
    }

    private fun WalPutBlob.encode(): ByteArray = contentHash.bytes + bytes
}
