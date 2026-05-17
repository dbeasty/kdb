package dev.kdb.cli

import dev.kdb.peersync.PeerHostConfig
import dev.kdb.peersync.peerSyncHost
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.wire.defaultWireCodec
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking

class PeerSyncFeatureTest {
    @Test
    fun sync_networkUriRejectedWhenFeatureDisabled() {
        val dir = createTempDirectory("kdb-cli-sync").toString()
        assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", "app/t")))
        val code =
            KdbCli.run(
                arrayOf(
                    "--data-dir",
                    dir,
                    "sync",
                    "app/t",
                    "kdb-tcp://127.0.0.1:7443",
                ),
            )
        assertEquals(2, code)
    }

    @Test
    fun sync_memoryAllowedWhenFeatureDisabled() =
        runBlocking {
            val dir = createTempDirectory("kdb-cli-sync-mem").toString()
            val hub = "feat-mem-hub"
            val ns = "app/t"
            val wire = defaultWireCodec()
            val dag = inMemoryCommitDag(ns)
            val host = peerSyncHost(wire, dag, InMemoryStorageAdapter())
            host.start(PeerHostConfig(ns, "host", hub))
            assertEquals(0, KdbCli.run(arrayOf("--data-dir", dir, "init", ns)))
            val code =
                KdbCli.run(
                    arrayOf(
                        "--data-dir",
                        dir,
                        "--quiet",
                        "sync",
                        ns,
                        "memory://$hub",
                    ),
                )
            host.stop()
            assertEquals(0, code)
        }
}
