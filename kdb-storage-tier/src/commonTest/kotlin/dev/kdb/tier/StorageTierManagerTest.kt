package dev.kdb.tier

import dev.kdb.codec.KdbHash
import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.DocumentTree
import dev.kdb.document.KdbCommit
import dev.kdb.document.KdbOp
import dev.kdb.document.computeCommitHash
import dev.kdb.error.ArchiveRestoreException
import dev.kdb.error.IceStorageException
import dev.kdb.error.VersionNotFoundException
import dev.kdb.policy.defaultMutable
import dev.kdb.policy.inMemoryNamespacePolicyRegistry
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.storage.manager.tier.DefaultDeltaLogTierRegistry
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class StorageTierManagerTest {
    @Test
    fun archiveCommitStubsDag() =
        runTest {
            val ns = "app/archive"
            val dag = inMemoryCommitDag(ns)
            val policies = inMemoryNamespacePolicyRegistry()
            policies.put(defaultMutable(ns))
            val manager =
                storageTierManager(
                    dag,
                    InMemoryStorageAdapter(),
                    DefaultDeltaLogTierRegistry(),
                    policyProvider = { policies.get(it) },
                )
            val head = dag.head()
            val tree = DocumentTree.EMPTY
            dag.putDocumentTree(tree)
            val commit =
                KdbCommit.build(
                    parentHashes = listOf(head),
                    namespaceId = ns,
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = emptyList(),
                    documentTreeHash = tree.treeHash,
                    schemaHash = null,
                )
            dag.putCommit(commit)
            dag.createTag("v1", commit.hash)
            val result =
                manager.archiveCommit(
                    ArchiveRequest(ns, commit.hash, tag = "v1"),
                )
            assertNull(dag.getCommit(commit.hash))
            assertNotNull(dag.getStub(commit.hash))
            assertEquals(result.bundleLocation, dag.getStub(commit.hash)?.archiveLocation)
        }

    @Test
    fun restoreIsolatedNamespace() =
        runTest {
            val ns = "app/live"
            val dag = inMemoryCommitDag(ns)
            val policies = inMemoryNamespacePolicyRegistry()
            policies.put(defaultMutable(ns))
            val backends = inMemoryTierBackendRegistry()
            val manager =
                storageTierManager(
                    dag,
                    InMemoryStorageAdapter(),
                    DefaultDeltaLogTierRegistry(),
                    { policies.get(it) },
                    backends,
                )
            val head = dag.head()
            val tree = DocumentTree.build(mapOf(KdbUuid.random() to KdbHash.fromHex("ab".repeat(32))))
            dag.putDocumentTree(tree)
            val commit =
                KdbCommit.build(
                    parentHashes = listOf(head),
                    namespaceId = ns,
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = listOf(KdbOp.Write(KdbUuid.random(), """{"x":1}""")),
                    documentTreeHash = tree.treeHash,
                    schemaHash = null,
                )
            dag.putCommit(commit)
            val archived = manager.archiveCommit(ArchiveRequest(ns, commit.hash))
            val restored =
                manager.restoreArchive(
                    RestoreRequest(archived.bundleLocation, "app/recovered"),
                )
            assertEquals("app/recovered", restored.namespaceId)
            assertEquals(1, restored.documentsImported)
            val liveDag = inMemoryCommitDag(ns)
            assertNull(liveDag.getCommit(commit.hash))
        }

    @Test
    fun archiveMissingCommit() =
        runTest {
            val ns = "app/x"
            val dag = inMemoryCommitDag(ns)
            val policies = inMemoryNamespacePolicyRegistry()
            policies.put(defaultMutable(ns))
            val manager =
                storageTierManager(
                    dag,
                    InMemoryStorageAdapter(),
                    DefaultDeltaLogTierRegistry(),
                    { policies.get(it) },
                )
            assertFailsWith<VersionNotFoundException> {
                manager.archiveCommit(
                    ArchiveRequest(ns, KdbHash.fromHex("ff".repeat(32))),
                )
            }
        }

    @Test
    fun tagSurvivesStub() =
        runTest {
            val ns = "app/tag"
            val dag = inMemoryCommitDag(ns)
            val policies = inMemoryNamespacePolicyRegistry()
            policies.put(defaultMutable(ns))
            val manager =
                storageTierManager(
                    dag,
                    InMemoryStorageAdapter(),
                    DefaultDeltaLogTierRegistry(),
                    { policies.get(it) },
                )
            val head = dag.head()
            val tree = DocumentTree.EMPTY
            dag.putDocumentTree(tree)
            val commit =
                KdbCommit.build(
                    parentHashes = listOf(head),
                    namespaceId = ns,
                    transactionId = KdbUuid.random(),
                    timestamp = KdbTimestamp.now(),
                    authorNodeId = KdbUuid.random(),
                    operations = emptyList(),
                    documentTreeHash = tree.treeHash,
                    schemaHash = null,
                )
            dag.putCommit(commit)
            dag.createTag("snap", commit.hash)
            manager.archiveCommit(ArchiveRequest(ns, commit.hash, tag = "snap"))
            val tag = dag.getTag("snap")
            assertNotNull(tag)
        }

    @Test
    fun restoreCorruptBundle() =
        runTest {
            val backends = inMemoryTierBackendRegistry()
            val backend = backends.get("default-ice")
            val loc = backend.put("bad", byteArrayOf(1, 2, 3))
            val manager =
                storageTierManager(
                    inMemoryCommitDag("x"),
                    InMemoryStorageAdapter(),
                    DefaultDeltaLogTierRegistry(),
                    { defaultMutable("x") },
                    backends,
                )
            assertFailsWith<ArchiveRestoreException> {
                manager.restoreArchive(RestoreRequest(loc, "app/bad"))
            }
        }
}
