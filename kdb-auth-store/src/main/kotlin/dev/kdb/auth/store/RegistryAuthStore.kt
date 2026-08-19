package dev.kdb.auth.store

import dev.kdb.codec.KdbUuid
import dev.kdb.dag.CommitDag
import dev.kdb.dag.inMemoryCommitDag
import dev.kdb.document.KdbDocument
import dev.kdb.document.kdbSha256
import dev.kdb.schema.KdbSchema
import dev.kdb.storage.StorageAdapter
import dev.kdb.storage.mem.InMemoryStorageAdapter
import dev.kdb.transaction.ConflictPolicy
import dev.kdb.transaction.TransactionBuilder
import dev.kdb.transaction.TransactionEngine
import dev.kdb.transaction.TransactionResult
import dev.kdb.transaction.transactionEngine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.Json

/**
 * [UserStore]/[RoleStore] persisted as documents inside KDB itself, written through the normal
 * commit path ([TransactionEngine]/[CommitDag]/[StorageAdapter]) rather than a static file —
 * this is what makes "add/remove roles at runtime" actually durable. See docs/kdb-rbac-plan.md.
 *
 * Users and roles live in two reserved collections, each with its own [CommitDag] (a `CommitDag`
 * instance is single-namespace in this codebase — see [inMemoryCommitDag] usage below) sharing
 * one [StorageAdapter]. Note: as of this writing the only [CommitDag] implementation available
 * anywhere in the repo is the in-memory one; a real deployment wanting durability across restarts
 * needs to supply dags backed by a persistent implementation once one exists, or wire this store
 * to whatever durable `(dag, storage)` pair the server process already holds.
 *
 * Mutating calls are serialized through [mutex] — this is a single-writer store, appropriate
 * for an admin-path registry, not a high-throughput data collection.
 */
public class RegistryAuthStore(
    private val userDag: CommitDag,
    private val roleDag: CommitDag,
    private val storage: StorageAdapter,
    private val authorNodeId: KdbUuid = KdbUuid.random(),
    private val engine: TransactionEngine = transactionEngine(ConflictPolicy.LAST_WRITE),
) : UserStore, RoleStore {
    private val mutex = Mutex()

    // encodeDefaults = true is required: writes go through KdbDocument.merge, a shallow overlay
    // that only replaces keys present in the patch. Json omits fields equal to their default
    // value by default (e.g. an emptied grants: Set<String> = emptySet()), which would leave
    // the old value in place instead of clearing it — encodeDefaults=true always includes every
    // field so every write is a true full replacement, not an accidental partial patch.
    private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    override suspend fun createUser(
        id: String,
        password: String,
        roles: Set<String>,
    ) {
        mutex.withLock {
            if (readUser(id) != null) throw UserAlreadyExistsException(id)
            val (hash, salt) = PasswordHasher.hash(password)
            writeUser(UserRecord(id, hash, salt, roles))
        }
    }

    override suspend fun getUser(id: String): UserRecord? = mutex.withLock { readUser(id) }

    override suspend fun listUsers(): List<UserRecord> =
        mutex.withLock {
            scanAll(userDag, USERS_NAMESPACE) { json.decodeFromString(UserRecord.serializer(), it) }
        }

    override suspend fun updateCredentials(
        id: String,
        newPassword: String,
    ) {
        mutex.withLock {
            val existing = readUser(id) ?: throw UserNotFoundException(id)
            val (hash, salt) = PasswordHasher.hash(newPassword)
            writeUser(existing.copy(passwordHash = hash, passwordSalt = salt))
        }
    }

    override suspend fun deleteUser(id: String) {
        mutex.withLock {
            readUser(id) ?: throw UserNotFoundException(id)
            val docId = userDocId(id)
            val tx =
                TransactionBuilder(USERS_NAMESPACE, userDag.head(), authorNodeId, KdbSchema.NONE)
                    .delete(docId)
                    .build()
            commitOrThrow(userDag, tx)
        }
    }

    override suspend fun assignRole(
        id: String,
        role: String,
    ) {
        mutex.withLock {
            val existing = readUser(id) ?: throw UserNotFoundException(id)
            writeUser(existing.copy(roles = existing.roles + role))
        }
    }

    override suspend fun revokeRole(
        id: String,
        role: String,
    ) {
        mutex.withLock {
            val existing = readUser(id) ?: throw UserNotFoundException(id)
            writeUser(existing.copy(roles = existing.roles - role))
        }
    }

    override suspend fun verifyPassword(
        id: String,
        password: String,
    ): Boolean {
        val record = mutex.withLock { readUser(id) } ?: return false
        return PasswordHasher.verify(password, record.passwordHash, record.passwordSalt)
    }

    override suspend fun createRole(
        name: String,
        grants: Set<String>,
    ) {
        mutex.withLock {
            if (readRole(name) != null) throw RoleAlreadyExistsException(name)
            writeRole(RoleRecord(name, grants))
        }
    }

    override suspend fun getRole(name: String): RoleRecord? = mutex.withLock { readRole(name) }

    override suspend fun listRoles(): List<RoleRecord> =
        mutex.withLock {
            scanAll(roleDag, ROLES_NAMESPACE) { json.decodeFromString(RoleRecord.serializer(), it) }
        }

    override suspend fun updateGrants(
        name: String,
        grants: Set<String>,
    ) {
        mutex.withLock {
            readRole(name) ?: throw RoleNotFoundException(name)
            writeRole(RoleRecord(name, grants))
        }
    }

    override suspend fun deleteRole(name: String) {
        mutex.withLock {
            readRole(name) ?: throw RoleNotFoundException(name)
            val docId = roleDocId(name)
            val tx =
                TransactionBuilder(ROLES_NAMESPACE, roleDag.head(), authorNodeId, KdbSchema.NONE)
                    .delete(docId)
                    .build()
            commitOrThrow(roleDag, tx)
        }
    }

    /** Snapshot suitable for [dev.kdb.auth.principalHasPermission]: `roleName -> grants`. */
    public suspend fun grantsByRole(): Map<String, Set<String>> = listRoles().associate { it.name to it.grants }

    private suspend fun readUser(id: String): UserRecord? {
        val docId = userDocId(id)
        val treeHash = currentTreeHash(userDag) ?: return null
        val doc = storage.getDocument(USERS_NAMESPACE, docId, treeHash) ?: return null
        return json.decodeFromString(UserRecord.serializer(), doc.json)
    }

    private suspend fun readRole(name: String): RoleRecord? {
        val docId = roleDocId(name)
        val treeHash = currentTreeHash(roleDag) ?: return null
        val doc = storage.getDocument(ROLES_NAMESPACE, docId, treeHash) ?: return null
        return json.decodeFromString(RoleRecord.serializer(), doc.json)
    }

    /** [StorageAdapter] indexes documents by document-tree hash, not commit hash — resolve the
     * head commit's tree before every read. Null before the first commit (empty registry). */
    private suspend fun currentTreeHash(dag: CommitDag): dev.kdb.codec.KdbHash? {
        val head = dag.head()
        val commit = dag.getCommit(head) ?: return null
        return commit.documentTreeHash
    }

    private suspend fun writeUser(record: UserRecord) {
        val document = KdbDocument.fromJson(userDocId(record.id), json.encodeToString(UserRecord.serializer(), record))
        val tx =
            TransactionBuilder(USERS_NAMESPACE, userDag.head(), authorNodeId, KdbSchema.NONE)
                .writeDocument(document)
                .build()
        commitOrThrow(userDag, tx)
    }

    private suspend fun writeRole(record: RoleRecord) {
        val document = KdbDocument.fromJson(roleDocId(record.name), json.encodeToString(RoleRecord.serializer(), record))
        val tx =
            TransactionBuilder(ROLES_NAMESPACE, roleDag.head(), authorNodeId, KdbSchema.NONE)
                .writeDocument(document)
                .build()
        commitOrThrow(roleDag, tx)
    }

    private suspend fun commitOrThrow(
        dag: CommitDag,
        tx: dev.kdb.document.KdbTransaction,
    ) {
        when (val result = engine.commit(tx, dag, storage, KdbSchema.NONE)) {
            is TransactionResult.Success -> Unit
            is TransactionResult.Conflict ->
                throw IllegalStateException("registry write conflict: ${result.report.conflicts.size} operation(s)")
            is TransactionResult.SchemaError ->
                throw IllegalStateException("registry write rejected: ${result.violations.size} violation(s)")
            is TransactionResult.Aborted ->
                throw IllegalStateException("registry write aborted: ${result.cause.message}", result.cause)
        }
    }

    private suspend fun <T> scanAll(
        dag: CommitDag,
        namespaceId: String,
        decode: (String) -> T,
    ): List<T> {
        val treeHash = currentTreeHash(dag) ?: return emptyList()
        val out = mutableListOf<T>()
        storage.scanDocuments(namespaceId, treeHash) { batch ->
            batch.forEach { out += decode(it.json) }
        }
        return out
    }

    public companion object {
        public const val USERS_NAMESPACE: String = "_system/users"
        public const val ROLES_NAMESPACE: String = "_system/roles"

        private fun deterministicId(key: String): KdbUuid {
            val digest = kdbSha256(key.encodeToByteArray())
            return KdbUuid.fromBytes(digest.copyOf(16))
        }

        private fun userDocId(id: String): KdbUuid = deterministicId("user:$id")

        private fun roleDocId(name: String): KdbUuid = deterministicId("role:$name")

        /** In-memory store for tests/dev — see the durability caveat in the class doc. */
        public fun inMemory(authorNodeId: KdbUuid = KdbUuid.random()): RegistryAuthStore =
            RegistryAuthStore(
                userDag = inMemoryCommitDag(USERS_NAMESPACE),
                roleDag = inMemoryCommitDag(ROLES_NAMESPACE),
                storage = InMemoryStorageAdapter(),
                authorNodeId = authorNodeId,
            )
    }
}
