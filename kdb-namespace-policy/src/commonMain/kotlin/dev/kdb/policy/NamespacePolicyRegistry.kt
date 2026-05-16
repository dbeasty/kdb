package dev.kdb.policy

import dev.kdb.storage.StorageAdapter
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

public interface NamespacePolicyRegistry {
    public suspend fun get(namespaceId: String): NamespacePolicy
    public suspend fun getOrNull(namespaceId: String): NamespacePolicy?
    public suspend fun put(policy: NamespacePolicy)
    public suspend fun delete(namespaceId: String): Boolean
    public suspend fun list(): List<String>
}

public fun namespacePolicyRegistry(storage: StorageAdapter): NamespacePolicyRegistry =
    StorageBackedNamespacePolicyRegistry(storage, InMemoryNamespacePolicyRegistry())

public fun inMemoryNamespacePolicyRegistry(): NamespacePolicyRegistry =
    InMemoryNamespacePolicyRegistry()

public class InMemoryNamespacePolicyRegistry : NamespacePolicyRegistry {
    private val mutex = Mutex()
    private val policies = mutableMapOf<String, NamespacePolicy>()

    override suspend fun get(namespaceId: String): NamespacePolicy =
        mutex.withLock {
            policies[namespaceId] ?: defaultMutable(namespaceId)
        }

    override suspend fun getOrNull(namespaceId: String): NamespacePolicy? =
        mutex.withLock { policies[namespaceId] }

    override suspend fun put(policy: NamespacePolicy) {
        val validated = DefaultPolicyValidator.validate(policy)
        if (!validated.ok) {
            throw PolicyValidationException(validated.errors)
        }
        mutex.withLock {
            val next =
                policy.copy(
                    revision = (policies[policy.namespaceId]?.revision ?: 0L) + 1L,
                )
            policies[policy.namespaceId] = next
        }
    }

    override suspend fun delete(namespaceId: String): Boolean =
        mutex.withLock { policies.remove(namespaceId) != null }

    override suspend fun list(): List<String> = mutex.withLock { policies.keys.sorted() }
}

/** Persists encoded policy blobs via [StorageAdapter.writeBlob] (content-addressed backup). */
public class StorageBackedNamespacePolicyRegistry(
    private val storage: StorageAdapter,
    private val delegate: NamespacePolicyRegistry,
) : NamespacePolicyRegistry {
    override suspend fun get(namespaceId: String): NamespacePolicy = delegate.get(namespaceId)

    override suspend fun getOrNull(namespaceId: String): NamespacePolicy? = delegate.getOrNull(namespaceId)

    override suspend fun put(policy: NamespacePolicy) {
        delegate.put(policy)
        storage.writeBlob(encodePolicy(policy))
    }

    override suspend fun delete(namespaceId: String): Boolean = delegate.delete(namespaceId)

    override suspend fun list(): List<String> = delegate.list()
}
