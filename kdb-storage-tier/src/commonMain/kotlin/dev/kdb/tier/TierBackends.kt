package dev.kdb.tier

import dev.kdb.policy.StorageKind
import dev.kdb.storage.PlatformIoShim

public class InMemoryTierBackend(
    override val id: String,
    override val storageKind: StorageKind = StorageKind.ARCHIVE,
) : TierBackend {
    private val store = mutableMapOf<String, ByteArray>()

    override suspend fun put(key: String, bytes: ByteArray): String {
        val loc = "mem://$id/$key"
        store[loc] = bytes.copyOf()
        return loc
    }

    override suspend fun get(location: String): ByteArray =
        store[location]?.copyOf() ?: throw dev.kdb.error.StorageTierException("missing $location", "COLD")

    override suspend fun delete(location: String): Boolean = store.remove(location) != null

    override suspend fun exists(location: String): Boolean = store.containsKey(location)
}

public class DefaultTierBackendRegistry(
    backends: Map<String, TierBackend> = emptyMap(),
) : TierBackendRegistry {
    private val map = backends.toMutableMap()

    init {
        if (!map.containsKey("default-warm")) {
            register("default-warm", InMemoryTierBackend("default-warm", StorageKind.LOCAL_FS))
        }
        if (!map.containsKey("default-cold")) {
            register("default-cold", InMemoryTierBackend("default-cold", StorageKind.OBJECT_STORE))
        }
        if (!map.containsKey("default-ice")) {
            register("default-ice", InMemoryTierBackend("default-ice", StorageKind.ARCHIVE))
        }
    }

    override fun get(backendId: String): TierBackend =
        map[backendId] ?: throw dev.kdb.error.StorageTierException("unknown backend $backendId", backendId)

    override fun register(backendId: String, backend: TierBackend) {
        map[backendId] = backend
    }
}

public fun inMemoryTierBackendRegistry(): TierBackendRegistry = DefaultTierBackendRegistry()

/**
 * Registry whose warm/cold/ice backends are all real [PlatformIoShimTierBackend]s sharing one
 * [ioShim] — genuinely persistent storage (files on disk on JVM/Native, browser storage on JS),
 * distinguished from HOT purely by directory/key namespace, exactly like a real deployment would
 * route cold/ice to a separate (slower, cheaper) volume or object store.
 */
public fun platformIoTierBackendRegistry(ioShim: PlatformIoShim): TierBackendRegistry =
    DefaultTierBackendRegistry(
        mapOf(
            "default-warm" to PlatformIoShimTierBackend(ioShim, "default-warm", StorageKind.LOCAL_FS),
            "default-cold" to PlatformIoShimTierBackend(ioShim, "default-cold", StorageKind.OBJECT_STORE),
            "default-ice" to PlatformIoShimTierBackend(ioShim, "default-ice", StorageKind.ARCHIVE),
        ),
    )
