package dev.kdb.tier

import dev.kdb.error.StorageTierException
import dev.kdb.policy.StorageKind
import dev.kdb.storage.PlatformIoShim

/**
 * Real, persistent [TierBackend] backed by [PlatformIoShim]'s snapshot store — actual files on
 * disk on JVM/Native, browser-native storage on JS — not an in-memory map. Segment bytes moved
 * out of HOT land here and survive process restarts exactly like WAL/SSTable data does.
 */
public class PlatformIoShimTierBackend(
    private val ioShim: PlatformIoShim,
    override val id: String,
    override val storageKind: StorageKind,
) : TierBackend {
    override suspend fun put(
        key: String,
        bytes: ByteArray,
    ): String {
        val location = "$id/$key"
        ioShim.writeSnapshot(location, bytes)
        return location
    }

    override suspend fun get(location: String): ByteArray = ioShim.readSnapshot(location) ?: throw StorageTierException("missing $location", id)

    override suspend fun delete(location: String): Boolean {
        val existed = ioShim.readSnapshot(location) != null
        if (existed) ioShim.deleteSnapshot(location)
        return existed
    }

    override suspend fun exists(location: String): Boolean = ioShim.readSnapshot(location) != null
}
