package dev.kdb.storage.manager.eviction

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.EnlistmentEvictionState

public interface EvictionManager {
    public fun onHandleReleased(enlistmentId: KdbUuid)
    public fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState
}

public class DefaultEvictionManager(
    private val pool: dev.kdb.storage.manager.pool.RealizedStorePool,
) : EvictionManager {
    private val states = mutableMapOf<KdbUuid, EnlistmentEvictionState>()

    override fun onHandleReleased(enlistmentId: KdbUuid) {
        // mark candidate for LRU
    }

    override fun evictionState(enlistmentId: KdbUuid): EnlistmentEvictionState =
        states.getOrDefault(enlistmentId, EnlistmentEvictionState.FULL)
}
