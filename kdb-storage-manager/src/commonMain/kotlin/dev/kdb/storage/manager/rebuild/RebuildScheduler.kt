package dev.kdb.storage.manager.rebuild

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.manager.pool.RealizedStorePool

public interface RebuildScheduler {
    public fun scheduleRebuild(enlistmentId: KdbUuid)
}

public class DefaultRebuildScheduler(
    private val pool: RealizedStorePool,
) : RebuildScheduler {
    override fun scheduleRebuild(enlistmentId: KdbUuid) {}
}
