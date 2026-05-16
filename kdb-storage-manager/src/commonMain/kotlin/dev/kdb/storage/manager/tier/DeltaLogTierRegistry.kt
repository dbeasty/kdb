package dev.kdb.storage.manager.tier

import dev.kdb.codec.KdbUuid
import dev.kdb.storage.DeltaSegmentRef
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow

public enum class SegmentTier { HOT, WARM, COLD, ICE }

public enum class SegmentAccessKind { READ, REBUILD, PEER_SYNC }

public interface DeltaLogTierRegistry {
    public suspend fun onSegmentSealed(ref: DeltaSegmentRef, namespaceId: String)
    public fun onSegmentAccess(ref: DeltaSegmentRef, accessKind: SegmentAccessKind)
    public fun tierOf(segmentId: KdbUuid): SegmentTier?
    public val tierSignals: SharedFlow<TierSignal>
}

public data class TierSignal(val segmentId: KdbUuid, val from: SegmentTier, val to: SegmentTier)

public class DefaultDeltaLogTierRegistry : DeltaLogTierRegistry {
    private val tiers = mutableMapOf<KdbUuid, SegmentTier>()
    private val _signals = MutableSharedFlow<TierSignal>(extraBufferCapacity = 64)
    override val tierSignals: SharedFlow<TierSignal> = _signals

    override suspend fun onSegmentSealed(ref: DeltaSegmentRef, namespaceId: String) {
        tiers[ref.segmentId] = SegmentTier.HOT
    }

    override fun onSegmentAccess(ref: DeltaSegmentRef, accessKind: SegmentAccessKind) {}

    override fun tierOf(segmentId: KdbUuid): SegmentTier? = tiers[segmentId]
}
