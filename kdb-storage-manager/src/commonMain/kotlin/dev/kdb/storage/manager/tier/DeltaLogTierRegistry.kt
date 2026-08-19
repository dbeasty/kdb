package dev.kdb.storage.manager.tier

import dev.kdb.codec.KdbTimestamp
import dev.kdb.codec.KdbUuid
import dev.kdb.storage.DeltaSegmentRef
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.SharedFlow

public enum class SegmentTier { HOT, WARM, COLD, ICE }

public enum class SegmentAccessKind { READ, REBUILD, PEER_SYNC }

public interface DeltaLogTierRegistry {
    public suspend fun onSegmentSealed(ref: DeltaSegmentRef, namespaceId: String)

    public fun onSegmentAccess(
        ref: DeltaSegmentRef,
        accessKind: SegmentAccessKind,
    )

    public fun tierOf(segmentId: KdbUuid): SegmentTier?

    /** Wall-clock millis the segment was sealed at (age origin for tier-band thresholds). */
    public fun sealedAtMillis(segmentId: KdbUuid): Long?

    public fun refOf(segmentId: KdbUuid): DeltaSegmentRef?

    public fun namespaceOf(segmentId: KdbUuid): String?

    /** Records that a segment now lives in [tier]; emits a [TierSignal] if the tier actually changed. */
    public fun setTier(
        segmentId: KdbUuid,
        tier: SegmentTier,
    )

    public fun segmentsInTier(
        namespaceId: String,
        tier: SegmentTier,
    ): List<KdbUuid>

    public val tierSignals: SharedFlow<TierSignal>
}

public data class TierSignal(val segmentId: KdbUuid, val from: SegmentTier, val to: SegmentTier)

/**
 * [clockMillis] is injectable so tests can simulate segment age (days/months) without
 * real wall-clock waits.
 */
public class DefaultDeltaLogTierRegistry(
    private val clockMillis: () -> Long = { KdbTimestamp.now().epochMillis },
) : DeltaLogTierRegistry {
    private val tiers = mutableMapOf<KdbUuid, SegmentTier>()
    private val sealedAt = mutableMapOf<KdbUuid, Long>()
    private val refs = mutableMapOf<KdbUuid, DeltaSegmentRef>()
    private val namespaces = mutableMapOf<KdbUuid, String>()
    private val _signals = MutableSharedFlow<TierSignal>(extraBufferCapacity = 64)
    override val tierSignals: SharedFlow<TierSignal> = _signals

    override suspend fun onSegmentSealed(
        ref: DeltaSegmentRef,
        namespaceId: String,
    ) {
        tiers[ref.segmentId] = SegmentTier.HOT
        sealedAt[ref.segmentId] = clockMillis()
        refs[ref.segmentId] = ref
        namespaces[ref.segmentId] = namespaceId
    }

    override fun onSegmentAccess(
        ref: DeltaSegmentRef,
        accessKind: SegmentAccessKind,
    ) {}

    override fun tierOf(segmentId: KdbUuid): SegmentTier? = tiers[segmentId]

    override fun sealedAtMillis(segmentId: KdbUuid): Long? = sealedAt[segmentId]

    override fun refOf(segmentId: KdbUuid): DeltaSegmentRef? = refs[segmentId]

    override fun namespaceOf(segmentId: KdbUuid): String? = namespaces[segmentId]

    override fun setTier(
        segmentId: KdbUuid,
        tier: SegmentTier,
    ) {
        val from = tiers[segmentId] ?: return
        if (from == tier) return
        tiers[segmentId] = tier
        _signals.tryEmit(TierSignal(segmentId, from, tier))
    }

    override fun segmentsInTier(
        namespaceId: String,
        tier: SegmentTier,
    ): List<KdbUuid> =
        tiers.entries
            .filter { (id, t) -> t == tier && namespaces[id] == namespaceId }
            .map { it.key }
}
