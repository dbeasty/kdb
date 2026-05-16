package dev.kdb.policy

import dev.kdb.storage.IndexRetention
import dev.kdb.transaction.ConflictPolicy
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
internal data class NamespacePolicyJson(
    val namespaceId: String,
    val mode: String = "MUTABLE",
    val history: String = "FULL",
    val conflict: String = "STRICT",
    val compaction: CompactionPolicyJson = CompactionPolicyJson(),
    val tiers: TierPolicyJson = TierPolicyJson(),
    val indexRetentionDefault: String = "EVICTABLE",
    val gpuPromotion: GpuPromotionPolicyJson? = null,
    val vectorIndex: VectorIndexPolicyJson = VectorIndexPolicyJson(),
    val revision: Long = 1L,
)

@Serializable
internal data class CompactionPolicyJson(
    val keepTagged: Boolean = true,
    val keepBranchPoints: Boolean = true,
    val squashAfter: String = "AUTO",
    val retainGranularity: List<RetainRuleJson> = emptyList(),
)

@Serializable
internal data class RetainRuleJson(
    val olderThanMillis: Long,
    val strategy: String,
)

@Serializable
internal data class TierPolicyJson(
    val hotMaxAgeMillis: Long = 7L * 24 * 3600 * 1000,
    val warmMaxAgeMillis: Long = 90L * 24 * 3600 * 1000,
    val coldMaxAgeMillis: Long = 365L * 24 * 3600 * 1000,
)

@Serializable
internal data class GpuPromotionPolicyJson(
    val minSegmentAgeMillis: Long,
    val minSegmentSizeBytes: Long,
    val maxChangeRatePerMinute: Double,
)

@Serializable
internal data class VectorIndexPolicyJson(
    val hnswM: Int = 16,
    val hnswEfConstruction: Int = 200,
    val defaultDimensions: Int = 128,
)

internal val policyJson =
    Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

internal fun NamespacePolicyJson.toModel(schema: dev.kdb.schema.KdbSchema?): NamespacePolicy =
    NamespacePolicy(
        namespaceId = namespaceId,
        schema = schema,
        mode = enumValueOf<NamespaceMode>(mode),
        history = enumValueOf<HistoryMode>(history),
        conflict = enumValueOf<ConflictPolicy>(conflict),
        compaction =
            CompactionPolicy(
                keepTagged = compaction.keepTagged,
                keepBranchPoints = compaction.keepBranchPoints,
                squashAfter = enumValueOf<SquashMode>(compaction.squashAfter),
                retainGranularity =
                    if (compaction.retainGranularity.isEmpty()) {
                        defaultRetainGranularity()
                    } else {
                        compaction.retainGranularity.map {
                            RetainRule(it.olderThanMillis, enumValueOf<RetainStrategy>(it.strategy))
                        }
                    },
            ),
        tiers =
            TierPolicy(
                hot = TierBand(tiers.hotMaxAgeMillis),
                warm = TierBand(tiers.warmMaxAgeMillis),
                cold = TierBand(tiers.coldMaxAgeMillis),
            ),
        indexRetentionDefault = enumValueOf<IndexRetention>(indexRetentionDefault),
        gpuPromotion =
            gpuPromotion?.let {
                GpuPromotionPolicyRef(
                    it.minSegmentAgeMillis,
                    it.minSegmentSizeBytes,
                    it.maxChangeRatePerMinute,
                )
            },
        vectorIndex =
            VectorIndexPolicy(
                vectorIndex.hnswM,
                vectorIndex.hnswEfConstruction,
                vectorIndex.defaultDimensions,
            ),
        revision = revision,
    )

internal fun NamespacePolicy.toJson(): NamespacePolicyJson =
    NamespacePolicyJson(
        namespaceId = namespaceId,
        mode = mode.name,
        history = history.name,
        conflict = conflict.name,
        compaction =
            CompactionPolicyJson(
                keepTagged = compaction.keepTagged,
                keepBranchPoints = compaction.keepBranchPoints,
                squashAfter = compaction.squashAfter.name,
                retainGranularity =
                    compaction.retainGranularity.map {
                        RetainRuleJson(it.olderThanMillis, it.strategy.name)
                    },
            ),
        tiers =
            TierPolicyJson(
                hotMaxAgeMillis = tiers.hot.maxAgeMillis,
                warmMaxAgeMillis = tiers.warm.maxAgeMillis,
                coldMaxAgeMillis = tiers.cold.maxAgeMillis,
            ),
        indexRetentionDefault = indexRetentionDefault.name,
        gpuPromotion =
            gpuPromotion?.let {
                GpuPromotionPolicyJson(
                    it.minSegmentAgeMillis,
                    it.minSegmentSizeBytes,
                    it.maxChangeRatePerMinute,
                )
            },
        vectorIndex =
            VectorIndexPolicyJson(
                vectorIndex.hnswM,
                vectorIndex.hnswEfConstruction,
                vectorIndex.defaultDimensions,
            ),
        revision = revision,
    )

internal fun encodePolicy(policy: NamespacePolicy): ByteArray =
    policyJson.encodeToString(NamespacePolicyJson.serializer(), policy.toJson()).encodeToByteArray()

internal fun decodePolicy(bytes: ByteArray, schema: dev.kdb.schema.KdbSchema?): NamespacePolicy {
    val dto = policyJson.decodeFromString(NamespacePolicyJson.serializer(), bytes.decodeToString())
    return dto.toModel(schema)
}
