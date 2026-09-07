package dev.kdb.policy

public sealed class PolicyValidationError {
    public data class InvalidRetainOrdering(val message: String) : PolicyValidationError()
    public data class SchemaRequired(val field: String) : PolicyValidationError()
    public data class UnsupportedMode(val detail: String) : PolicyValidationError()
    public data class InvalidExpiry(val detail: String) : PolicyValidationError()
}

public data class PolicyValidationResult(
    val ok: Boolean,
    val errors: List<PolicyValidationError>,
)

public interface PolicyValidator {
    public fun validate(policy: NamespacePolicy): PolicyValidationResult
}

public object DefaultPolicyValidator : PolicyValidator {
    override fun validate(policy: NamespacePolicy): PolicyValidationResult {
        val errors = mutableListOf<PolicyValidationError>()
        val rules = policy.compaction.retainGranularity
        if (rules.size > 1) {
            for (i in 1 until rules.size) {
                if (rules[i].olderThanMillis <= rules[i - 1].olderThanMillis) {
                    errors +=
                        PolicyValidationError.InvalidRetainOrdering(
                            "retainGranularity must be sorted by olderThanMillis ascending",
                        )
                    break
                }
            }
        }
        if (policy.history == HistoryMode.NONE && policy.compaction.squashAfter != SquashMode.NEVER) {
            errors +=
                PolicyValidationError.UnsupportedMode(
                    "history=NONE should use squashAfter=NEVER",
                )
        }
        if (policy.mode == NamespaceMode.APPEND_ONLY && policy.conflict != dev.kdb.transaction.ConflictPolicy.APPEND_ONLY) {
            errors +=
                PolicyValidationError.UnsupportedMode(
                    "APPEND_ONLY namespaces should use conflict=APPEND_ONLY",
                )
        }
        policy.documentExpiry?.let { expiry ->
            if (expiry.fieldPath.isBlank()) {
                errors += PolicyValidationError.InvalidExpiry("documentExpiry.fieldPath must not be blank")
            }
            if (expiry.graceMillis < 0) {
                errors += PolicyValidationError.InvalidExpiry("documentExpiry.graceMillis must be >= 0")
            }
            if (expiry.sweepIntervalMillis <= 0) {
                errors += PolicyValidationError.InvalidExpiry("documentExpiry.sweepIntervalMillis must be > 0")
            }
        }
        return PolicyValidationResult(errors.isEmpty(), errors)
    }
}
