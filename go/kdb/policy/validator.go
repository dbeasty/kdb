package policy

import (
	"github.com/limidus/kdb/go/kdb/transaction"
)

// ValidationError is a policy validation failure.
type ValidationError struct {
	Errors []ValidationIssue
}

func (e *ValidationError) Error() string {
	if len(e.Errors) == 0 {
		return "policy validation failed"
	}
	return e.Errors[0].Message
}

// ValidationIssue describes one validation problem.
type ValidationIssue struct {
	Kind    string
	Message string
}

// ValidationResult is the outcome of policy validation.
type ValidationResult struct {
	OK     bool
	Errors []ValidationIssue
}

// Validator validates namespace policies.
type Validator interface {
	Validate(policy NamespacePolicy) ValidationResult
}

// DefaultValidator is the standard policy validator.
var DefaultValidator Validator = defaultValidator{}

type defaultValidator struct{}

func (defaultValidator) Validate(policy NamespacePolicy) ValidationResult {
	var errors []ValidationIssue
	rules := policy.Compaction.RetainGranularity
	if len(rules) > 1 {
		for i := 1; i < len(rules); i++ {
			if rules[i].OlderThanMillis <= rules[i-1].OlderThanMillis {
				errors = append(errors, ValidationIssue{
					Kind:    "InvalidRetainOrdering",
					Message: "retainGranularity must be sorted by olderThanMillis ascending",
				})
				break
			}
		}
	}
	if policy.History == HistoryModeNone && policy.Compaction.SquashAfter != SquashModeNever {
		errors = append(errors, ValidationIssue{
			Kind:    "UnsupportedMode",
			Message: "history=NONE should use squashAfter=NEVER",
		})
	}
	if policy.Mode == NamespaceModeAppendOnly && policy.Conflict != transaction.ConflictPolicyAppendOnly {
		errors = append(errors, ValidationIssue{
			Kind:    "UnsupportedMode",
			Message: "APPEND_ONLY namespaces should use conflict=APPEND_ONLY",
		})
	}
	return ValidationResult{OK: len(errors) == 0, Errors: errors}
}
