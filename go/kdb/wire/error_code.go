package wire

// ErrorCode classifies a failed operation's response so a client can decide whether and when to
// retry without parsing prose - kdb-spec-layer13 Component 51 §8.1. Additive to the pre-existing
// Error *string field on SqlResultMessage/UpsertResultMessage/DocumentGetResultMessage: an old
// client that only reads Error keeps working unchanged, and a new client can additionally read
// Code/RetryAfterMs when present.
type ErrorCode string

const (
	// ErrorCodeBusy: the server cannot admit this operation right now, but the same request is
	// expected to succeed later - retry after RetryAfterMs.
	ErrorCodeBusy ErrorCode = "BUSY"
	// ErrorCodeUnavailable: the server is shutting down (or otherwise cannot accept new work at
	// all right now) - retry after reconnecting, likely to a different/restarted instance.
	ErrorCodeUnavailable ErrorCode = "UNAVAILABLE"
	// ErrorCodeDeadlineExceeded: the caller's own deadline passed before the operation could run.
	// Retryable, but raise the deadline - retrying with the same one is unlikely to help.
	ErrorCodeDeadlineExceeded ErrorCode = "DEADLINE_EXCEEDED"
	// ErrorCodeResourceExhausted: this specific operation is too large to ever be admitted -
	// resubmit smaller, don't just retry as-is.
	ErrorCodeResourceExhausted ErrorCode = "RESOURCE_EXHAUSTED"
	// ErrorCodeConflict: an optimistic-concurrency conflict - see ConflictReportMessage for the
	// structured detail this code accompanies when set on that message type.
	ErrorCodeConflict ErrorCode = "CONFLICT"
	// ErrorCodeSchemaViolation: the transaction itself is invalid - never retry unmodified.
	ErrorCodeSchemaViolation ErrorCode = "SCHEMA_VIOLATION"
	// ErrorCodeUnauthorized: RBAC denied the operation - never retry unmodified.
	ErrorCodeUnauthorized ErrorCode = "UNAUTHORIZED"
	// ErrorCodeInternal: unclassified - the fallback when no more specific code applies.
	ErrorCodeInternal ErrorCode = "INTERNAL"
)
