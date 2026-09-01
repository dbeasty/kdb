package server

import (
	"errors"

	"github.com/limidus/kdb/go/kdb/sql"
	"github.com/limidus/kdb/go/kdb/wire"
)

// classifyError maps a commitWith error to a wire.ErrorCode and, where meaningful, a
// retry-after hint - kdb-spec-layer13 Component 51 §8.1: a client needs to know whether and
// when to retry, not just prose. Falls back to ErrorCodeInternal for anything unrecognized
// (including the pre-existing AuthorizationError/SchemaError types, which callers already
// special-case for other reasons but which still deserve a code here for a client that only
// looks at ErrorCode).
func classifyError(err error) (wire.ErrorCode, *int) {
	var busy *BusyError
	if errors.As(err, &busy) {
		ms := int(busy.RetryAfter().Milliseconds())
		return wire.ErrorCodeBusy, &ms
	}
	var unavailable *UnavailableError
	if errors.As(err, &unavailable) {
		return wire.ErrorCodeUnavailable, nil
	}
	var deadline *DeadlineExceededError
	if errors.As(err, &deadline) {
		return wire.ErrorCodeDeadlineExceeded, nil
	}
	var pressure *MemoryPressureError
	if errors.As(err, &pressure) {
		// Memory pressure is the same "retry later" shape as BUSY, just from a different gate.
		// Component 48 gives it a real retry-after: the zone it was shed in implies how long a
		// caller should wait (deeper zones, longer backoff - see retryAfterMsForZone). Before
		// the zones existed there was no honest number to put here, since the old boolean guard
		// cleared whenever the sampler happened to next see usage under the threshold.
		if pressure.RetryAfterMs > 0 {
			ms := pressure.RetryAfterMs
			return wire.ErrorCodeBusy, &ms
		}
		return wire.ErrorCodeBusy, nil
	}
	var exhausted *ResourceExhaustedError
	if errors.As(err, &exhausted) {
		// Deliberately *not* BUSY: this operation is larger than the node's entire grant
		// capacity, so "retry later" would be a lie - no amount of waiting makes it admissible.
		// The client's only useful move is to resubmit smaller, which is exactly what
		// RESOURCE_EXHAUSTED tells it (Component 51 §8.1). Before Component 48's cost model
		// existed there was nothing that could make this judgment, which is why this code had
		// no producer.
		return wire.ErrorCodeResourceExhausted, nil
	}
	var rowBudget *sql.ScanRowBudgetExceededError
	if errors.As(err, &rowBudget) {
		// Same reasoning as ResourceExhaustedError above: the query as written costs more than
		// this node will spend on it, so "retry later" would be misleading - it will cost exactly
		// as much next time.
		return wire.ErrorCodeResourceExhausted, nil
	}
	var conflict *ConflictError
	if errors.As(err, &conflict) {
		// Retryable, but only after re-reading: the transaction as submitted is anchored on a
		// base version that has moved, so resubmitting it byte-for-byte fails identically. The
		// remedy CONFLICT names is "re-read, recompute, retry after RetryAfterMs" - distinct
		// from BUSY, where the same bytes would have succeeded.
		if conflict.RetryAfterMs > 0 {
			ms := conflict.RetryAfterMs
			return wire.ErrorCodeConflict, &ms
		}
		return wire.ErrorCodeConflict, nil
	}
	var auth *AuthorizationError
	if errors.As(err, &auth) {
		return wire.ErrorCodeUnauthorized, nil
	}
	var schemaErr *SchemaError
	if errors.As(err, &schemaErr) {
		if schemaErr.HasUniqueViolation() {
			return wire.ErrorCodeUniqueViolation, nil
		}
		return wire.ErrorCodeSchemaViolation, nil
	}
	return wire.ErrorCodeInternal, nil
}
