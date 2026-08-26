package server

import (
	"errors"

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
		// Memory pressure is the same "retry later" shape as BUSY, just from a different gate -
		// no fixed retry-after is meaningful here (it clears whenever the sampler next observes
		// usage back under the clear threshold, not on a schedule the server can promise), so
		// RetryAfterMs is left nil rather than inventing a number.
		return wire.ErrorCodeBusy, nil
	}
	var auth *AuthorizationError
	if errors.As(err, &auth) {
		return wire.ErrorCodeUnauthorized, nil
	}
	var schemaErr *SchemaError
	if errors.As(err, &schemaErr) {
		return wire.ErrorCodeSchemaViolation, nil
	}
	return wire.ErrorCodeInternal, nil
}
