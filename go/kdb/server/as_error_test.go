package server

import (
	"fmt"
	"testing"
)

// TestAsErrorUnwrapsWrappedErrors is the regression test for the finding recorded in
// docs/kdb-finish-up-plan.md as 1-G7: asError's doc comment claimed it checked "err (or
// something in its chain)", but the implementation was a plain err.(T) type assertion with no
// chain unwrapping - a *ConflictError wrapped by any future fmt.Errorf("%w", ...) call would
// have been silently reclassified as a generic SQL error at handleTxCommit/handleTransaction
// Replay instead of surfacing as a ConflictReportMessage the client can actually act on.
func TestAsErrorUnwrapsWrappedErrors(t *testing.T) {
	inner := &ConflictError{}
	wrapped := fmt.Errorf("commit failed: %w", inner)

	var target *ConflictError
	if !asError(wrapped, &target) {
		t.Fatal("expected asError to find the *ConflictError wrapped inside another error")
	}
	if target != inner {
		t.Fatalf("expected the exact same *ConflictError instance, got %p want %p", target, inner)
	}
}

// TestAsErrorStillMatchesUnwrappedErrors is the plain (non-wrapped) case, unaffected by the fix.
func TestAsErrorStillMatchesUnwrappedErrors(t *testing.T) {
	inner := &ConflictError{}
	var target *ConflictError
	if !asError(error(inner), &target) {
		t.Fatal("expected asError to match a plain, unwrapped *ConflictError")
	}
	if target != inner {
		t.Fatal("expected the exact same *ConflictError instance")
	}
}

// TestAsErrorRejectsMismatchedType confirms asError still correctly reports false rather than
// panicking or matching the wrong type.
func TestAsErrorRejectsMismatchedType(t *testing.T) {
	var target *ConflictError
	if asError(fmt.Errorf("unrelated"), &target) {
		t.Fatal("expected asError to reject an error that isn't a *ConflictError")
	}
}
