package client

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

// RESOURCE_EXHAUSTED used to fall through to an untyped error, so a caller could not tell "this
// query is too large" from any other failure, and a retry loop keyed on ErrBusy rightly would not
// retry it - but nothing said why. It has its own class now, and must never read as busy.
func TestResourceExhaustedIsItsOwnNonRetryableError(t *testing.T) {
	code := wire.ErrorCodeResourceExhausted
	err := classifiedError("result has more than 10000 rows - add a LIMIT", &code, nil)

	if !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("want ErrResourceExhausted, got %T: %v", err, err)
	}
	if errors.Is(err, ErrBusy) {
		t.Fatal("an over-large query is not busy: retrying it fails identically")
	}
	var typed *ResourceExhaustedError
	if !errors.As(err, &typed) || typed.Message == "" {
		t.Fatalf("want a *ResourceExhaustedError carrying the server's message, got %v", err)
	}
}

func TestBusyStaysRetryableAndDistinct(t *testing.T) {
	code := wire.ErrorCodeBusy
	ms := 40
	err := classifiedError("queue full", &code, &ms)
	if !errors.Is(err, ErrBusy) || errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("BUSY must stay ErrBusy only, got %T", err)
	}
}
