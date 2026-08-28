package error_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// Every typed error here carries a Code that ends up on the wire as the reason a client is
// given, so the mapping from error to code has to survive the trip up through the layers.

func TestEveryExceptionCarriesItsCode(t *testing.T) {
	cases := []struct {
		name string
		err  kdberr.Exception
		want kdberr.Code
	}{
		{"decode", kdberr.NewDecodeError("bad bytes", 12, nil), kdberr.KdbDecodeError},
		{"encode", kdberr.NewEncodeError("bad value", nil), kdberr.KdbEncodeError},
		{"schema", kdberr.NewSchemaError("bad schema", nil), kdberr.KdbSchemaError},
		{"json path", kdberr.NewJsonPathError("bad path", "$.a", nil), kdberr.JSONPathError},
		{"version not found", kdberr.NewVersionNotFoundError("missing", "ns", "abc"), kdberr.VersionNotFound},
		{"ice storage", kdberr.NewIceStorageError("archived", "ns", "h", "s3://x"), kdberr.IceStorage},
		{"conflict", kdberr.NewConflictError("conflict", kdberr.ConflictReport{}), kdberr.Conflict},
		{"document locked", kdberr.NewDocumentLockedError("held", "ns", "d", "session"), kdberr.DocumentLocked},
		{"namespace not found", kdberr.NewNamespaceNotFoundError("missing", "ns"), kdberr.NamespaceNotFound},
		{"transport", kdberr.NewTransportErr("socket died", nil), kdberr.TransportError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.err.Code(); got != tc.want {
				t.Errorf("Code() = %d, want %d", got, tc.want)
			}
			if got := kdberr.CodeOf(tc.err); got != tc.want {
				t.Errorf("CodeOf() = %d, want %d", got, tc.want)
			}
			if !kdberr.IsException(tc.err) {
				t.Error("IsException() = false")
			}
			if tc.err.Error() == "" {
				t.Error("Error() is empty")
			}
		})
	}
}

// An exception that has passed through a fmt.Errorf("%w") anywhere on its way up is still the
// same failure. A plain type assertion stops seeing it the moment any layer adds context, so
// CodeOf answered 0 - "no code" - for errors carrying a perfectly good one, and the client got
// a generic failure instead of the typed one it could have acted on.
func TestExceptionsAreRecognizedThroughWrapping(t *testing.T) {
	inner := kdberr.NewConflictError("write conflict", kdberr.ConflictReport{TransactionID: "tx-1"})

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"unwrapped", inner},
		{"wrapped once", fmt.Errorf("while committing: %w", inner)},
		{"wrapped twice", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", inner))},
		{"joined with another error", errors.Join(errors.New("unrelated"), inner)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !kdberr.IsException(tc.err) {
				t.Error("IsException() = false")
			}
			if got := kdberr.CodeOf(tc.err); got != kdberr.Conflict {
				t.Errorf("CodeOf() = %d, want %d", got, kdberr.Conflict)
			}
			got, ok := kdberr.AsException(tc.err)
			if !ok {
				t.Fatal("AsException() found nothing")
			}
			if got.Code() != kdberr.Conflict {
				t.Errorf("AsException() gave code %d", got.Code())
			}
		})
	}
}

// The concrete type has to remain reachable too - a caller that needs the conflict report, or
// the offset of a decode failure, gets at it with errors.As.
func TestConcreteTypeSurvivesWrapping(t *testing.T) {
	inner := kdberr.NewDecodeError("truncated", 42, nil)
	wrapped := fmt.Errorf("reading segment: %w", inner)

	var decodeErr *kdberr.DecodeError
	if !errors.As(wrapped, &decodeErr) {
		t.Fatal("errors.As did not find the DecodeError")
	}
	if decodeErr.Offset != 42 {
		t.Fatalf("offset is %d, want 42", decodeErr.Offset)
	}

	conflict := kdberr.NewConflictError("conflict", kdberr.ConflictReport{TransactionID: "tx-9"})
	var conflictErr *kdberr.ConflictError
	if !errors.As(fmt.Errorf("committing: %w", conflict), &conflictErr) {
		t.Fatal("errors.As did not find the ConflictError")
	}
	if conflictErr.Report.TransactionID != "tx-9" {
		t.Fatalf("report lost through wrapping: %+v", conflictErr.Report)
	}
}

func TestNonExceptionsAreNotMistakenForOne(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"nil", nil},
		{"plain error", errors.New("just an error")},
		{"wrapped plain error", fmt.Errorf("context: %w", errors.New("just an error"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if kdberr.IsException(tc.err) {
				t.Error("IsException() = true")
			}
			if got := kdberr.CodeOf(tc.err); got != 0 {
				t.Errorf("CodeOf() = %d, want 0", got)
			}
			if _, ok := kdberr.AsException(tc.err); ok {
				t.Error("AsException() found an exception")
			}
		})
	}
}

// A cause is part of the message and stays reachable through Unwrap, so the underlying failure
// is not lost when a layer wraps it in a typed error.
func TestCauseIsReportedAndUnwrappable(t *testing.T) {
	cause := errors.New("disk full")
	err := kdberr.NewEncodeError("could not write segment", cause)

	if !strings.Contains(err.Error(), "disk full") {
		t.Errorf("message does not mention the cause: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "could not write segment") {
		t.Errorf("message does not mention its own text: %q", err.Error())
	}
	if !errors.Is(err, cause) {
		t.Error("errors.Is did not find the cause")
	}

	// Without a cause the message is just the message - no trailing separator or empty tail.
	bare := kdberr.NewEncodeError("could not write segment", nil)
	if bare.Error() != "could not write segment" {
		t.Errorf("message with no cause is %q", bare.Error())
	}
	if errors.Unwrap(bare) != nil {
		t.Error("an error with no cause unwrapped to something")
	}
}

// The codes are a published contract - the comment on Code says the numbers must not change
// once published, so they are pinned here rather than only being read back from the constants.
func TestCodeNumbersAreStable(t *testing.T) {
	for _, tc := range []struct {
		code kdberr.Code
		want int
	}{
		{kdberr.KdbDecodeError, 1001},
		{kdberr.KdbEncodeError, 1002},
		{kdberr.KdbSchemaError, 1005},
		{kdberr.JSONPathError, 2001},
		{kdberr.SchemaViolation, 3001},
		{kdberr.SchemaMigrationFailed, 3002},
		{kdberr.VersionNotFound, 3101},
		{kdberr.IceStorage, 3102},
		{kdberr.CompactionBoundary, 3103},
		{kdberr.Conflict, 4001},
		{kdberr.DocumentLocked, 4002},
		{kdberr.StorageTierError, 4101},
		{kdberr.DataDirectoryLocked, 4102},
		{kdberr.NamespaceNotFound, 4201},
		{kdberr.IndexCorruption, 5001},
		{kdberr.UnsupportedProtocolVersion, 6001},
		{kdberr.EncodingNegotiationFailure, 6002},
		{kdberr.TransportError, 6101},
		{kdberr.ComputeUnavailable, 6201},
		{kdberr.ComputeDispatchError, 6202},
		{kdberr.AuthenticationFailed, 6301},
		{kdberr.AuthorizationFailed, 6302},
		{kdberr.ArchiveRestore, 7001},
	} {
		if got := tc.code.Numeric(); got != tc.want {
			t.Errorf("code %d has numeric value %d, want %d - these are published and must not change",
				tc.code, got, tc.want)
		}
	}
}

func TestConflictOperationTypeNames(t *testing.T) {
	for _, tc := range []struct {
		op   kdberr.ConflictOperationType
		want string
	}{
		{kdberr.ConcurrentWrite, "CONCURRENT_WRITE"},
		{kdberr.WriteDelete, "WRITE_DELETE"},
		{kdberr.DeleteWrite, "DELETE_WRITE"},
		{kdberr.SchemaIncompatible, "SCHEMA_INCOMPATIBLE"},
	} {
		if got := tc.op.String(); got != tc.want {
			t.Errorf("%d.String() = %q, want %q", tc.op, got, tc.want)
		}
	}
}

// Result is how the schema layer returns either a value or a typed failure.
func TestResultOkAndFail(t *testing.T) {
	ok := kdberr.Ok(42)
	if !ok.IsSuccess() || ok.IsFailure() {
		t.Error("Ok is not reported as a success")
	}
	if v, present := ok.Value(); !present || v != 42 {
		t.Errorf("Value() = (%d, %v)", v, present)
	}
	if ok.Exception() != nil {
		t.Error("a successful Result carries an exception")
	}
	if got := ok.MustValue(); got != 42 {
		t.Errorf("MustValue() = %d", got)
	}

	exc := kdberr.NewSchemaError("bad", nil)
	fail := kdberr.Fail[int](exc)
	if fail.IsSuccess() || !fail.IsFailure() {
		t.Error("Fail is not reported as a failure")
	}
	if v, present := fail.Value(); present || v != 0 {
		t.Errorf("Value() on a failure = (%d, %v), want (0, false)", v, present)
	}
	if fail.Exception() != exc {
		t.Error("Exception() did not give back the exception")
	}
}

func TestResultMustValuePanicsOnFailure(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustValue on a failure did not panic")
		}
	}()
	kdberr.Fail[int](kdberr.NewSchemaError("bad", nil)).MustValue()
}

// Run turns a (value, error) pair into a Result, keeping a typed exception typed and giving a
// plain error somewhere to go rather than being dropped.
func TestRunConvertsBothErrorKinds(t *testing.T) {
	got := kdberr.Run(func() (int, error) { return 7, nil })
	if v, ok := got.Value(); !ok || v != 7 {
		t.Errorf("success case: (%d, %v)", v, ok)
	}

	exc := kdberr.NewConflictError("conflict", kdberr.ConflictReport{})
	got = kdberr.Run(func() (int, error) { return 0, exc })
	if got.IsSuccess() {
		t.Fatal("an exception was reported as success")
	}
	if got.Exception() != exc {
		t.Error("Run did not keep the typed exception")
	}

	plain := errors.New("something else")
	got = kdberr.Run(func() (int, error) { return 0, plain })
	if got.IsSuccess() {
		t.Fatal("a plain error was reported as success")
	}
	if got.Exception() == nil {
		t.Fatal("a plain error produced no exception at all")
	}
	if !strings.Contains(got.Exception().Error(), "something else") {
		t.Errorf("the original message was lost: %q", got.Exception().Error())
	}
}
