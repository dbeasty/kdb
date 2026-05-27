package error

import "testing"

func TestResultOkFail(t *testing.T) {
	r := Ok(42)
	if !r.IsSuccess() || r.MustValue() != 42 {
		t.Fatal("expected ok 42")
	}
	f := Fail[int](NewDecodeError("bad", 3, nil))
	if !f.IsFailure() || f.Exception() == nil {
		t.Fatal("expected failure")
	}
	if f.Exception().Code() != KdbDecodeError {
		t.Fatalf("code %v", f.Exception().Code())
	}
}

func TestStableErrorCodes(t *testing.T) {
	if KdbDecodeError.Numeric() != 1001 {
		t.Fatal("decode code changed")
	}
	if Conflict.Numeric() != 4001 {
		t.Fatal("conflict code changed")
	}
}
