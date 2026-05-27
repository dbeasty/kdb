package stream

import (
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// DesyncError indicates the parent hash did not match the expected position.
type DesyncError struct {
	*kdberr.DecodeError
	ExpectedParent string
	ActualParent   string
}

func NewDesyncError(expected, actual string) *DesyncError {
	return &DesyncError{
		DecodeError:    kdberr.NewDecodeError("stream desync: expected parent "+expected+", got "+actual, -1, nil),
		ExpectedParent: expected,
		ActualParent:   actual,
	}
}

// NotConnectedError indicates the subscriber is not connected.
type NotConnectedError struct{ *kdberr.DecodeError }

func NewNotConnectedError() *NotConnectedError {
	return &NotConnectedError{DecodeError: kdberr.NewDecodeError("stream not connected", -1, nil)}
}
