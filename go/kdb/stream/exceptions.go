package stream

import (
	"fmt"

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

// EventOverflowError is the Cause of the terminal EventDisconnected a subscriber emits when its
// consumer let the events buffer fill: the subscription is closed rather than lose Dropped, the
// event that did not fit. Unwrap returns Dropped's own Cause, so errors.As still finds a
// *DesyncError that met a full buffer. Reconnect from Connection.Position after draining Events.
type EventOverflowError struct {
	Buffered int
	Dropped  Event
}

func (e *EventOverflowError) Error() string {
	msg := fmt.Sprintf("stream: subscription closed: %d undrained events filled the buffer", e.Buffered)
	if e.Dropped.Cause != nil {
		msg += "; the event that did not fit carried: " + e.Dropped.Cause.Error()
	}
	return msg
}

func (e *EventOverflowError) Unwrap() error { return e.Dropped.Cause }

// NotConnectedError indicates the subscriber is not connected.
type NotConnectedError struct{ *kdberr.DecodeError }

func NewNotConnectedError() *NotConnectedError {
	return &NotConnectedError{DecodeError: kdberr.NewDecodeError("stream not connected", -1, nil)}
}
