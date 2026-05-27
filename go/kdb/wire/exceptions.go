package wire

import (
	"fmt"

	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// DecodeError is a wire frame or payload decode failure.
type DecodeError struct {
	*kdberr.DecodeError
}

func newDecodeError(msg string) *DecodeError {
	return &DecodeError{DecodeError: kdberr.NewDecodeError(msg, -1, nil)}
}

// FrameTooLargeError indicates a frame exceeds the configured maximum.
type FrameTooLargeError struct {
	*kdberr.DecodeError
	Length int
	Max    int
}

func newFrameTooLarge(length, max int) *FrameTooLargeError {
	return &FrameTooLargeError{
		DecodeError: kdberr.NewDecodeError(
			fmt.Sprintf("frame length %d exceeds max %d", length, max),
			-1,
			nil,
		),
		Length: length,
		Max:    max,
	}
}

// InvalidCorrelationError indicates an unknown correlation id.
type InvalidCorrelationError struct {
	*kdberr.DecodeError
	CorrelationID int
}

func NewInvalidCorrelationError(id int) *InvalidCorrelationError {
	return &InvalidCorrelationError{
		DecodeError:   kdberr.NewDecodeError(fmt.Sprintf("unknown correlation %d", id), -1, nil),
		CorrelationID: id,
	}
}
