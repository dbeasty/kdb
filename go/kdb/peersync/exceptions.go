package peersync

import kdberr "github.com/limidus/kdb/go/kdb/error"

// Error is a peer sync failure.
type Error struct {
	*kdberr.TransportErr
}

func NewError(msg string, cause error) *Error {
	return &Error{TransportErr: kdberr.NewTransportErr(msg, cause)}
}
