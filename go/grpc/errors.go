package kdbgrpc

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isStreamEnd reports whether err is an ordinary end of stream rather than a fault worth
// returning from the handler.
//
// A client that has finished and half-closed, and a client that has simply gone away, are both
// normal ways for a connection to end - the socket listeners treat a closed connection the same
// way. Returning a status for either would fill a server's logs with errors describing clients
// behaving correctly.
func isStreamEnd(err error) bool {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.Unavailable:
		return true
	default:
		return false
	}
}
