package error

import "fmt"

// UnsupportedProtocolVersionError indicates an incompatible wire protocol version.
type UnsupportedProtocolVersionError struct {
	*base
	PeerVersion      int
	SupportedVersion int
}

func NewUnsupportedProtocolVersionError(msg string, peer, supported int) *UnsupportedProtocolVersionError {
	return &UnsupportedProtocolVersionError{
		base:             &base{code: UnsupportedProtocolVersion, msg: msg},
		PeerVersion:      peer,
		SupportedVersion: supported,
	}
}

// EncodingNegotiationFailureError indicates no common payload encoding.
type EncodingNegotiationFailureError struct {
	*base
	LocalEncodings  []string
	RemoteEncodings []string
}

func NewEncodingNegotiationFailureError(msg string, local, remote []string) *EncodingNegotiationFailureError {
	return &EncodingNegotiationFailureError{
		base:            &base{code: EncodingNegotiationFailure, msg: msg},
		LocalEncodings:  local,
		RemoteEncodings: remote,
	}
}

// ConnectionClosedError indicates the transport closed before a complete frame.
type ConnectionClosedError struct {
	*base
	BufferedBytes int
}

func NewConnectionClosedError(buffered int) *ConnectionClosedError {
	return &ConnectionClosedError{
		base: &base{
			code: TransportError,
			msg:  fmt.Sprintf("EOF before full frame (%d bytes buffered)", buffered),
		},
		BufferedBytes: buffered,
	}
}
