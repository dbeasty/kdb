package wire

import (
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// HandshakeNegotiator selects encoding and protocol version during handshake.
type HandshakeNegotiator interface {
	Negotiate(local, remote HandshakePayload) (HandshakeAckPayload, error)
}

// DefaultHandshakeNegotiator is the v1 handshake negotiator.
type DefaultHandshakeNegotiator struct{}

func NewHandshakeNegotiator() *DefaultHandshakeNegotiator {
	return &DefaultHandshakeNegotiator{}
}

func (n *DefaultHandshakeNegotiator) Negotiate(local, remote HandshakePayload) (HandshakeAckPayload, error) {
	remotePV := remote.ProtocolVersion
	if remotePV == 0 {
		remotePV = KdbWireProtocolVersion
	}
	if remotePV > KdbWireProtocolVersion || remotePV < MinSupportedWireProtocolVersion {
		return HandshakeAckPayload{}, kdberr.NewUnsupportedProtocolVersionError(
			"peer protocol not supported",
			remotePV,
			KdbWireProtocolVersion,
		)
	}
	encoding, ok := intersectEncodings(local.PreferredEncodings, remote.PreferredEncodings)
	if !ok {
		localNames := encodingNames(local.PreferredEncodings)
		remoteNames := encodingNames(remote.PreferredEncodings)
		return HandshakeAckPayload{}, kdberr.NewEncodingNegotiationFailureError(
			"no common encoding",
			localNames,
			remoteNames,
		)
	}
	pv := KdbWireProtocolVersion
	if remotePV < pv {
		pv = remotePV
	}
	return HandshakeAckPayload{
		Accepted:           true,
		NegotiatedEncoding: encoding,
		ProtocolVersion:    pv,
		RemoteHeads:        remote.LocalHeads,
	}, nil
}

func intersectEncodings(a, b []PayloadEncoding) (PayloadEncoding, bool) {
	if len(a) == 0 {
		a = []PayloadEncoding{EncodingKdbBinary, EncodingJSON}
	}
	if len(b) == 0 {
		b = []PayloadEncoding{EncodingKdbBinary, EncodingJSON}
	}
	for _, enc := range a {
		for _, other := range b {
			if enc == other {
				return enc, true
			}
		}
	}
	return 0, false
}

func encodingNames(encs []PayloadEncoding) []string {
	if len(encs) == 0 {
		encs = []PayloadEncoding{EncodingKdbBinary, EncodingJSON}
	}
	out := make([]string, len(encs))
	for i, e := range encs {
		out[i] = e.String()
	}
	return out
}
