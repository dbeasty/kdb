package core

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"os"

	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TransportConnectOptions configures outbound transport connections.
type TransportConnectOptions struct {
	ConnectTimeoutMs int64
	ReadTimeoutMs    int64
	MaxFrameBytes    int
	ConnectHeaders   map[string]string
	TLS              *TransportTlsSettings
	// Admitter, when set, is consulted at each frame boundary with the frame's header, before
	// the body is buffered or decoded - kdb-spec-layer13 Component 48 §5.4's "admit early".
	// See FrameAdmitter.
	Admitter FrameAdmitter
	// MaxConnections caps concurrently-accepted server connections; 0 means unlimited.
	// Component 49 §6.5: an unbounded goroutine-per-connection model is a memory commitment the
	// grant system cannot see, since nothing charges it against the budget.
	MaxConnections int
	// IncomingQueueFrames overrides how many whole frames a connection may buffer awaiting a
	// consumer; 0 selects DefaultIncomingQueueFrames.
	IncomingQueueFrames int
}

// DefaultIncomingQueueFrames is how many decoded frames a connection buffers before the read
// loop stops reading and applies real backpressure to the peer.
//
// It was 32. At the 16MB DefaultMaxFrameBytes ceiling that is a 512MB per-connection commitment
// that no part of the system accounted for - with no connection cap, "32 x 16MB x N connections"
// was not a bound worth having (Component 48 §5.4: "reduce the per-connection incoming buffer
// from 32 frames to a small number (2-4) and account for it"). Four keeps a pipelining client
// from stalling on every single request while bounding the unaccounted commitment to an eighth
// of what it was.
const DefaultIncomingQueueFrames = 4

// FrameAdmitter decides, from a frame's header alone, whether the server will serve the request
// at all. It is called once per inbound frame, as soon as the header has arrived and *before*
// the body is buffered.
//
// Returning a nil error admits the frame, which is then read and dispatched normally. Returning
// a non-nil error sheds it: the body is consumed and discarded without ever being assembled or
// decoded, and the returned rejection frame - if any - is written back to the peer, so the client
// gets a typed, actionable answer instead of a request that silently vanishes.
//
// The point is cost asymmetry. Before this, a server already shedding load still paid to read
// the whole frame off the socket, assemble it, and JSON-decode it before anything looked at
// whether it was going to be served - so the requests arriving *because* the node was struggling
// were also the most expensive ones to refuse. A shed request now costs a header read and a small
// write.
type FrameAdmitter func(header wire.Header) (rejection []byte, err error)

// DefaultConnectOptions returns default transport options.
func DefaultConnectOptions() TransportConnectOptions {
	return TransportConnectOptions{MaxFrameBytes: wire.DefaultMaxFrameBytes}
}

// TransportTlsSettings configures TLS for transports that support it.
type TransportTlsSettings struct {
	Enabled            bool
	InsecureSkipVerify bool
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	// RequireClientAuth, server-side only, turns on mTLS: the peer must present a certificate
	// signed by CAFile, or the handshake fails. Requires CAFile to be set - there would
	// otherwise be nothing to verify a presented client certificate against.
	RequireClientAuth bool
}

// BuildTLSConfig turns s into a *tls.Config for either a server or a client role. Returns (nil,
// nil) when s is nil or s.Enabled is false - the transport-package caller decides what "no TLS
// configured" means for its own scheme (typically: fine for a plaintext URI, a hard error for a
// secure one - never a silent downgrade to plaintext when the caller asked for TLS).
func (s *TransportTlsSettings) BuildTLSConfig(server bool) (*tls.Config, error) {
	if s == nil || !s.Enabled {
		return nil, nil
	}
	cfg := &tls.Config{
		ServerName:         s.ServerName,
		InsecureSkipVerify: s.InsecureSkipVerify,
	}
	if s.CertFile != "" || s.KeyFile != "" {
		if s.CertFile == "" || s.KeyFile == "" {
			return nil, fmt.Errorf("tls: CertFile and KeyFile must both be set (or both empty)")
		}
		cert, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: load cert/key pair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	} else if server {
		return nil, fmt.Errorf("tls: server requires CertFile and KeyFile")
	}
	if s.CAFile != "" {
		pemBytes, err := os.ReadFile(s.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("tls: no certificates found in %s", s.CAFile)
		}
		if server {
			cfg.ClientCAs = pool
		} else {
			cfg.RootCAs = pool
		}
	}
	if server {
		if s.RequireClientAuth {
			if s.CAFile == "" {
				return nil, fmt.Errorf("tls: RequireClientAuth requires CAFile, to verify a presented client certificate against")
			}
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		} else if s.CAFile != "" {
			// A CAFile was given but client auth wasn't required - accept a client cert if
			// offered and verify it, but don't demand one (useful for a server that wants to
			// log/authorize by client identity when present without breaking clients that have
			// none).
			cfg.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return cfg, nil
}

// FrameStreamReader reassembles length-prefixed wire frames from a byte stream.
type FrameStreamReader struct {
	maxFrameBytes int
	pending       []byte

	// admit, when set, is consulted once per frame as soon as its header is available. See
	// FrameAdmitter.
	admit FrameAdmitter
	// discardRemaining counts down the body bytes of a shed frame still to be dropped. While it
	// is non-zero the reader consumes and throws away everything it is fed, which is what keeps
	// a shed frame from ever being assembled - and what lets the connection stay usable
	// afterward, since the stream is resynchronized exactly at the next frame boundary rather
	// than being torn down.
	discardRemaining int
	// headDecided guards against re-asking the admitter about the same frame each time more of
	// its body arrives - the decision is made once, on the header.
	headDecided bool
	rejections  [][]byte
}

func NewFrameStreamReader(maxFrameBytes int) *FrameStreamReader {
	if maxFrameBytes == 0 {
		maxFrameBytes = wire.DefaultMaxFrameBytes
	}
	return &FrameStreamReader{maxFrameBytes: maxFrameBytes}
}

// NewAdmittingFrameStreamReader returns a reader that consults admit at every frame boundary.
func NewAdmittingFrameStreamReader(maxFrameBytes int, admit FrameAdmitter) *FrameStreamReader {
	r := NewFrameStreamReader(maxFrameBytes)
	r.admit = admit
	return r
}

// TakeRejections returns and clears the rejection frames produced since the last call - the
// replies the caller should write back to the peer for frames that were shed.
func (r *FrameStreamReader) TakeRejections() [][]byte {
	if len(r.rejections) == 0 {
		return nil
	}
	out := r.rejections
	r.rejections = nil
	return out
}

func (r *FrameStreamReader) Feed(chunk []byte) ([][]byte, error) {
	if len(chunk) > 0 {
		r.pending = append(r.pending, chunk...)
	}
	return r.drainCompleteFrames()
}

func (r *FrameStreamReader) Reset() {
	r.pending = nil
	r.discardRemaining = 0
	r.headDecided = false
	r.rejections = nil
}

func (r *FrameStreamReader) BufferedBytes() int { return len(r.pending) }

func (r *FrameStreamReader) drainCompleteFrames() ([][]byte, error) {
	var out [][]byte
	for {
		// Finish dropping a shed frame's body before looking for the next frame. Nothing is
		// copied here: the bytes are consumed straight off the pending buffer, which is the
		// whole point of shedding at the header.
		if r.discardRemaining > 0 {
			n := r.discardRemaining
			if n > len(r.pending) {
				n = len(r.pending)
			}
			r.pending = r.pending[n:]
			r.discardRemaining -= n
			if r.discardRemaining > 0 {
				break // need more bytes before this frame is fully drained
			}
			continue
		}
		if len(r.pending) < 4 {
			break
		}
		frameLength := int(int32(binary.LittleEndian.Uint32(r.pending[:4])))
		if err := wire.ValidateFrameLength(frameLength, r.maxFrameBytes); err != nil {
			r.pending = nil
			return nil, err
		}
		if r.admit != nil && !r.headDecided {
			header, ok, err := wire.PeekHeader(r.pending, r.maxFrameBytes)
			if err != nil {
				// An unparseable header is not an admission decision - leave it to the normal
				// path, which produces the established decode error once the frame is whole.
				r.headDecided = true
			} else if !ok {
				break // header not fully arrived yet; decide once it is
			} else {
				r.headDecided = true
				if rejection, aerr := r.admit(header); aerr != nil {
					if rejection != nil {
						r.rejections = append(r.rejections, rejection)
					}
					r.discardRemaining = frameLength
					r.headDecided = false
					continue
				}
			}
		}
		if len(r.pending) < frameLength {
			break
		}
		frame := make([]byte, frameLength)
		copy(frame, r.pending[:frameLength])
		r.pending = r.pending[frameLength:]
		r.headDecided = false
		out = append(out, frame)
	}
	return out, nil
}

// ValidateInboundFrame checks that a frame received whole from a message-oriented transport
// really is one complete frame: its length prefix must be in bounds and must equal the buffer
// it arrived in. The stream transports get this invariant for free from FrameStreamReader,
// which only emits a buffer once exactly frameLength bytes have arrived; WebSocket delivers a
// whole message with no such guarantee, so a peer can hand over a buffer whose prefix disagrees
// with its size. That mismatch has to be rejected at the transport, before the decoder is asked
// to slice a payload that isn't there.
func ValidateInboundFrame(frame []byte, maxFrameBytes int) error {
	return ValidateOutgoingFrame(frame, maxFrameBytes)
}

// ValidateOutgoingFrame checks a complete outgoing frame length prefix.
func ValidateOutgoingFrame(frame []byte, maxFrameBytes int) error {
	if len(frame) < 4 {
		return kdberr.NewTransportErr("frame shorter than length prefix", nil)
	}
	length := int(int32(binary.LittleEndian.Uint32(frame[:4])))
	if err := wire.ValidateFrameLength(length, maxFrameBytes); err != nil {
		return err
	}
	if length != len(frame) {
		return kdberr.NewTransportErr("frame length prefix does not match buffer size", nil)
	}
	return nil
}

// ReadFrame reads one complete frame using chunked reads. readChunk returns nil at EOF.
func ReadFrame(maxFrameBytes int, readChunk func() ([]byte, error)) ([]byte, error) {
	reader := NewFrameStreamReader(maxFrameBytes)
	for {
		frames, err := reader.Feed(nil)
		if err != nil {
			return nil, err
		}
		if len(frames) > 0 {
			return frames[0], nil
		}
		chunk, err := readChunk()
		if err != nil {
			return nil, err
		}
		if chunk == nil {
			break
		}
		if len(chunk) == 0 {
			break
		}
		frames, err = reader.Feed(chunk)
		if err != nil {
			return nil, err
		}
		if len(frames) > 0 {
			return frames[0], nil
		}
	}
	if reader.BufferedBytes() > 0 {
		return nil, kdberr.NewConnectionClosedError(reader.BufferedBytes())
	}
	return nil, nil
}
