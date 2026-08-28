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
}

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
}

func NewFrameStreamReader(maxFrameBytes int) *FrameStreamReader {
	if maxFrameBytes == 0 {
		maxFrameBytes = wire.DefaultMaxFrameBytes
	}
	return &FrameStreamReader{maxFrameBytes: maxFrameBytes}
}

func (r *FrameStreamReader) Feed(chunk []byte) ([][]byte, error) {
	if len(chunk) > 0 {
		r.pending = append(r.pending, chunk...)
	}
	return r.drainCompleteFrames()
}

func (r *FrameStreamReader) Reset() { r.pending = nil }

func (r *FrameStreamReader) BufferedBytes() int { return len(r.pending) }

func (r *FrameStreamReader) drainCompleteFrames() ([][]byte, error) {
	var out [][]byte
	for {
		if len(r.pending) < 4 {
			break
		}
		frameLength := int(int32(binary.LittleEndian.Uint32(r.pending[:4])))
		if err := wire.ValidateFrameLength(frameLength, r.maxFrameBytes); err != nil {
			r.pending = nil
			return nil, err
		}
		if len(r.pending) < frameLength {
			break
		}
		frame := make([]byte, frameLength)
		copy(frame, r.pending[:frameLength])
		r.pending = r.pending[frameLength:]
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
