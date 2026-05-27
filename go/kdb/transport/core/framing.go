package core

import (
	"encoding/binary"

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
