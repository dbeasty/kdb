package server

import (
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// FrameConn is the whole of what serving a client connection needs from a transport: frames in,
// frames out. Nothing here knows about sockets, TLS or HTTP/2.
//
// stream.ConnectionHandle satisfies this, which is why the TCP and WebSocket listeners needed no
// change when it was introduced.
type FrameConn interface {
	// Incoming yields complete, length-prefixed wire frames - exactly what wire.Codec.Decode
	// and wire.PeekHeader expect - and is closed when the peer goes away.
	Incoming() <-chan []byte
	// Send writes one encoded frame back. Implementations need not be safe for concurrent use;
	// FrameHost serializes sends itself.
	Send(frame []byte) error
}

// FrameHost serves client connections for one runtime, independent of the transport carrying
// them.
//
// This is the seam a new transport plugs into, and it exists because everything that actually
// makes a KDB connection a KDB connection sits above the socket: the JSON wire codec, the
// handshake and its authentication, RBAC re-checked at commit time, session lifecycle and the
// per-session frame ordering that sessions depend on, memory-pressure shedding, the bound on
// concurrently dispatched frames, and the per-request panic backstop. A transport that
// reimplemented any of that would be a divergence between what one kind of client can do and
// what another can - the split ListenSqlWireWSTLS's doc comment already warns about, and the
// reason WebSocket was built on the same handler rather than beside it.
//
// So: supply bytes, inherit the rest. See go/grpc for the transport that motivated exporting it.
type FrameHost struct {
	runtime *KdbServerRuntime
	codec   wire.Codec
}

// NewFrameHost returns a host serving connections for rt over the standard JSON wire codec - the
// same codec every listener in this package uses.
func NewFrameHost(rt *KdbServerRuntime) *FrameHost {
	return &FrameHost{runtime: rt, codec: wire.NewCodec(wire.EncodingJSON)}
}

// Codec is the wire codec this host speaks.
func (f *FrameHost) Codec() wire.Codec { return f.codec }

// Admitter is the memory-pressure gate: given a frame's header it returns either nothing (serve
// it) or a typed refusal to send back instead (shed it). The byte-stream transports install this
// in their frame reader, where it can drop a request's body without ever reading it; a
// message-oriented transport that already holds the whole frame should use Admit instead.
func (f *FrameHost) Admitter() core.FrameAdmitter { return f.runtime.frameAdmitter(f.codec) }

// Admit applies the memory-pressure gate to one complete frame. It returns the refusal frame to
// send back in place of serving the request, or nil to serve it.
//
// For transports that deliver whole messages and so cannot shed before reading a body - there is
// no I/O to save by then, but the shedding behaviour itself still has to match, or a server under
// pressure would refuse a TCP client and accept an identical request from a gRPC one.
//
// A frame whose header will not parse is admitted rather than refused: that is not an admission
// decision, and letting it through produces the established decode error on the normal path
// instead of an invented one here.
func (f *FrameHost) Admit(frame []byte) []byte {
	header, ok, err := wire.PeekHeader(frame, wire.DefaultMaxFrameBytes)
	if err != nil || !ok {
		return nil
	}
	rejection, shed := f.Admitter()(header)
	if shed == nil {
		return nil
	}
	return rejection
}

// Serve runs one connection to completion, returning when its inbound frames stop.
//
// Every session opened on the connection is ended and every document lock it held is released
// before Serve returns, and in-flight frames are waited for first - a client that drops
// mid-transaction must not leave its locks held for the life of the process, and must not have
// its leases released while a frame on that session is still executing.
func (f *FrameHost) Serve(conn FrameConn) {
	newSqlWireConnHandler(f.codec, f.runtime).run(conn)
}
