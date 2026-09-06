package server

import (
	"context"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/ws"
	"github.com/limidus/kdb/go/kdb/wire"
)

// ListenSqlWireWSTLS serves the SQL wire protocol over WebSocket, the ws:// / wss:// counterpart
// to ListenSqlWireTLS.
//
// This is what makes the Go server reachable from a browser at all. Every other listener in this
// package speaks raw TCP, which a browser cannot open; WebSocket is the only transport available
// to page JavaScript, so without this the Go service - the production deployment target - could
// serve every client except the one that has to run in a tab.
//
// The connection handling above the transport is deliberately identical to the TCP path: the
// same codec, the same newSqlWireConnHandler, the same admitter. A WebSocket connection differs
// only in how bytes are framed, and anything that behaved differently here would be a
// divergence between what a browser client and a native client can do - exactly the kind of
// split that turns into "works in Node, fails in the browser" bug reports.
func ListenSqlWireWSTLS(
	addr string,
	runtime *KdbServerRuntime,
	tlsSettings *core.TransportTlsSettings,
) (*Listener, error) {
	codec := wire.NewCodec(wire.EncodingJSON)
	opts := core.DefaultConnectOptions()
	opts.TLS = tlsSettings
	opts.MaxConnections = runtime.MaxConnections
	opts.Admitter = runtime.frameAdmitter(codec)

	transport := ws.NewTransport(opts)
	ln, err := transport.ListenBound(addr, opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	l := &Listener{ln: ln, cancel: cancel, done: done}
	go func() {
		defer close(done)
		_ = transport.Serve(ctx, ln, opts, func(conn stream.ConnectionHandle) {
			newSqlWireConnHandler(codec, runtime).run(conn)
		})
	}()
	return l, nil
}

// ListenSqlWireWS is ListenSqlWireWSTLS with no TLS settings, for a plaintext ws:// listener.
func ListenSqlWireWS(addr string, runtime *KdbServerRuntime) (*Listener, error) {
	return ListenSqlWireWSTLS(addr, runtime, nil)
}
