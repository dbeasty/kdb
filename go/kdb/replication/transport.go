package replication

import (
	"strings"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/transport/ws"
)

// defaultTransport picks the transport by the peer address's scheme: WebSocket for ws:// and
// wss:// (a peer handler mounted on an HTTP server), TCP otherwise.
func defaultTransport(addr string, tls *core.TransportTlsSettings) stream.Transport {
	opts := core.DefaultConnectOptions()
	opts.TLS = tls
	if strings.HasPrefix(addr, "ws://") || strings.HasPrefix(addr, "wss://") {
		return ws.NewTransport(opts)
	}
	return tcp.NewTransport(opts)
}
