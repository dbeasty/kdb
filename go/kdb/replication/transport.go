package replication

import (
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
)

func defaultTransport(tls *core.TransportTlsSettings) stream.Transport {
	opts := core.DefaultConnectOptions()
	opts.TLS = tls
	return tcp.NewTransport(opts)
}
