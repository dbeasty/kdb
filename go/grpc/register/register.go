// Package register links the gRPC transport into kdb-service.
//
// Importing it for side effects is what turns --grpc-addr from an error into a listener. It is a
// separate package from kdbgrpc so that importing the transport (to embed it in something else)
// does not silently also register it with the service - linking a listener into a binary should
// be a decision that binary makes.
package register

import (
	"crypto/tls"

	kdbgrpc "github.com/limidus/kdb/go/grpc"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/service"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

func init() {
	service.RegisterGRPCListener(func(
		addr string, rt *server.KdbServerRuntime, tlsSettings *core.TransportTlsSettings,
	) (service.GRPCListener, error) {
		var tlsCfg *tls.Config
		if tlsSettings != nil {
			// The server role, and the same settings every other listener is built from - so
			// --tls-cert/--tls-key/--tls-ca and mTLS mean here exactly what they mean there.
			cfg, err := tlsSettings.BuildTLSConfig(true)
			if err != nil {
				return nil, err
			}
			tlsCfg = cfg
		}
		return kdbgrpc.Listen(addr, rt, kdbgrpc.Options{TLS: tlsCfg})
	})
}
