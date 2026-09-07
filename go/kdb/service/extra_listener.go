package service

import (
	"io"
	"net"

	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

// GRPCListener is the shape Main needs from a running gRPC listener: something it can report the
// bound address of and close on shutdown, the same two things it uses every other listener for.
type GRPCListener interface {
	io.Closer
	Addr() net.Addr
}

// GRPCListenerFactory starts the gRPC listener. It takes the same three things every other
// listener in Main takes, so wiring one is not a different shape of work from wiring the others.
type GRPCListenerFactory func(addr string, rt *server.KdbServerRuntime, tls *core.TransportTlsSettings) (GRPCListener, error)

// grpcListenerFactory is nil in a build that does not link the gRPC module - which is the default
// build, and deliberately so.
//
// gRPC lives in its own Go module so its transitive dependencies never reach the embedded and
// gomobile builds (docs/kdb-spec-layer17-multi-namespace-runtime.md §3.2). That means this
// package cannot import it, and the edge has to run the other way: the gRPC module registers
// itself here, and the binary that wants it links the registering package. A build that has not
// done so refuses a non-empty --grpc-addr rather than ignoring it, because a flag that silently
// does nothing is worse than no flag at all.
var grpcListenerFactory GRPCListenerFactory

// RegisterGRPCListener installs the gRPC listener for this build. Called from an init function
// in github.com/limidus/kdb/go/grpc/register, which kdb-service-grpc links and kdb-service does
// not. Not safe for concurrent use and not meant to be: it runs at init, before Main.
func RegisterGRPCListener(f GRPCListenerFactory) { grpcListenerFactory = f }

// GRPCLinked reports whether this build can serve --grpc-addr.
func GRPCLinked() bool { return grpcListenerFactory != nil }
