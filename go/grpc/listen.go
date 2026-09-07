// Package kdbgrpc carries the KDB wire protocol over gRPC.
//
// It is a transport and nothing more. Every frame it moves is one complete, length-prefixed KDB
// wire frame, byte for byte what the TCP and WebSocket listeners carry, and everything above the
// socket - handshake authentication, RBAC, sessions and their frame ordering, SQL execution,
// memory-pressure shedding, the per-request panic backstop - is the core's, reached through
// server.FrameHost. There is no second dispatch table here to keep in step with the wire message
// types, and no behaviour a gRPC client gets that a TCP client does not.
//
// Why it exists: trading a network hop and serialization for process isolation and independent
// restart is a reasonable bargain for some deployments, and gRPC is what most of them expect to
// speak. See docs/kdb-spec-layer17-multi-namespace-runtime.md §3.
package kdbgrpc

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	kdbwirev1 "github.com/limidus/kdb/go/grpc/gen/kdbwire/v1"
)

// protoOverheadBytes is slack for the Frame message wrapping a wire frame: a tag byte and a
// varint length. Ten bytes is more than a 16 MiB length needs and costs nothing.
const protoOverheadBytes = 10

// Options configures a listener.
type Options struct {
	// TLS, when set, serves grpcs:// with these settings.
	TLS *tls.Config
	// MaxFrameBytes bounds one wire frame. Zero means wire.DefaultMaxFrameBytes.
	MaxFrameBytes int
	// ServerOptions are appended to the gRPC server options this package builds, for anything
	// it does not model itself - interceptors, keepalive policy, stats handlers.
	ServerOptions []grpc.ServerOption
}

// Listener is a running gRPC listener. Close stops it gracefully.
type Listener struct {
	srv  *grpc.Server
	ln   net.Listener
	done chan struct{}
}

// Addr is the bound local address, useful when the listen URI's port was 0.
func (l *Listener) Addr() net.Addr { return l.ln.Addr() }

// Close stops accepting, lets in-flight streams finish, and waits for the serve loop to exit.
func (l *Listener) Close() error {
	l.srv.GracefulStop()
	<-l.done
	return nil
}

// Listen serves the KDB wire protocol over gRPC on addr, which may be a bare host:port or a
// grpc:// / grpcs:// URI. A grpcs:// address requires Options.TLS.
//
// Mirrors server.ListenSqlWire's shape deliberately: same argument order, same Listener contract,
// so a service wiring one listener can wire the other without learning a second idiom.
func Listen(addr string, rt *server.KdbServerRuntime, opts Options) (*Listener, error) {
	hostPort, wantTLS, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	if wantTLS && opts.TLS == nil {
		return nil, fmt.Errorf("kdbgrpc: %q requires TLS settings", addr)
	}
	maxFrame := opts.MaxFrameBytes
	if maxFrame <= 0 {
		maxFrame = wire.DefaultMaxFrameBytes
	}

	ln, err := net.Listen("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	// gRPC defaults to a 4 MiB receive limit, and a KDB frame may be 16 MiB. Left at the
	// default, a large scan or snapshot response would be refused by the transport - and the
	// failure would look like a KDB error rather than a transport limit. Raised explicitly on
	// both directions so the frame bound is the wire protocol's, in one place.
	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxFrame + protoOverheadBytes),
		grpc.MaxSendMsgSize(maxFrame + protoOverheadBytes),
	}
	if opts.TLS != nil {
		serverOpts = append(serverOpts, grpc.Creds(credentials.NewTLS(opts.TLS)))
	}
	serverOpts = append(serverOpts, opts.ServerOptions...)

	srv := grpc.NewServer(serverOpts...)
	kdbwirev1.RegisterKdbWireServer(srv, &wireService{
		host:     server.NewFrameHost(rt),
		maxFrame: maxFrame,
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ln)
	}()
	return &Listener{srv: srv, ln: ln, done: done}, nil
}

// parseAddr accepts "host:port", "grpc://host:port" and "grpcs://host:port". The query string is
// ignored, so the "?bind=true" the other listeners' URIs carry is accepted and harmless.
func parseAddr(addr string) (hostPort string, useTLS bool, err error) {
	if !strings.Contains(addr, "://") {
		return addr, false, nil
	}
	u, err := url.Parse(addr)
	if err != nil {
		return "", false, fmt.Errorf("kdbgrpc: parsing %q: %w", addr, err)
	}
	switch u.Scheme {
	case "grpc":
		return u.Host, false, nil
	case "grpcs":
		return u.Host, true, nil
	default:
		return "", false, fmt.Errorf("kdbgrpc: %q: want a grpc:// or grpcs:// address", addr)
	}
}

type wireService struct {
	kdbwirev1.UnimplementedKdbWireServer
	host     *server.FrameHost
	maxFrame int
}

// Connect serves one client connection for the life of one stream.
//
// The reader loop runs here and dispatch runs in FrameHost.Serve, which is the same division the
// socket transports have: frames are handed over in arrival order, and the ordering sessions
// depend on is the core's to maintain, not this package's to reimplement.
func (s *wireService) Connect(stream kdbwirev1.KdbWire_ConnectServer) error {
	conn := newStreamConn(stream)

	served := make(chan struct{})
	go func() {
		defer close(served)
		// Returns once the incoming channel closes, having ended every session opened on this
		// connection and released every document lock it held.
		s.host.Serve(conn)
	}()

	var recvErr error
	for {
		msg, err := stream.Recv()
		if err != nil {
			recvErr = err
			break
		}
		frame := msg.GetPayload()
		if len(frame) > s.maxFrame {
			// Refused here rather than let through: a frame past the protocol's own bound is
			// not something the dispatch path is expected to be handed.
			recvErr = fmt.Errorf("kdbgrpc: frame of %d bytes exceeds the %d byte limit", len(frame), s.maxFrame)
			break
		}
		// The same memory-pressure gate the socket transports apply in their frame reader. A
		// server shedding load must refuse a gRPC client exactly as it refuses a TCP one.
		if rejection := s.host.Admit(frame); rejection != nil {
			if err := conn.Send(rejection); err != nil {
				recvErr = err
				break
			}
			continue
		}
		conn.push(frame)
	}

	// Closing incoming is what tells Serve the connection is over; waiting for it is what makes
	// "the stream ended" and "this connection's sessions and locks are gone" the same event,
	// rather than leaving cleanup racing the next stream.
	conn.closeIncoming()
	<-served

	if recvErr != nil && !isStreamEnd(recvErr) {
		return recvErr
	}
	return nil
}

// streamConn adapts one gRPC bidirectional stream to server.FrameConn.
type streamConn struct {
	stream kdbwirev1.KdbWire_ConnectServer
	in     chan []byte
	// sendMu serializes Send. FrameHost serializes its own dispatch replies, but shed
	// rejections are written from the reader goroutine, so the two can overlap - and a gRPC
	// stream is not safe for concurrent SendMsg.
	sendMu sync.Mutex
}

// streamConnBuffer is how many frames may sit between the reader and dispatch. Small on purpose:
// FrameHost already bounds how many frames it will dispatch at once, and a deeper buffer here
// would only move a slow server's backlog off the connection and into memory.
const streamConnBuffer = 16

func newStreamConn(stream kdbwirev1.KdbWire_ConnectServer) *streamConn {
	return &streamConn{stream: stream, in: make(chan []byte, streamConnBuffer)}
}

func (c *streamConn) Incoming() <-chan []byte { return c.in }

func (c *streamConn) Send(frame []byte) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream.Send(&kdbwirev1.Frame{Payload: frame})
}

// push hands one frame to dispatch, blocking while the buffer is full - which is the
// backpressure that keeps a fast client from queueing work faster than the server retires it.
// Only ever called from the reader goroutine, which is also the only caller of closeIncoming.
func (c *streamConn) push(frame []byte) { c.in <- frame }

func (c *streamConn) closeIncoming() { close(c.in) }
