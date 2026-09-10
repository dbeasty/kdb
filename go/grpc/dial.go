package kdbgrpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	kdbwirev1 "github.com/limidus/kdb/go/grpc/gen/kdbwire/v1"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Transport dials a KDB gRPC listener and presents it as a stream.Transport,
// so client.ConnectWithOptions can drive it with the ordinary client.
//
// This is the dialing half of what register.go registers on the server side.
// It lives in this module for the same reason the listener does: the core
// module must not depend on google.golang.org/grpc (it is what gomobile and
// embedbundle build from), so the dependency is inverted - the caller hands
// this to client.ConnectOptions.Transport and the core module stays unaware
// that gRPC exists.
//
// One client connection is one bidirectional stream, which is load-bearing
// rather than incidental: sessions are scoped to a connection, own document
// locks, and require frames naming the same session to be served in send
// order. HTTP/2 gives no ordering *between* streams, so spreading one client
// over several would break all of that. See the KdbWireClient.Connect doc
// comment.
type Transport struct {
	// TLS, when set, is used instead of an insecure connection.
	TLS *tls.Config
	// DialOptions are appended to the defaults, for callers needing
	// interceptors, keepalives or credentials of their own.
	DialOptions []grpc.DialOption
}

// NewTransport returns a Transport dialing plaintext unless TLS is set on it.
func NewTransport() *Transport { return &Transport{} }

// Connect implements stream.Transport. uri may carry a grpc:// or grpcs://
// scheme, or be a bare host:port.
func (t *Transport) Connect(uri string) (stream.ConnectionHandle, error) {
	target := uri
	useTLS := t.TLS != nil
	switch {
	case strings.HasPrefix(target, "grpcs://"):
		target, useTLS = strings.TrimPrefix(target, "grpcs://"), true
	case strings.HasPrefix(target, "grpc://"):
		target = strings.TrimPrefix(target, "grpc://")
	}
	if target == "" {
		return nil, fmt.Errorf("kdb/grpc: empty dial target in %q", uri)
	}

	creds := insecure.NewCredentials()
	if useTLS {
		cfg := t.TLS
		if cfg == nil {
			cfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		creds = credentials.NewTLS(cfg)
	}
	// The frame size limits match the listener's, so a frame this transport can
	// carry in one direction it can carry in the other - a mismatch would show
	// up only on whichever payload first exceeded the smaller side.
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(wire.DefaultMaxFrameBytes+frameOverheadBytes),
			grpc.MaxCallSendMsgSize(wire.DefaultMaxFrameBytes+frameOverheadBytes),
		),
	}, t.DialOptions...)

	cc, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s, err := kdbwirev1.NewKdbWireClient(cc).Connect(ctx)
	if err != nil {
		cancel()
		_ = cc.Close()
		return nil, err
	}

	c := &grpcConnection{
		cc:       cc,
		stream:   s,
		cancel:   cancel,
		incoming: make(chan []byte, incomingBufferFrames),
	}
	go c.readLoop()
	return c, nil
}

const (
	// frameOverheadBytes is the slack the listener allows over a maximum wire
	// frame for protobuf framing; kept identical to grpc/listen.go's.
	frameOverheadBytes = 64
	// incomingBufferFrames buffers received frames so a burst does not block
	// the reader goroutine on a client that is between polls.
	incomingBufferFrames = 64
)

type grpcConnection struct {
	cc     *grpc.ClientConn
	stream grpc.BidiStreamingClient[kdbwirev1.Frame, kdbwirev1.Frame]
	cancel context.CancelFunc

	incoming chan []byte

	mu     sync.Mutex
	closed bool
}

func (c *grpcConnection) Send(frame []byte) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return fmt.Errorf("kdb/grpc: connection closed")
	}
	return c.stream.Send(&kdbwirev1.Frame{Payload: frame})
}

func (c *grpcConnection) Incoming() <-chan []byte { return c.incoming }

func (c *grpcConnection) TryPoll() []byte {
	select {
	case frame := <-c.incoming:
		return frame
	default:
		return nil
	}
}

func (c *grpcConnection) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	_ = c.stream.CloseSend()
	// Cancelling unblocks readLoop's Recv, which is what closes c.incoming -
	// closing it here instead would race with a send from that goroutine.
	c.cancel()
	return c.cc.Close()
}

func (c *grpcConnection) readLoop() {
	defer close(c.incoming)
	for {
		frame, err := c.stream.Recv()
		if err != nil {
			// Includes the ordinary end of stream and the cancellation Close
			// issues. The client above surfaces the disconnect when its
			// pending requests fail; there is nothing to report from here.
			return
		}
		c.incoming <- frame.GetPayload()
	}
}
