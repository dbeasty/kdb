package tcp

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

// Transport connects and listens over TCP with length-prefixed wire frames.
type Transport interface {
	stream.Transport
	Listen(ctx context.Context, uri string, handler func(stream.ConnectionHandle)) error

	// ListenBound resolves uri and binds synchronously, so a caller can learn the bound
	// address (e.g. when uri's port is 0) before frames start flowing. Pair with Serve to
	// run the accept loop once bound. Listen itself is ListenBound+Serve for callers that
	// don't need the address ahead of time.
	ListenBound(uri string) (net.Listener, error)
	// Serve runs the accept loop over an already-bound listener (from ListenBound) until
	// ctx is canceled or Accept fails; it closes ln before returning.
	Serve(ctx context.Context, ln net.Listener, handler func(stream.ConnectionHandle)) error
}

type defaultTransport struct {
	options core.TransportConnectOptions
}

// NewTransport returns a TCP wire transport with the given options.
func NewTransport(options core.TransportConnectOptions) Transport {
	if options.MaxFrameBytes == 0 {
		options.MaxFrameBytes = core.DefaultConnectOptions().MaxFrameBytes
	}
	return &defaultTransport{options: options}
}

func (t *defaultTransport) Connect(uri string) (stream.ConnectionHandle, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	if parsed.Bind {
		return nil, fmt.Errorf("connect URI must not use bind=true: %s", uri)
	}
	dialer := net.Dialer{Timeout: timeoutFromMs(t.options.ConnectTimeoutMs)}
	conn, err := dialer.Dial("tcp", net.JoinHostPort(parsed.Host, strconv.Itoa(parsed.Port)))
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return newSocketConnection(conn, t.options), nil
}

func (t *defaultTransport) Listen(ctx context.Context, uri string, handler func(stream.ConnectionHandle)) error {
	ln, err := t.ListenBound(uri)
	if err != nil {
		return err
	}
	return t.Serve(ctx, ln, handler)
}

func (t *defaultTransport) ListenBound(uri string) (net.Listener, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	if !parsed.Bind {
		return nil, fmt.Errorf("listen URI requires bind=true: %s", uri)
	}
	return net.Listen("tcp", net.JoinHostPort(parsed.Host, strconv.Itoa(parsed.Port)))
}

func (t *defaultTransport) Serve(ctx context.Context, ln net.Listener, handler func(stream.ConnectionHandle)) error {
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		go handler(newSocketConnection(conn, t.options))
	}
}

type socketConnection struct {
	conn          net.Conn
	maxFrameBytes int
	reader        *core.FrameStreamReader
	incoming      chan []byte
	// done is closed exactly once, by Close, to unblock readLoop if it's currently blocked
	// trying to deliver a frame on incoming (see readLoop's doc comment).
	done   chan struct{}
	closed bool
	mu     sync.Mutex
}

func newSocketConnection(conn net.Conn, options core.TransportConnectOptions) *socketConnection {
	max := options.MaxFrameBytes
	if max == 0 {
		max = core.DefaultConnectOptions().MaxFrameBytes
	}
	c := &socketConnection{
		conn:          conn,
		maxFrameBytes: max,
		reader:        core.NewFrameStreamReader(max),
		incoming:      make(chan []byte, 32),
		done:          make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// readLoop is incoming's only sender and its only closer - both previous bugs here traced back
// to that not being true. It used to send with `select { case incoming <- frame: default: }`,
// silently dropping a frame whenever a consumer fell behind the 32-slot buffer (a lost request
// on the server side, a lost reply on the client side - the caller then just blocks until its
// own timeout). Blocking here instead applies real backpressure, but a blocking send needed a
// way to be interrupted by Close() (called from another goroutine, or from readLoop's own error
// paths below) without racing it - `done` is that: Close closes it once, this select then
// returns instead of sending. incoming itself is only ever closed here, via the deferred call,
// once this loop has permanently stopped trying to send on it - Close used to close incoming
// directly while a concurrent send from this goroutine held no such guarantee, which could panic
// with "send on closed channel".
func (c *socketConnection) readLoop() {
	defer close(c.incoming)
	buf := make([]byte, 4096)
	for {
		n, err := c.conn.Read(buf)
		if n > 0 {
			frames, ferr := c.reader.Feed(buf[:n])
			if ferr != nil {
				_ = c.Close()
				return
			}
			for _, frame := range frames {
				select {
				case c.incoming <- frame:
				case <-c.done:
					return
				}
			}
		}
		if err != nil {
			_ = c.Close()
			return
		}
	}
}

func (c *socketConnection) Send(frame []byte) error {
	if err := core.ValidateOutgoingFrame(frame, c.maxFrameBytes); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return stream.NewNotConnectedError()
	}
	_, err := c.conn.Write(frame)
	return err
}

func (c *socketConnection) Incoming() <-chan []byte { return c.incoming }

func (c *socketConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.done)
	return c.conn.Close()
}

func (c *socketConnection) TryPoll() []byte {
	select {
	case frame := <-c.incoming:
		return frame
	default:
		return nil
	}
}

func timeoutFromMs(ms int64) time.Duration {
	if ms <= 0 {
		return 10 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

// ParseURI parses tcp://host:port?bind=true wire transport URIs.
func ParseURI(raw string) (TransportURI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return TransportURI{}, err
	}
	if u.Scheme != "tcp" && u.Scheme != "kdb+tcp" {
		return TransportURI{}, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		host = "127.0.0.1"
	}
	port := 9090
	if p := u.Port(); p != "" {
		port, err = strconv.Atoi(p)
		if err != nil {
			return TransportURI{}, err
		}
	}
	bind := u.Query().Get("bind") == "true"
	return TransportURI{Host: host, Port: port, Bind: bind}, nil
}

// TransportURI is a parsed TCP transport URI.
type TransportURI struct {
	Host string
	Port int
	Bind bool
}
