package tcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
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
	// Validated before dialing, not after: a tcps:// caller with no usable TLS settings should
	// get the same clear "TLS settings required" error every time, not one that depends on
	// whether the target host happened to be reachable - dialing first, then finding out TLS
	// was never actually configured, means the error message an operator sees is whichever of
	// the two failures the network happened to hit first.
	var tlsCfg *tls.Config
	if parsed.Secure {
		tlsCfg, err = t.options.TLS.BuildTLSConfig(false)
		if err != nil {
			return nil, err
		}
		if tlsCfg == nil {
			return nil, fmt.Errorf("kdb: tcps:// connect requires TLS settings (enabled, with at least a CA or InsecureSkipVerify) - refusing to fall back to plaintext")
		}
		if tlsCfg.ServerName == "" {
			tlsCfg.ServerName = parsed.Host
		}
	}
	dialer := net.Dialer{Timeout: timeoutFromMs(t.options.ConnectTimeoutMs)}
	conn, err := dialer.Dial("tcp", net.JoinHostPort(parsed.Host, strconv.Itoa(parsed.Port)))
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if parsed.Secure {
		tlsConn, err := tlsClientHandshake(conn, tlsCfg)
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = tlsConn
	}
	return newSocketConnection(conn, t.options, nil), nil
}

// tlsClientHandshake wraps conn (already TCP-connected) in a TLS client handshake using cfg -
// conn is dialed raw first, not via tls.DialWithDialer, so SetNoDelay above still applies to the
// underlying socket before the handshake starts. Explicitly calling HandshakeContext (rather
// than letting the first Read/Write trigger it lazily) fails fast: a Connect call should report
// a bad cert/CA immediately, not on the first unrelated read.
func tlsClientHandshake(conn net.Conn, cfg *tls.Config) (net.Conn, error) {
	tlsConn := tls.Client(conn, cfg)
	if err := tlsConn.HandshakeContext(context.Background()); err != nil {
		return nil, fmt.Errorf("tls handshake: %w", err)
	}
	return tlsConn, nil
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
	ln, err := net.Listen("tcp", net.JoinHostPort(parsed.Host, strconv.Itoa(parsed.Port)))
	if err != nil {
		return nil, err
	}
	if !parsed.Secure {
		return ln, nil
	}
	cfg, err := t.options.TLS.BuildTLSConfig(true)
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	if cfg == nil {
		_ = ln.Close()
		return nil, fmt.Errorf("kdb: tcps:// listen requires TLS settings (enabled, with CertFile/KeyFile) - refusing to fall back to plaintext: %s", uri)
	}
	return tls.NewListener(ln, cfg), nil
}

func (t *defaultTransport) Serve(ctx context.Context, ln net.Listener, handler func(stream.ConnectionHandle)) error {
	defer ln.Close()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	var active atomic.Int64
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
		// kdb-spec-layer13 Component 49 §6.5: cap concurrent connections and refuse past the cap
		// at accept time. A goroutine-per-connection model with no cap is a memory commitment
		// nothing accounts for - each accepted connection costs a goroutine stack and a frame
		// buffer regardless of whether it ever sends anything, so an unbounded accept loop can
		// exhaust the budget without a single request being admitted.
		//
		// Closing immediately, rather than accepting and then failing the request, is the point:
		// a connection that is never established costs nothing to refuse, which is the property
		// that matters when the reason for refusing is that resources are already short.
		if max := t.options.MaxConnections; max > 0 && active.Load() >= int64(max) {
			_ = conn.Close()
			continue
		}
		active.Add(1)
		setNoDelay(conn)
		// onClose is passed to the constructor rather than assigned afterward: the constructor
		// starts readLoop, which can reach Close on its own, so a later assignment would race
		// with that read.
		go handler(newSocketConnection(conn, t.options, func() { active.Add(-1) }))
	}
}

// setNoDelay reaches through a *tls.Conn (via NetConn, Go 1.21+) to the raw *net.TCPConn
// underneath when conn is TLS-wrapped, so a TLS listener's accepted connections get the same
// TCP_NODELAY treatment as a plaintext one - tls.NewListener's Accept returns *tls.Conn, which
// isn't itself a *net.TCPConn, so the naive type assertion silently no-ops for every TLS
// connection otherwise.
func setNoDelay(conn net.Conn) {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
}

type socketConnection struct {
	conn          net.Conn
	maxFrameBytes int
	reader        *core.FrameStreamReader
	incoming      chan []byte
	// onClose, if set, runs exactly once when this connection closes - how Serve's connection
	// cap learns that a slot has freed.
	onClose func()
	// done is closed exactly once, by Close, to unblock readLoop if it's currently blocked
	// trying to deliver a frame on incoming (see readLoop's doc comment).
	done   chan struct{}
	closed bool
	mu     sync.Mutex
}

func newSocketConnection(conn net.Conn, options core.TransportConnectOptions, onClose func()) *socketConnection {
	max := options.MaxFrameBytes
	if max == 0 {
		max = core.DefaultConnectOptions().MaxFrameBytes
	}
	queue := options.IncomingQueueFrames
	if queue <= 0 {
		queue = core.DefaultIncomingQueueFrames
	}
	c := &socketConnection{
		conn:          conn,
		onClose:       onClose,
		maxFrameBytes: max,
		reader:        core.NewAdmittingFrameStreamReader(max, options.Admitter),
		incoming:      make(chan []byte, queue),
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
			// Rejections are written before the admitted frames are dispatched, and directly
			// from this loop rather than through the incoming queue: the whole value of shedding
			// at the header is that a refusal does not have to queue behind the very work the
			// server has already decided it cannot do.
			for _, rejection := range c.reader.TakeRejections() {
				if sendErr := c.Send(rejection); sendErr != nil {
					_ = c.Close()
					return
				}
			}
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
	if c.onClose != nil {
		c.onClose()
		c.onClose = nil // Close is idempotent above, but never let the cap be decremented twice
	}
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

// ParseURI parses tcp://host:port?bind=true (or tcps://... for TLS) wire transport URIs. The
// scheme is the sole, authoritative source of whether a connection is secured - mirroring
// kdb/transport/ws's ws://+wss:// split - so a caller can never end up silently downgraded to
// plaintext by a misconfigured or ignored options field: a tcps:// URI without usable TLS
// settings is a hard connect/listen error (see Transport.Connect/ListenBound), never a silent
// fallback.
func ParseURI(raw string) (TransportURI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return TransportURI{}, err
	}
	secure := u.Scheme == "tcps" || u.Scheme == "kdb+tcps"
	if u.Scheme != "tcp" && u.Scheme != "kdb+tcp" && !secure {
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
	return TransportURI{Host: host, Port: port, Bind: bind, Secure: secure}, nil
}

// TransportURI is a parsed TCP transport URI.
type TransportURI struct {
	Host   string
	Port   int
	Bind   bool
	Secure bool
}
