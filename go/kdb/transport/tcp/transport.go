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
	parsed, err := ParseURI(uri)
	if err != nil {
		return err
	}
	if !parsed.Bind {
		return fmt.Errorf("listen URI requires bind=true: %s", uri)
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(parsed.Host, strconv.Itoa(parsed.Port)))
	if err != nil {
		return err
	}
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
	closed        bool
	mu            sync.Mutex
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
	}
	go c.readLoop()
	return c
}

func (c *socketConnection) readLoop() {
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
				default:
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
	close(c.incoming)
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
