package ws

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// websocketGUID is RFC 6455's fixed handshake magic value.
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Transport connects and listens over WebSocket binary frames.
type Transport interface {
	stream.Transport
	ConnectWithOptions(uri string, options core.TransportConnectOptions) (stream.ConnectionHandle, error)
	Listen(ctx context.Context, uri string, options core.TransportConnectOptions, handler func(stream.ConnectionHandle)) error
	// ListenBound binds the listening socket and returns it without serving, so a caller can
	// learn the actual address before anything is accepted - which is what makes a ":0" test
	// listener usable, and mirrors tcp.Transport's identically-named method.
	ListenBound(uri string, options core.TransportConnectOptions) (net.Listener, error)
	// Serve runs the accept loop over an already-bound listener, upgrading each connection and
	// handing the result to handler.
	Serve(ctx context.Context, ln net.Listener, options core.TransportConnectOptions, handler func(stream.ConnectionHandle)) error
}

type defaultTransport struct {
	options core.TransportConnectOptions
}

// NewTransport returns a WebSocket wire transport, both halves: ConnectWithOptions dials and
// Listen/Serve accept. The two share this file's handshake helpers deliberately, so the client
// and server sides of the upgrade cannot drift apart unnoticed.
func NewTransport(options core.TransportConnectOptions) Transport {
	if options.MaxFrameBytes == 0 {
		options = core.DefaultConnectOptions()
	}
	return &defaultTransport{options: options}
}

func (t *defaultTransport) Connect(uri string) (stream.ConnectionHandle, error) {
	return t.ConnectWithOptions(uri, t.options)
}

// ConnectWithOptions dials a raw TCP socket and performs the RFC 6455 client handshake, then
// returns a stream.ConnectionHandle that reads/writes single-frame (unfragmented) binary
// messages - the same subset kdb-transport-ws's hand-rolled JVM implementation
// (WebSocketFraming.kt / JvmRawSocketWebSocketConnection.kt) speaks: opcode 0x2 for data,
// client frames always masked (server frames are not, matching JvmNetworkWebSocketServer's own
// unmasked writes), ping answered with a masked pong, no fragmentation.
func (t *defaultTransport) ConnectWithOptions(uri string, options core.TransportConnectOptions) (stream.ConnectionHandle, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	if options.MaxFrameBytes == 0 {
		options = core.DefaultConnectOptions()
	}
	// Validated before dialing, not after: a wss:// caller with no usable TLS settings should
	// get the same clear "TLS settings required" error every time, not one that depends on
	// whether the target host happened to be reachable (see kdb/transport/tcp.Connect's
	// identical ordering, and its doc comment, for the full reasoning).
	var tlsCfg *tls.Config
	if parsed.Secure {
		tlsCfg, err = options.TLS.BuildTLSConfig(false)
		if err != nil {
			return nil, err
		}
		if tlsCfg == nil {
			return nil, fmt.Errorf("kdb: wss:// connect requires TLS settings (enabled, with at least a CA or InsecureSkipVerify) - refusing to fall back to plaintext")
		}
		if tlsCfg.ServerName == "" {
			tlsCfg.ServerName = parsed.Host
		}
	}
	addr := netJoin(parsed.Host, parsed.Port)
	rawConn, err := net.DialTimeout("tcp", addr, timeoutFromMs(options.ConnectTimeoutMs))
	if err != nil {
		return nil, err
	}
	if tc, ok := rawConn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	var handshakeConn net.Conn = rawConn
	if parsed.Secure {
		tlsConn, err := tlsClientHandshake(rawConn, tlsCfg)
		if err != nil {
			_ = rawConn.Close()
			return nil, err
		}
		handshakeConn = tlsConn
	}
	conn, err := performClientHandshake(handshakeConn, parsed.Host, parsed.Port, parsed.Path, options)
	if err != nil {
		_ = handshakeConn.Close()
		return nil, err
	}
	return conn, nil
}

// tlsClientHandshake wraps rawConn (already TCP-connected) in a TLS client handshake using cfg,
// for wss://. Explicitly calling HandshakeContext (rather than letting the first Read/Write
// trigger it lazily inside performClientHandshake) fails fast on a bad cert/CA, before any HTTP
// upgrade bytes are sent.
func tlsClientHandshake(rawConn net.Conn, cfg *tls.Config) (net.Conn, error) {
	tlsConn := tls.Client(rawConn, cfg)
	if err := tlsConn.HandshakeContext(context.Background()); err != nil {
		return nil, fmt.Errorf("tls handshake: %w", err)
	}
	return tlsConn, nil
}

// Listen binds uri and serves WebSocket connections until ctx is cancelled.
func (t *defaultTransport) Listen(ctx context.Context, uri string, options core.TransportConnectOptions, handler func(stream.ConnectionHandle)) error {
	ln, err := t.ListenBound(uri, options)
	if err != nil {
		return err
	}
	return t.Serve(ctx, ln, options, handler)
}

// ListenBound binds the listening socket for uri, wrapping it in TLS for wss://.
func (t *defaultTransport) ListenBound(uri string, options core.TransportConnectOptions) (net.Listener, error) {
	parsed, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}
	if options.MaxFrameBytes == 0 {
		options = core.DefaultConnectOptions()
	}
	ln, err := net.Listen("tcp", netJoin(parsed.Host, parsed.Port))
	if err != nil {
		return nil, err
	}
	if !parsed.Secure {
		return ln, nil
	}
	cfg, err := options.TLS.BuildTLSConfig(true)
	if err != nil {
		_ = ln.Close()
		return nil, err
	}
	if cfg == nil {
		_ = ln.Close()
		return nil, fmt.Errorf("kdb: wss:// listen requires TLS settings (enabled, with CertFile/KeyFile) - refusing to fall back to plaintext: %s", uri)
	}
	return tls.NewListener(ln, cfg), nil
}

// Serve accepts connections on ln, performs the RFC 6455 server handshake on each, and hands
// every successfully upgraded connection to handler on its own goroutine.
//
// The upgrade is done by hand rather than through net/http's Hijacker for one reason: this
// package already contains the client half of exactly the same hand-rolled handshake, and the
// JVM server it has to interoperate with (JvmNetworkWebSocketServer.acceptWebSocket) is written
// the same way. Sharing one readHTTPHeaders/websocketAccept pair between the two halves keeps
// the two sides of the handshake provably symmetric - a mismatch shows up as a failing test in
// this package rather than as a browser-only bug nothing here can reproduce.
func (t *defaultTransport) Serve(ctx context.Context, ln net.Listener, options core.TransportConnectOptions, handler func(stream.ConnectionHandle)) error {
	defer ln.Close()
	if options.MaxFrameBytes == 0 {
		options = core.DefaultConnectOptions()
	}
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
		// Same connection cap, and the same reasoning, as tcp.Serve: refusing a connection that
		// was never established costs nothing, which is the property that matters precisely when
		// the reason to refuse is that resources are already short.
		if max := options.MaxConnections; max > 0 && active.Load() >= int64(max) {
			_ = conn.Close()
			continue
		}
		active.Add(1)
		setNoDelay(conn)
		go func(raw net.Conn) {
			// onClose is passed to the constructor rather than assigned to the returned
			// connection: performServerHandshake starts readLoop before it returns, and
			// readLoop can reach Close on its own, so a later assignment races with that read.
			// Same shape, same reason, as tcp's newSocketConnection.
			released := &sync.Once{}
			release := func() { released.Do(func() { active.Add(-1) }) }
			wsConn, err := performServerHandshake(raw, options, release)
			if err != nil {
				// The handshake writes its own HTTP error response before failing, so there is
				// nothing to send here - just release the slot.
				_ = raw.Close()
				release()
				return
			}
			handler(wsConn)
		}(conn)
	}
}

// setNoDelay reaches through a *tls.Conn to the raw *net.TCPConn underneath, so a wss://
// listener's accepted connections get the same treatment as a ws:// one - tls.NewListener's
// Accept returns *tls.Conn, which is not itself a *net.TCPConn, so a naive type assertion
// silently no-ops for every TLS connection. Same fix as kdb/transport/tcp's setNoDelay.
func setNoDelay(conn net.Conn) {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
}

// handshakeTimeout bounds how long a connection may sit mid-upgrade. Without it a peer that
// opens a socket and sends nothing holds a goroutine, a connection slot and a buffer
// indefinitely - the cheapest possible way to exhaust a server that otherwise caps everything.
const handshakeTimeout = 10 * time.Second

// performServerHandshake reads the client's upgrade request and answers it.
//
// Validation is deliberately stricter than the JVM server's, which checks only the path and the
// presence of a key. Everything added here is required by RFC 6455 §4.2.1 and is free to check,
// and each rejection writes a real HTTP response rather than dropping the socket, so a
// misconfigured client sees why it was refused instead of an unexplained disconnect.
func performServerHandshake(
	conn net.Conn,
	options core.TransportConnectOptions,
	onClose func(),
) (*wsConnection, error) {
	if err := conn.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	requestLine, headers, err := readHTTPHeaders(reader)
	if err != nil {
		return nil, err
	}

	parts := strings.Fields(requestLine)
	if len(parts) < 3 || parts[0] != http.MethodGet {
		return nil, respondHandshakeError(conn, http.StatusMethodNotAllowed, "websocket upgrade requires GET")
	}

	if !headerHasToken(headers["connection"], "upgrade") ||
		!strings.EqualFold(strings.TrimSpace(headers["upgrade"]), "websocket") {
		return nil, respondHandshakeError(conn, http.StatusBadRequest, "not a websocket upgrade request")
	}
	if version := strings.TrimSpace(headers["sec-websocket-version"]); version != "13" {
		// RFC 6455 §4.2.2 asks for the supported version in the failure response, so a client
		// speaking an older draft learns what to speak rather than merely that it failed.
		return nil, respondHandshakeErrorWith(
			conn,
			http.StatusUpgradeRequired,
			"unsupported Sec-WebSocket-Version: "+version,
			"Sec-WebSocket-Version: 13\r\n",
		)
	}
	key := strings.TrimSpace(headers["sec-websocket-key"])
	if !validWebSocketKey(key) {
		return nil, respondHandshakeError(conn, http.StatusBadRequest, "missing or malformed Sec-WebSocket-Key")
	}

	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + websocketAccept(key) + "\r\n\r\n"
	if _, err := conn.Write([]byte(response)); err != nil {
		return nil, err
	}

	// The upgrade is done; the connection is now long-lived and must not inherit the handshake
	// deadline, or every established connection would die ten seconds later.
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}

	maxFrameBytes := options.MaxFrameBytes
	if maxFrameBytes == 0 {
		maxFrameBytes = core.DefaultConnectOptions().MaxFrameBytes
	}
	queue := options.IncomingQueueFrames
	if queue <= 0 {
		queue = core.DefaultIncomingQueueFrames
	}
	c := &wsConnection{
		conn:          conn,
		reader:        reader,
		maxFrameBytes: maxFrameBytes,
		incoming:      make(chan []byte, queue),
		done:          make(chan struct{}),
		// Server side, the mirror image of the client's: never mask outbound, require inbound.
		maskOutbound:       false,
		requireInboundMask: true,
		admit:              options.Admitter,
		onClose:            onClose,
	}
	go c.readLoop()
	return c, nil
}

// headerHasToken reports whether a comma-separated header value contains token. `Connection` is
// a list header, and a browser routinely sends "keep-alive, Upgrade" - an equality check against
// "Upgrade" rejects those, which is the classic way a hand-rolled server works against curl and
// fails against a real browser.
func headerHasToken(value, token string) bool {
	for _, part := range strings.Split(value, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

// validWebSocketKey checks that the key is base64 of exactly 16 bytes, per RFC 6455 §4.1.
func validWebSocketKey(key string) bool {
	if key == "" {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(key)
	return err == nil && len(decoded) == 16
}

func respondHandshakeError(conn net.Conn, status int, reason string) error {
	return respondHandshakeErrorWith(conn, status, reason, "")
}

func respondHandshakeErrorWith(conn net.Conn, status int, reason, extraHeaders string) error {
	body := reason + "\n"
	response := fmt.Sprintf(
		"HTTP/1.1 %d %s\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: %d\r\nConnection: close\r\n%s\r\n%s",
		status, http.StatusText(status), len(body), extraHeaders, body,
	)
	_, _ = conn.Write([]byte(response))
	return fmt.Errorf("websocket handshake rejected (%d): %s", status, reason)
}

func performClientHandshake(conn net.Conn, host string, port int, path string, options core.TransportConnectOptions) (*wsConnection, error) {
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	portSuffix := ""
	if port > 0 && port != 80 && port != 443 {
		portSuffix = fmt.Sprintf(":%d", port)
	}
	var req strings.Builder
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&req, "Host: %s%s\r\n", host, portSuffix)
	req.WriteString("Upgrade: websocket\r\n")
	req.WriteString("Connection: Upgrade\r\n")
	for name, value := range options.ConnectHeaders {
		fmt.Fprintf(&req, "%s: %s\r\n", name, value)
	}
	fmt.Fprintf(&req, "Sec-WebSocket-Key: %s\r\n", key)
	req.WriteString("Sec-WebSocket-Version: 13\r\n\r\n")
	if _, err := conn.Write([]byte(req.String())); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	statusLine, headers, err := readHTTPHeaders(reader)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(statusLine, "101") {
		return nil, fmt.Errorf("websocket upgrade failed: %s", statusLine)
	}
	if accept := headers["sec-websocket-accept"]; accept != websocketAccept(key) {
		return nil, fmt.Errorf("invalid Sec-WebSocket-Accept")
	}

	maxFrameBytes := options.MaxFrameBytes
	if maxFrameBytes == 0 {
		maxFrameBytes = core.DefaultConnectOptions().MaxFrameBytes
	}
	c := &wsConnection{
		conn:          conn,
		reader:        reader,
		maxFrameBytes: maxFrameBytes,
		incoming:      make(chan []byte, 32),
		done:          make(chan struct{}),
		// Client side: mask everything sent, accept the server's unmasked frames.
		maskOutbound: true,
	}
	go c.readLoop()
	return c, nil
}

func websocketAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key + websocketGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// readHTTPHeaders reads a status/request line and header block terminated by a blank line - the
// same minimal parsing WebSocketFraming.readHttpHeaders does on the Kotlin side (no continuation
// lines, no body).
func readHTTPHeaders(r *bufio.Reader) (statusLine string, headers map[string]string, err error) {
	statusLine, err = readCRLFLine(r)
	if err != nil {
		return "", nil, err
	}
	headers = make(map[string]string)
	for {
		line, err := readCRLFLine(r)
		if err != nil {
			return "", nil, err
		}
		if line == "" {
			break
		}
		idx := strings.IndexByte(line, ':')
		if idx <= 0 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(line[:idx]))
		headers[name] = strings.TrimSpace(line[idx+1:])
	}
	return statusLine, headers, nil
}

func readCRLFLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

type wsConnection struct {
	conn          net.Conn
	reader        *bufio.Reader
	maxFrameBytes int
	incoming      chan []byte
	// maskOutbound is RFC 6455 §5.1's asymmetry, and it is not cosmetic: a client MUST mask
	// every frame it sends and a server MUST NOT mask any. Browsers enforce both directions, so
	// a server that masked its replies would be closed by every browser it served.
	maskOutbound bool
	// requireInboundMask is the same rule read from the other side: a server MUST reject an
	// unmasked client frame. Only set server-side - a client accepts its peer's unmasked frames,
	// which is exactly what it should be receiving.
	requireInboundMask bool
	// admit, when set, is consulted with each inbound message's frame header before the message
	// is queued. Unlike the stream transports there is no partial-body saving to be had here (a
	// WebSocket message arrives whole), but the rejection reply still matters: a shed request
	// gets a typed answer instead of vanishing.
	admit core.FrameAdmitter
	// onClose, if set, runs exactly once when this connection closes - how Serve's connection
	// cap learns that a slot has freed.
	onClose func()
	// done is closed exactly once, by Close, to unblock readLoop if it's currently blocked
	// trying to deliver a frame on incoming (see readLoop's doc comment - same fix, same reason,
	// as kdb/transport/tcp's socketConnection).
	done    chan struct{}
	closed  bool
	mu      sync.Mutex
	writeMu sync.Mutex
}

// readLoop is incoming's only sender and its only closer - see kdb/transport/tcp's
// socketConnection.readLoop doc comment for the full reasoning (kdb-finish-up-plan.md's 1-G12/
// 1-G13): a non-blocking `select ... default` send here silently dropped frames whenever a
// consumer fell behind the 32-slot buffer, and Close closing incoming directly while this
// goroutine could still be sending on it was a "send on closed channel" panic race. Blocking
// on the send (interruptible via done) fixes both.
func (c *wsConnection) readLoop() {
	defer close(c.incoming)
	for {
		payload, err := c.readFrame()
		if err != nil {
			_ = c.Close()
			return
		}
		// A WebSocket message arrives whole, so unlike the stream transports nothing has yet
		// checked that this buffer is one complete kdb frame. Handing a buffer whose length
		// prefix disagrees with its size to the decoder is how a peer gets it to slice past the
		// end of what it was given; drop the connection instead of forwarding it.
		if err := core.ValidateInboundFrame(payload, c.maxFrameBytes); err != nil {
			_ = c.Close()
			return
		}
		// Load shedding, server-side. The frame is already fully read - a WebSocket message
		// arrives whole, so there is no body-read to save the way the stream reader saves one -
		// but the answer still is: a shed request gets a typed rejection back rather than
		// silently vanishing, and the connection stays usable, since a message boundary is
		// necessarily a frame boundary here.
		if c.admit != nil {
			header, headerErr := wire.DecodeHeader(payload)
			if headerErr == nil {
				rejection, admitErr := c.admit(header)
				if admitErr != nil {
					if len(rejection) > 0 {
						_ = c.writeFrame(0x2, rejection, c.maskOutbound)
					}
					continue
				}
			}
		}
		select {
		case c.incoming <- payload:
		case <-c.done:
			return
		}
	}
}

// readFrame reads one WebSocket frame, transparently answering pings and skipping pongs, until
// it sees a binary (0x2) data frame, a close frame, or the connection ends. No fragmentation
// support (FIN is not inspected) - matches the same subset the JVM server/client speak.
func (c *wsConnection) readFrame() ([]byte, error) {
	for {
		b0, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		opcode := b0 & 0x0F
		if opcode == 0x8 {
			return nil, io.EOF
		}
		b1, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		masked := b1&0x80 != 0
		// RFC 6455 §5.1: "The server MUST close the connection upon receiving a frame that is
		// not masked." Every real client masks - browsers, this repo's Go client, and the JVM
		// client (WebSocketFraming.writeBinaryFrame with maskOutbound = true) - so enforcing it
		// costs no interoperability, and not enforcing it would let an unmasked frame through
		// to be XORed against a key that was never read.
		if c.requireInboundMask && !masked {
			return nil, fmt.Errorf("websocket: client frame is not masked")
		}
		length := int64(b1 & 0x7F)
		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.reader, ext[:]); err != nil {
				return nil, err
			}
			length = int64(ext[0])<<8 | int64(ext[1])
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.reader, ext[:]); err != nil {
				return nil, err
			}
			length = 0
			for _, b := range ext {
				length = (length << 8) | int64(b)
			}
		}
		if c.maxFrameBytes > 0 && length > int64(c.maxFrameBytes) {
			return nil, fmt.Errorf("websocket frame too large: %d bytes", length)
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
				return nil, err
			}
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(c.reader, data); err != nil {
			return nil, err
		}
		if masked {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}
		switch opcode {
		case 0x2:
			return data, nil
		case 0x9: // ping
			if err := c.writeFrame(0xA, data, c.maskOutbound); err != nil {
				return nil, err
			}
		case 0xA: // pong
			// no-op, keep reading
		default:
			return nil, fmt.Errorf("unsupported WebSocket opcode %d", opcode)
		}
	}
}

func (c *wsConnection) Send(frame []byte) error {
	if err := core.ValidateOutgoingFrame(frame, c.maxFrameBytes); err != nil {
		return err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return stream.NewNotConnectedError()
	}
	return c.writeFrame(0x2, frame, c.maskOutbound)
}

// writeFrame writes one unfragmented frame. mask must be true for every client->server frame
// per RFC 6455's client-masking requirement - a real (non-hand-rolled) WebSocket server would
// reject an unmasked client frame, even though this repo's own JVM server tolerates either.
func (c *wsConnection) writeFrame(opcode byte, payload []byte, mask bool) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	buf := append([]byte{0x80 | opcode}, encodeLength(len(payload), mask)...)
	if _, err := c.conn.Write(buf); err != nil {
		return err
	}
	if !mask {
		_, err := c.conn.Write(payload)
		return err
	}
	var maskKey [4]byte
	if _, err := rand.Read(maskKey[:]); err != nil {
		return err
	}
	if _, err := c.conn.Write(maskKey[:]); err != nil {
		return err
	}
	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ maskKey[i%4]
	}
	_, err := c.conn.Write(masked)
	return err
}

func encodeLength(size int, masked bool) []byte {
	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch {
	case size < 126:
		return []byte{maskBit | byte(size)}
	case size < 65536:
		return []byte{maskBit | 126, byte(size >> 8), byte(size)}
	default:
		b := make([]byte, 9)
		b[0] = maskBit | 127
		for i := 0; i < 8; i++ {
			b[8-i] = byte(size >> (8 * i))
		}
		return b
	}
}

func (c *wsConnection) Incoming() <-chan []byte { return c.incoming }

func (c *wsConnection) TryPoll() []byte {
	select {
	case frame := <-c.incoming:
		return frame
	default:
		return nil
	}
}

func (c *wsConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	close(c.done)
	if c.onClose != nil {
		c.onClose()
	}
	return c.conn.Close()
}

func timeoutFromMs(ms int64) time.Duration {
	if ms <= 0 {
		return 10 * time.Second
	}
	return time.Duration(ms) * time.Millisecond
}

// ParseURI parses ws:// or wss:// transport URIs.
func ParseURI(raw string) (TransportURI, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return TransportURI{}, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return TransportURI{}, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		host = "127.0.0.1"
	}
	port := 8080
	if u.Scheme == "wss" {
		port = 443
	}
	if p := u.Port(); p != "" {
		port, err = strconv.Atoi(p)
		if err != nil {
			return TransportURI{}, err
		}
	}
	path := u.Path
	if path == "" {
		path = "/kdb"
	}
	return TransportURI{
		Host:   host,
		Port:   port,
		Path:   path,
		Secure: u.Scheme == "wss",
	}, nil
}

// TransportURI is a parsed WebSocket transport URI.
type TransportURI struct {
	Host   string
	Port   int
	Path   string
	Secure bool
}

func netJoin(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}
