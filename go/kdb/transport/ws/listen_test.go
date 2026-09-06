package ws

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// serveEcho starts a listener on an ephemeral port and echoes every frame back. It returns the
// ws:// URI to dial and a stop function.
func serveEcho(t *testing.T, options core.TransportConnectOptions) (string, func()) {
	t.Helper()
	return serveWith(t, options, func(conn stream.ConnectionHandle) {
		for frame := range conn.Incoming() {
			if err := conn.Send(frame); err != nil {
				return
			}
		}
	})
}

func serveWith(
	t *testing.T,
	options core.TransportConnectOptions,
	handler func(stream.ConnectionHandle),
) (string, func()) {
	t.Helper()
	transport := NewTransport(options)
	ln, err := transport.ListenBound("ws://127.0.0.1:0/kdb", options)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = transport.Serve(ctx, ln, options, handler)
	}()
	uri := fmt.Sprintf("ws://%s/kdb", ln.Addr().String())
	return uri, func() {
		cancel()
		<-done
	}
}

// rawUpgrade dials uri and completes the RFC 6455 handshake by hand, returning the raw socket -
// so a test can write frames the real client would never produce (unmasked, oversize).
func rawUpgrade(t *testing.T, uri string) net.Conn {
	t.Helper()
	addr := strings.TrimSuffix(strings.TrimPrefix(uri, "ws://"), "/kdb")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(key)
	request := "GET /kdb HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n" +
		"Connection: Upgrade\r\nSec-WebSocket-Key: " + encoded + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("write handshake: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	statusLine, headers, err := readHTTPHeaders(bufio.NewReader(conn))
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	if !strings.Contains(statusLine, "101") {
		t.Fatalf("upgrade failed: %s", statusLine)
	}
	if got := headers["sec-websocket-accept"]; got != websocketAccept(encoded) {
		t.Fatalf("wrong Sec-WebSocket-Accept: %q", got)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn
}

func testFrame(t *testing.T, correlation int, sql string) []byte {
	t.Helper()
	codec := wire.NewCodec(wire.EncodingJSON)
	frame, err := codec.Encode(wire.SqlExecMessage{
		H: wire.Header{
			MessageType:     wire.MsgSqlExec,
			ProtocolVersion: wire.KdbWireProtocolVersion,
			CorrelationID:   correlation,
		},
		Namespace: "app/users",
		SessionID: "sess-1",
		SQL:       sql,
	})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return frame
}

// TestServeRoundTrip is the test the 501 stub made impossible: this package's own client
// talking to this package's own server.
func TestServeRoundTrip(t *testing.T) {
	options := core.DefaultConnectOptions()
	uri, stop := serveEcho(t, options)
	defer stop()

	transport := NewTransport(options)
	conn, err := transport.Connect(uri)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	sent := testFrame(t, 7, "SELECT 1")
	if err := conn.Send(sent); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case got := <-conn.Incoming():
		if string(got) != string(sent) {
			t.Fatalf("frame round trip mismatch:\n sent %x\n  got %x", sent, got)
		}
		header, err := wire.DecodeHeader(got)
		if err != nil {
			t.Fatalf("decode header: %v", err)
		}
		if header.CorrelationID != 7 || header.MessageType != wire.MsgSqlExec {
			t.Fatalf("unexpected header: %+v", header)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the echoed frame")
	}
}

// TestServeRoundTripLargeFrame exercises both extended length encodings (126 -> 16-bit,
// 127 -> 64-bit), which a small-frame-only test would never reach.
func TestServeRoundTripLargeFrame(t *testing.T) {
	options := core.DefaultConnectOptions()
	uri, stop := serveEcho(t, options)
	defer stop()

	conn, err := NewTransport(options).Connect(uri)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	for _, size := range []int{200, 70000} {
		sent := testFrame(t, 1, strings.Repeat("x", size))
		if err := conn.Send(sent); err != nil {
			t.Fatalf("send %d: %v", size, err)
		}
		select {
		case got := <-conn.Incoming():
			if string(got) != string(sent) {
				t.Fatalf("size %d: round trip mismatch (%d bytes back, %d out)", size, len(got), len(sent))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("size %d: timed out", size)
		}
	}
}

func TestServeConcurrentConnections(t *testing.T) {
	options := core.DefaultConnectOptions()
	uri, stop := serveEcho(t, options)
	defer stop()

	const clients = 8
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			conn, err := NewTransport(options).Connect(uri)
			if err != nil {
				errs <- fmt.Errorf("client %d connect: %w", n, err)
				return
			}
			defer conn.Close()
			sent := testFrame(t, n+1, fmt.Sprintf("SELECT %d", n))
			if err := conn.Send(sent); err != nil {
				errs <- fmt.Errorf("client %d send: %w", n, err)
				return
			}
			select {
			case got := <-conn.Incoming():
				if string(got) != string(sent) {
					errs <- fmt.Errorf("client %d: frame mismatch", n)
				}
			case <-time.After(5 * time.Second):
				errs <- fmt.Errorf("client %d: timed out", n)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestServeRejectsUnmaskedClientFrame covers RFC 6455 §5.1's requirement that a server close on
// an unmasked client frame. Every real client masks, so this needs a hand-built frame.
func TestServeRejectsUnmaskedClientFrame(t *testing.T) {
	options := core.DefaultConnectOptions()
	uri, stop := serveEcho(t, options)
	defer stop()

	conn := rawUpgrade(t, uri)
	defer conn.Close()

	payload := testFrame(t, 1, "SELECT 1")
	header := append([]byte{0x80 | 0x2}, encodeLength(len(payload), false)...)
	if _, err := conn.Write(append(header, payload...)); err != nil {
		t.Fatalf("write unmasked frame: %v", err)
	}

	// The server must drop the connection rather than unmask against a key it never read.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the server to close the connection on an unmasked client frame")
	}
}

func TestServeRejectsOversizeFrame(t *testing.T) {
	options := core.DefaultConnectOptions()
	options.MaxFrameBytes = 1024
	uri, stop := serveEcho(t, options)
	defer stop()

	conn := rawUpgrade(t, uri)
	defer conn.Close()

	// Only the length header is written: the server must refuse on the declared length alone,
	// without waiting to read a payload that may never arrive.
	header := append([]byte{0x80 | 0x2}, encodeLength(1<<20, true)...)
	if _, err := conn.Write(append(header, 0, 0, 0, 0)); err != nil {
		t.Fatalf("write oversize header: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the server to close the connection on an oversize frame")
	}
}

// TestServeShedsFrameViaAdmitter checks the load-shedding path: the frame is refused, the
// typed rejection comes back, and - the part worth having a test for - the connection stays
// usable afterward instead of being torn down.
func TestServeShedsFrameViaAdmitter(t *testing.T) {
	codec := wire.NewCodec(wire.EncodingJSON)
	busy := "server busy"
	code := wire.ErrorCodeBusy
	retry := 25

	var shed int
	options := core.DefaultConnectOptions()
	options.Admitter = func(header wire.Header) ([]byte, error) {
		// Shed only the first frame, so the follow-up proves the connection survived.
		if shed > 0 {
			return nil, nil
		}
		shed++
		rejection, err := codec.Encode(wire.SqlResultMessage{
			H: wire.Header{
				MessageType:     wire.MsgSqlResult,
				ProtocolVersion: wire.KdbWireProtocolVersion,
				CorrelationID:   header.CorrelationID,
			},
			Namespace: "app/users", SessionID: "sess-1",
			Error: &busy, ErrorCode: &code, RetryAfterMs: &retry,
		})
		if err != nil {
			return nil, err
		}
		return rejection, fmt.Errorf("shed")
	}

	uri, stop := serveEcho(t, options)
	defer stop()

	conn, err := NewTransport(core.DefaultConnectOptions()).Connect(uri)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	if err := conn.Send(testFrame(t, 11, "SELECT 1")); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case got := <-conn.Incoming():
		msg, err := codec.Decode(got)
		if err != nil {
			t.Fatalf("decode rejection: %v", err)
		}
		result, ok := msg.(wire.SqlResultMessage)
		if !ok {
			t.Fatalf("expected SqlResult rejection, got %T", msg)
		}
		if result.ErrorCode == nil || *result.ErrorCode != wire.ErrorCodeBusy {
			t.Fatalf("expected BUSY, got %+v", result.ErrorCode)
		}
		if result.H.CorrelationID != 11 {
			t.Fatalf("rejection lost the correlation id: %d", result.H.CorrelationID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the rejection frame")
	}

	// The shed frame was never delivered to the handler, and the connection still works.
	second := testFrame(t, 12, "SELECT 2")
	if err := conn.Send(second); err != nil {
		t.Fatalf("send after shed: %v", err)
	}
	select {
	case got := <-conn.Incoming():
		if string(got) != string(second) {
			t.Fatal("connection unusable after a shed frame")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out after a shed frame - the connection should have stayed usable")
	}
}

func TestServeRespectsMaxConnections(t *testing.T) {
	options := core.DefaultConnectOptions()
	options.MaxConnections = 1

	accepted := make(chan struct{}, 4)
	release := make(chan struct{})
	uri, stop := serveWith(t, options, func(conn stream.ConnectionHandle) {
		accepted <- struct{}{}
		<-release
		_ = conn.Close()
	})
	defer func() {
		close(release)
		stop()
	}()

	first, err := NewTransport(options).Connect(uri)
	if err != nil {
		t.Fatalf("first connect: %v", err)
	}
	defer first.Close()
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("first connection was never handled")
	}

	// The cap is enforced at accept time, so the second connection is closed before any
	// handshake: the dial may succeed, but the upgrade cannot.
	second, err := NewTransport(options).Connect(uri)
	if err == nil {
		second.Close()
		t.Fatal("expected the second connection to be refused past MaxConnections")
	}
}

func TestHandshakeRejections(t *testing.T) {
	options := core.DefaultConnectOptions()
	uri, stop := serveEcho(t, options)
	defer stop()
	addr := strings.TrimSuffix(strings.TrimPrefix(uri, "ws://"), "/kdb")

	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	validKey := base64.StdEncoding.EncodeToString(key)

	cases := []struct {
		name    string
		request string
		status  string
	}{
		{
			name: "not a GET",
			request: "POST /kdb HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n" +
				"Connection: Upgrade\r\nSec-WebSocket-Key: " + validKey + "\r\n" +
				"Sec-WebSocket-Version: 13\r\n\r\n",
			status: "405",
		},
		{
			name:    "no upgrade header",
			request: "GET /kdb HTTP/1.1\r\nHost: x\r\n\r\n",
			status:  "400",
		},
		{
			name: "wrong websocket version",
			request: "GET /kdb HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n" +
				"Connection: Upgrade\r\nSec-WebSocket-Key: " + validKey + "\r\n" +
				"Sec-WebSocket-Version: 8\r\n\r\n",
			status: "426",
		},
		{
			name: "missing key",
			request: "GET /kdb HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n" +
				"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n",
			status: "400",
		},
		{
			name: "key is not 16 bytes of base64",
			request: "GET /kdb HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n" +
				"Connection: Upgrade\r\nSec-WebSocket-Key: dG9vLXNob3J0\r\n" +
				"Sec-WebSocket-Version: 13\r\n\r\n",
			status: "400",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			if _, err := conn.Write([]byte(tc.request)); err != nil {
				t.Fatalf("write: %v", err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			// A rejection is an HTTP response, not a dropped socket: a misconfigured client
			// should be able to see why it was refused.
			if !strings.Contains(line, tc.status) {
				t.Fatalf("expected status %s, got %q", tc.status, strings.TrimSpace(line))
			}
		})
	}
}

// TestHandshakeAcceptsBrowserStyleConnectionHeader guards the specific mistake a hand-rolled
// server makes: `Connection` is a list header and a browser routinely sends "keep-alive,
// Upgrade", so an equality check against "Upgrade" works with curl and fails with every browser.
func TestHandshakeAcceptsBrowserStyleConnectionHeader(t *testing.T) {
	options := core.DefaultConnectOptions()
	uri, stop := serveEcho(t, options)
	defer stop()
	addr := strings.TrimSuffix(strings.TrimPrefix(uri, "ws://"), "/kdb")

	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("rand: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(key)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	request := "GET /kdb HTTP/1.1\r\nHost: x\r\nUpgrade: WebSocket\r\n" +
		"Connection: keep-alive, Upgrade\r\nSec-WebSocket-Key: " + encoded + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	statusLine, headers, err := readHTTPHeaders(reader)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	if !strings.Contains(statusLine, "101") {
		t.Fatalf("expected 101, got %q", statusLine)
	}
	if got := headers["sec-websocket-accept"]; got != websocketAccept(encoded) {
		t.Fatalf("wrong Sec-WebSocket-Accept: %q", got)
	}
}

// TestHandshakeTimeout covers the cheapest denial-of-service against a server that otherwise
// caps everything: open a socket, send nothing, hold a goroutine and a connection slot forever.
func TestHandshakeTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the handshake deadline")
	}
	options := core.DefaultConnectOptions()
	uri, stop := serveEcho(t, options)
	defer stop()
	addr := strings.TrimSuffix(strings.TrimPrefix(uri, "ws://"), "/kdb")

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Never send the request line. The read must end at the handshake deadline rather than
	// blocking until the test's own timeout.
	_ = conn.SetReadDeadline(time.Now().Add(handshakeTimeout + 5*time.Second))
	start := time.Now()
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the server to close an idle half-open handshake")
	}
	if elapsed := time.Since(start); elapsed > handshakeTimeout+3*time.Second {
		t.Fatalf("handshake deadline did not fire promptly: waited %s", elapsed)
	}
}

func TestListenBoundRejectsNonWebSocketURI(t *testing.T) {
	options := core.DefaultConnectOptions()
	if _, err := NewTransport(options).ListenBound("tcp://127.0.0.1:0", options); err == nil {
		t.Fatal("expected a tcp:// listen URI to be refused by the WebSocket transport")
	}
}

func TestListenBoundRefusesWssWithoutTLSSettings(t *testing.T) {
	options := core.DefaultConnectOptions()
	_, err := NewTransport(options).ListenBound("wss://127.0.0.1:0/kdb", options)
	if err == nil {
		t.Fatal("expected wss:// listen without TLS settings to be refused")
	}
	// Silently serving plaintext on a wss:// URI is the failure mode worth naming explicitly.
	if !strings.Contains(err.Error(), "refusing to fall back to plaintext") {
		t.Fatalf("unexpected error: %v", err)
	}
}
