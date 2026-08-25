package ws

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/transport/core"
)

func TestParseURIDefaults(t *testing.T) {
	got, err := ParseURI("ws://example.com/kdb")
	if err != nil {
		t.Fatal(err)
	}
	want := TransportURI{Host: "example.com", Port: 8080, Path: "/kdb", Secure: false}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestParseURIRejectsUnknownScheme(t *testing.T) {
	if _, err := ParseURI("http://example.com/kdb"); err == nil {
		t.Fatal("expected an error for a non-ws(s) scheme")
	}
}

// fakeWsServer is an independent, hand-rolled RFC 6455 server (deliberately not sharing any code
// with transport.go's client implementation) - it exists to prove the Go client is wire-compatible
// with a real WebSocket peer, the same way kdb-transport-ws's JVM server would see it, without
// needing a JVM process running. It performs the handshake, then echoes every binary frame it
// receives back unmasked (matching JvmNetworkWebSocketServer's own unmasked-write behavior).
type fakeWsServer struct {
	ln   net.Listener
	addr string
}

func startFakeWsServer(t *testing.T) *fakeWsServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeWsServer{ln: ln, addr: ln.Addr().String()}
	go s.acceptLoop(t)
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *fakeWsServer) acceptLoop(t *testing.T) {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handle(t, conn)
	}
}

func (s *fakeWsServer) handle(t *testing.T, conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	// Read request line + headers (minimal parsing, same shape the JVM server does).
	requestLine, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	_ = requestLine
	var key string
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if idx := strings.IndexByte(line, ':'); idx > 0 {
			name := strings.ToLower(strings.TrimSpace(line[:idx]))
			if name == "sec-websocket-key" {
				key = strings.TrimSpace(line[idx+1:])
			}
		}
	}
	if key == "" {
		return
	}
	h := sha1.New()
	h.Write([]byte(key + websocketGUID))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(response)); err != nil {
		return
	}

	for {
		payload, opcode, err := readServerSideFrame(reader)
		if err != nil {
			return
		}
		switch opcode {
		case 0x8:
			return
		case 0x2:
			// Echo back unmasked, exactly like JvmNetworkWebSocketServer's own writes.
			if err := writeServerSideFrame(conn, 0x2, payload, false); err != nil {
				return
			}
		case 0x9:
			if err := writeServerSideFrame(conn, 0xA, payload, false); err != nil {
				return
			}
		}
	}
}

// readServerSideFrame is intentionally a separate, from-scratch implementation of RFC 6455
// framing (server-side: expects masked client frames) - independent of transport.go's client-side
// readFrame, so a bug shared between "how the client writes" and "how the client reads" can't
// silently cancel out and still pass.
func readServerSideFrame(r *bufio.Reader) ([]byte, byte, error) {
	b0, err := r.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	opcode := b0 & 0x0F
	b1, err := r.ReadByte()
	if err != nil {
		return nil, 0, err
	}
	masked := b1&0x80 != 0
	length := int64(b1 & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return nil, 0, err
		}
		length = int64(ext[0])<<8 | int64(ext[1])
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return nil, 0, err
		}
		length = 0
		for _, b := range ext {
			length = (length << 8) | int64(b)
		}
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return nil, 0, err
		}
	} else if opcode == 0x2 {
		return nil, 0, fmt.Errorf("client frame must be masked per RFC 6455")
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, 0, err
	}
	if masked {
		for i := range data {
			data[i] ^= mask[i%4]
		}
	}
	return data, opcode, nil
}

func writeServerSideFrame(w io.Writer, opcode byte, payload []byte, mask bool) error {
	buf := []byte{0x80 | opcode}
	size := len(payload)
	switch {
	case size < 126:
		buf = append(buf, byte(size))
	case size < 65536:
		buf = append(buf, 126, byte(size>>8), byte(size))
	default:
		ext := make([]byte, 9)
		ext[0] = 127
		for i := 0; i < 8; i++ {
			ext[8-i] = byte(size >> (8 * i))
		}
		buf = append(buf, ext...)
	}
	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func (s *fakeWsServer) uri(path string) string {
	_, port, _ := net.SplitHostPort(s.addr)
	p, _ := strconv.Atoi(port)
	return fmt.Sprintf("ws://127.0.0.1:%d%s", p, path)
}

// wireShapedFrame prepends the 4-byte little-endian total-length prefix Send's own
// core.ValidateOutgoingFrame requires - matching what wire.Codec.Encode always produces (see
// DefaultWireCodec.encodeFrameOnly), which is the only kind of buffer real callers ever pass to
// Send. A bare arbitrary byte slice without this prefix is not a valid input to Send.
func wireShapedFrame(payload []byte) []byte {
	total := 4 + len(payload)
	out := make([]byte, total)
	out[0] = byte(total)
	out[1] = byte(total >> 8)
	out[2] = byte(total >> 16)
	out[3] = byte(total >> 24)
	copy(out[4:], payload)
	return out
}

func TestConnectAndEchoRoundTrip(t *testing.T) {
	server := startFakeWsServer(t)
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	payload := wireShapedFrame([]byte("hello over websocket"))
	if err := conn.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case got := <-conn.Incoming():
		if !bytes.Equal(got, payload) {
			t.Fatalf("got %q, want %q", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for echo")
	}
}

func TestConnectAndEchoLargePayload(t *testing.T) {
	server := startFakeWsServer(t)
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	// > 65536 bytes forces the 64-bit extended length path on both write and read.
	payload := wireShapedFrame(bytes.Repeat([]byte("kdb-ws-large-payload-"), 5000))
	if err := conn.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case got := <-conn.Incoming():
		if !bytes.Equal(got, payload) {
			t.Fatalf("large payload mismatch: got %d bytes, want %d bytes", len(got), len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for large echo")
	}
}

func TestConnectFailsForNonWebSocketPeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
	}()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	transport := NewTransport(core.DefaultConnectOptions())
	_, err = transport.Connect("ws://127.0.0.1:" + port + "/kdb")
	if err == nil {
		t.Fatal("expected an error connecting to a peer that refuses the upgrade")
	}
}
