package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/wire"
)

// hostileWsServer completes a normal RFC 6455 handshake and then pushes one unsolicited binary
// message of the client's choosing - unlike fakeWsServer, which only ever echoes. That is the
// only way to exercise what the client does with a message it did not ask for and would never
// have produced itself: Send validates its own outgoing frames, so a malformed buffer cannot be
// smuggled in through the echo path.
type hostileWsServer struct {
	ln      net.Listener
	addr    string
	payload []byte
}

func startHostileWsServer(t *testing.T, payload []byte) *hostileWsServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &hostileWsServer{ln: ln, addr: ln.Addr().String(), payload: payload}
	go func() {
		for {
			conn, err := s.ln.Accept()
			if err != nil {
				return
			}
			go s.handle(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *hostileWsServer) handle(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}
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
			if strings.EqualFold(strings.TrimSpace(line[:idx]), "sec-websocket-key") {
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
	if _, err := conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n")); err != nil {
		return
	}
	_ = writeServerSideFrame(conn, 0x2, s.payload, false)
	// Hold the connection open so the client's reaction is its own doing, not a server hangup.
	time.Sleep(3 * time.Second)
}

func (s *hostileWsServer) uri(path string) string {
	_, port, _ := net.SplitHostPort(s.addr)
	p, _ := strconv.Atoi(port)
	return fmt.Sprintf("ws://127.0.0.1:%d%s", p, path)
}

// malformedWireFrame is a buffer whose 4-byte length prefix declares declaredLen while the
// buffer itself is bufLen bytes - what the stream transports can never emit (FrameStreamReader
// only releases a buffer once the declared byte count has arrived) but a WebSocket peer can
// send trivially, since a WS message is delivered whole and was, until this was fixed, never
// checked against its own prefix.
func malformedWireFrame(bufLen, declaredLen int) []byte {
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf[0:], uint32(int32(declaredLen)))
	if bufLen >= wire.FrameHeaderSize {
		binary.LittleEndian.PutUint16(buf[4:], uint16(wire.MsgHandshake))
		binary.LittleEndian.PutUint16(buf[6:], wire.KdbWireProtocolVersion)
		binary.LittleEndian.PutUint32(buf[8:], 7)
	}
	return buf
}

// The connection must be dropped rather than the buffer forwarded. Before this fix the
// mismatched frame reached wire.Codec.Decode, which sliced the payload out using the declared
// length and panicked with a slice-bounds error - and with no recover() on any frame-handling
// path, that panic ends the process, so an unauthenticated peer could kill any Go component
// that speaks WebSocket by sending twenty bytes.
func TestReadLoopRejectsFrameDeclaringMoreBytesThanItCarries(t *testing.T) {
	server := startHostileWsServer(t, malformedWireFrame(20, 1000))
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	select {
	case frame, ok := <-conn.Incoming():
		if ok {
			t.Fatalf("malformed frame was delivered to the decoder: %d bytes, prefix declares %d",
				len(frame), int32(binary.LittleEndian.Uint32(frame[:4])))
		}
		// Channel closed: readLoop rejected the frame and tore the connection down.
	case <-time.After(5 * time.Second):
		t.Fatal("timed out; expected the connection to be closed")
	}
}

// The mirror case: a prefix declaring fewer bytes than arrived is equally impossible from a
// stream transport, and equally must not be passed on - the surplus would be silently treated
// as a following frame that no sender ever framed.
func TestReadLoopRejectsFrameDeclaringFewerBytesThanItCarries(t *testing.T) {
	server := startHostileWsServer(t, malformedWireFrame(64, 20))
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	select {
	case _, ok := <-conn.Incoming():
		if ok {
			t.Fatal("frame with a short length prefix was delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out; expected the connection to be closed")
	}
}

// A message too short to even hold a length prefix must not index past its end.
func TestReadLoopRejectsFrameShorterThanLengthPrefix(t *testing.T) {
	server := startHostileWsServer(t, []byte{0x01, 0x02})
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	select {
	case _, ok := <-conn.Incoming():
		if ok {
			t.Fatal("2-byte frame was delivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out; expected the connection to be closed")
	}
}

// A well-formed frame still arrives - the new check must not reject legitimate traffic, which
// is the failure mode a "drop anything suspicious" guard would introduce.
func TestReadLoopStillDeliversWellFormedFrame(t *testing.T) {
	good := wireShapedFrame([]byte("a perfectly ordinary payload"))
	server := startHostileWsServer(t, good)
	transport := NewTransport(core.DefaultConnectOptions())
	conn, err := transport.Connect(server.uri("/kdb"))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	select {
	case frame, ok := <-conn.Incoming():
		if !ok {
			t.Fatal("connection closed on a well-formed frame")
		}
		if len(frame) != len(good) {
			t.Fatalf("got %d bytes, want %d", len(frame), len(good))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a well-formed frame")
	}
}
