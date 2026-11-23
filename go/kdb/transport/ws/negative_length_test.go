package ws

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

// A 64-bit extended payload length with its top bit set decodes to a negative int64. That
// slipped past the "length > maxFrameBytes" guard and reached make([]byte, length), which
// panics - and readFrame runs on the connection's own goroutine with no recover, so one
// ten-byte frame from an unauthenticated peer ended the whole process.
func TestReadFrameRejectsNegativeExtendedLength(t *testing.T) {
	raw := []byte{0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 1}
	c := &wsConnection{
		reader:        bufio.NewReader(bytes.NewReader(raw)),
		maxFrameBytes: 16 << 20,
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("readFrame panicked on a negative extended length: %v", r)
		}
	}()
	if _, err := c.readFrame(); err == nil {
		t.Fatal("readFrame accepted a frame whose 64-bit length is negative")
	}
}

// An upgrade request head that never ends must be refused once it passes the cap, not buffered
// for the whole handshake timeout.
func TestReadHTTPHeadersCapsTotalSize(t *testing.T) {
	head := "GET /kdb HTTP/1.1\r\n" + strings.Repeat("X-Pad: "+strings.Repeat("a", 1000)+"\r\n", 200) + "\r\n"
	if _, _, err := readHTTPHeaders(bufio.NewReader(strings.NewReader(head))); err == nil {
		t.Fatalf("readHTTPHeaders accepted a %d-byte header block", len(head))
	}
	noNewline := strings.Repeat("a", 1<<20)
	if _, _, err := readHTTPHeaders(bufio.NewReader(strings.NewReader(noNewline))); err != errHandshakeHeadersTooLarge {
		t.Fatalf("unterminated 1 MiB request line: err = %v, want errHandshakeHeadersTooLarge", err)
	}
	ok := "GET /kdb HTTP/1.1\r\nCookie: " + strings.Repeat("c", 8000) + "\r\nUpgrade: websocket\r\n\r\n"
	line, headers, err := readHTTPHeaders(bufio.NewReader(strings.NewReader(ok)))
	if err != nil || line != "GET /kdb HTTP/1.1" || headers["upgrade"] != "websocket" || len(headers["cookie"]) != 8000 {
		t.Fatalf("ordinary head with a long cookie: line=%q err=%v upgrade=%q cookie=%d", line, err, headers["upgrade"], len(headers["cookie"]))
	}
}
