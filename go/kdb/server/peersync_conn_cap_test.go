package server

import (
	"io"
	"net"
	"testing"
	"time"
)

// TestPeerSyncListenerCapsConnections: the peer-sync listener refuses connections past its cap
// at accept time, as the SQL and WebSocket listeners do. It used to accept without limit - each
// session a goroutine, a frame buffer and whatever sync state it built, none of it admitted.
func TestPeerSyncListenerCapsConnections(t *testing.T) {
	rt := newTestRuntime(t)
	rt.PeerSyncMaxConnections = 2
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", rt, rt.Runtime.DefaultNamespace)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	var kept []net.Conn
	defer func() {
		for _, c := range kept {
			_ = c.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		kept = append(kept, c)
	}
	// Let the accept loop register both before the third arrives.
	time.Sleep(100 * time.Millisecond)
	refused, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer refused.Close()
	if !closedByPeer(refused) {
		t.Fatal("a connection past the cap was accepted")
	}
	for i, c := range kept {
		if closedByPeerWithin(c, 200*time.Millisecond) {
			t.Fatalf("connection %d within the cap was closed", i)
		}
	}

	// A slot frees when a session ends.
	_ = kept[0].Close()
	kept = kept[1:]
	time.Sleep(100 * time.Millisecond)
	again, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if closedByPeerWithin(again, 300*time.Millisecond) {
		t.Fatal("a connection after a slot freed was refused")
	}
}

func TestPeerSyncMaxConnectionsSetting(t *testing.T) {
	for setting, want := range map[int]int{0: DefaultPeerSyncMaxConnections, -1: 0, 5: 5} {
		if got := peerSyncMaxConnections(setting); got != want {
			t.Errorf("setting %d: cap %d, want %d", setting, got, want)
		}
	}
}

func closedByPeer(c net.Conn) bool { return closedByPeerWithin(c, 2*time.Second) }

// closedByPeerWithin reports whether the server hung up on c within d. A read that times out
// means the connection is open and idle.
func closedByPeerWithin(c net.Conn, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	_, err := c.Read(make([]byte, 1))
	return err == io.EOF
}
