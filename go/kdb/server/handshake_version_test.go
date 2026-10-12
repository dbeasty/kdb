package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/version"
	"github.com/limidus/kdb/go/kdb/wire"
)

// The ack names the server's release so a client published for one release (@kdb/client) can
// refuse to talk to another.
func TestHandshakeAckCarriesServerVersion(t *testing.T) {
	ack := handshakeAck(wire.HandshakeMessage{}, true, "", nil)
	if ack.Response.ServerVersion == "" || ack.Response.ServerVersion != version.Get().Version {
		t.Fatalf("ServerVersion = %q, want %q", ack.Response.ServerVersion, version.Get().Version)
	}
}
