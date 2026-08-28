package server

import (
	"fmt"
	"strings"
	"testing"
)

// TestSessionBeginRejectionCarriesError covers Phase 2.7's explicit auth-failure frame: a
// rejected SessionBegin used to come back as an ack with an empty SessionID and nothing else -
// the client had no way to distinguish "no such grant" from any other refusal. The ack now
// carries an explicit Error string, in both the unauthorized and the unauthenticated case.
func TestSessionBeginRejectionCarriesError(t *testing.T) {
	rt := newTestRuntime(t)
	engine, store := newTestRegistryAuthEngine(t)
	rt.AuthEngine = engine
	if err := store.CreateRole("app-only", []string{"read:app/data", "write:app/data"}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateUser("scoped-user", "pw", []string{"app-only"}); err != nil {
		t.Fatal(err)
	}

	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())

	// Authorized namespace: session begins normally, no error.
	authed := dialRawWireClient(t, addr)
	if ack := authed.handshakeWithCredentials(t, "scoped-user", "pw"); !ack.Response.Accepted {
		t.Fatalf("handshake rejected: %v", ack.Response.RejectionReason)
	}
	if ack := authed.sessionBegin(t, "app/data", "READ_COMMITTED"); ack.SessionID == "" || ack.Error != nil {
		t.Fatalf("authorized session begin failed: id=%q err=%v", ack.SessionID, ack.Error)
	}

	// Unauthorized namespace: rejected with an explicit reason.
	if ack := authed.sessionBegin(t, "other/ns", "READ_COMMITTED"); ack.SessionID != "" {
		t.Fatal("session begin on an unauthorized namespace succeeded")
	} else if ack.Error == nil || *ack.Error == "" {
		t.Fatal("rejected session begin carried no error string")
	} else if !strings.Contains(*ack.Error, "other/ns") {
		t.Fatalf("error does not name the denied namespace: %q", *ack.Error)
	}

	// No handshake at all: rejected with the not-authenticated reason.
	unauthed := dialRawWireClient(t, addr)
	if ack := unauthed.sessionBegin(t, "app/data", "READ_COMMITTED"); ack.SessionID != "" {
		t.Fatal("session begin without handshake succeeded")
	} else if ack.Error == nil || !strings.Contains(*ack.Error, "not authenticated") {
		t.Fatalf("expected not-authenticated error, got %v", ack.Error)
	}
}
