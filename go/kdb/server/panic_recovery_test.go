package server_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// TestPanickingStatementDoesNotKillTheServer covers the backstop in dispatchRecovering.
//
// `SELECT 1` panics the SQL parser (kdb/sql/parser.go:475 - readIdentifier, reached for a
// projection that is a literal rather than an identifier). Before the recover, that panic
// unwound out of the connection goroutine and took the entire process with it: every other
// connection, every other namespace. Any client able to run a query could do it by accident,
// and the WebSocket listener widens who that is.
//
// The parser returning an error, the way handleSqlExec already expects it to, is the real fix
// and is tracked separately. This test pins the property that matters regardless of when that
// lands: one client's malformed statement is not everyone else's outage.
func TestPanickingStatementDoesNotKillTheServer(t *testing.T) {
	runtime, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := server.NewKdbServerRuntime(runtime)
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := fmt.Sprintf("tcp://%s", ln.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	victim, err := client.Connect(ctx, addr, "")
	if err != nil {
		t.Fatalf("connect victim: %v", err)
	}
	defer victim.Close()

	bystander, err := client.Connect(ctx, addr, "")
	if err != nil {
		t.Fatalf("connect bystander: %v", err)
	}
	defer bystander.Close()

	// A typed error, promptly - not a panic, and not silence. A dropped reply would leave the
	// caller waiting out its own deadline with no way to tell a wedged server from a slow one
	// (the failure mode finish-up plan item 4.H names).
	_, _, err = victim.QueryRaw(ctx, "app/data", "SELECT 1", nil)
	if err == nil {
		t.Fatal("expected an error from a statement that panics the parser")
	}
	if !strings.Contains(err.Error(), "internal server error") {
		t.Fatalf("expected a typed internal error, got: %v", err)
	}

	// The connection that triggered it is still usable...
	if _, _, err := victim.QueryRaw(ctx, "app/data", "SELECT 1", nil); err == nil {
		t.Fatal("expected the same error on a second attempt")
	}

	// ...and, the actual point, so is everyone else's. If the process had died, this would fail
	// on a closed connection rather than returning a clean per-statement error.
	if _, _, err := bystander.QueryRaw(ctx, "app/data", "SELECT 1", nil); err == nil {
		t.Fatal("expected an error for the bystander too")
	} else if !strings.Contains(err.Error(), "internal server error") {
		t.Fatalf("bystander connection did not survive: %v", err)
	}

	// A well-formed statement on the bystander connection still works, proving the server is
	// serving rather than merely still running.
	if err := bystander.Exec(ctx, "app/data",
		`CREATE TABLE players (name VARCHAR NOT NULL, level VARCHAR NOT NULL)`, nil); err != nil {
		t.Fatalf("server no longer serving valid statements: %v", err)
	}
}
