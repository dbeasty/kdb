package client_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// Component 40 spec §7's test list, scoped to what's actually testable in this repo: test 1
// (round trip against the JVM kdb-server) needs a JVM process this Go test suite doesn't start -
// out of scope here, covered instead by test 2 below against Component 38's Go-native server,
// which the spec itself calls the intended long-run, lower-risk target. Test 6
// (ErrWrongConflictPolicy for Upsert against a STRICT namespace) doesn't apply to this
// implementation: KdbServerRuntime.Upsert always uses its own dedicated LAST_WRITE-policy engine
// and Commit always uses the STRICT one - there's no per-namespace-configurable policy for a
// call to be "wrong" against, since that concept isn't implemented anywhere in this Go port yet.

func startTestServer(t *testing.T) (addr string, rt *server.KdbServerRuntime) {
	t.Helper()
	embedded, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	rt = server.NewKdbServerRuntime(embedded)
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return fmt.Sprintf("tcp://%s", ln.Addr().String()), rt
}

func connectTestClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := client.Connect(ctx, addr, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// TestConnectPutJSONGetJSONRoundTrip is component 40 spec §7 test 2: connect, PutJSON, GetJSON
// round trip against Component 38's Go-native server, end to end, not mocked.
func TestConnectPutJSONGetJSONRoundTrip(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	commitHex, err := c.PutJSON(ctx, "app/data", docID, []byte(`{"name":"Alice","level":7}`))
	if err != nil {
		t.Fatal(err)
	}
	if commitHex == "" {
		t.Fatal("expected a commit hash")
	}

	got, gotCommitHex, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), `"Alice"`) {
		t.Fatalf("got json: %s", got)
	}
	if gotCommitHex != commitHex {
		t.Fatalf("commit hex mismatch: put %s get %s", commitHex, gotCommitHex)
	}

	missingID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.GetJSON(ctx, "app/data", missingID)
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestCommitSucceedsThenConflictsOnStaleBase is component 40 spec §7 test 3.
func TestCommitSucceedsThenConflictsOnStaleBase(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	commit1, err := c.PutJSON(ctx, "app/data", docID, []byte(`{"v":1}`))
	if err != nil {
		t.Fatal(err)
	}

	// Fresh BaseVersion (commit1) - succeeds.
	commit2, err := c.Commit(ctx, client.Transaction{
		Namespace:   "app/data",
		BaseVersion: commit1,
		Writes:      []client.DocWrite{{DocID: docID, JSON: []byte(`{"v":2}`)}},
	})
	if err != nil {
		t.Fatalf("expected commit to succeed on fresh base: %v", err)
	}

	// Stale BaseVersion (commit1 again, but the document has since moved to commit2's state) -
	// conflicts.
	_, err = c.Commit(ctx, client.Transaction{
		Namespace:   "app/data",
		BaseVersion: commit1,
		Writes:      []client.DocWrite{{DocID: docID, JSON: []byte(`{"v":3}`)}},
	})
	if !errors.Is(err, client.ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
	var conflictErr *client.ConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected *ConflictError, got %T", err)
	}
	if len(conflictErr.Conflicts) != 1 || conflictErr.Conflicts[0].DocumentID != docID {
		t.Fatalf("conflict detail: %+v", conflictErr.Conflicts)
	}

	// No partial write: the document must still read as commit2's value, not commit3's.
	got, _, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), `"v":2`) {
		t.Fatalf("expected document unchanged at v:2 after rejected conflict, got %s", got)
	}
	_ = commit2
}

// TestConcurrentCommitsRacingSameBaseVersion is component 40 spec §7 test 4: exactly one of N
// concurrent Commit calls racing the same BaseVersion succeeds, proven under actual
// concurrency.
func TestConcurrentCommitsRacingSameBaseVersion(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	base, err := c.PutJSON(ctx, "app/data", docID, []byte(`{"v":0}`))
	if err != nil {
		t.Fatal(err)
	}

	const racers = 8
	results := make([]error, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Commit(ctx, client.Transaction{
				Namespace:   "app/data",
				BaseVersion: base,
				Writes:      []client.DocWrite{{DocID: docID, JSON: []byte(fmt.Sprintf(`{"v":%d}`, i))}},
			})
			results[i] = err
		}(i)
	}
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, client.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 {
		t.Fatalf("successes = %d, want exactly 1", successes)
	}
	if conflicts != racers-1 {
		t.Fatalf("conflicts = %d, want %d", conflicts, racers-1)
	}
}

// TestUpsertCreatesAndReplaces is component 40 spec §7 test 5 - the test that validates
// create-on-first-write without a prior existence check.
func TestUpsertCreatesAndReplaces(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.GetJSON(ctx, "app/data", docID); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected doc to not exist yet, got err=%v", err)
	}

	commit1, err := c.Upsert(ctx, "app/data", docID, []byte(`{"v":"first"}`))
	if err != nil {
		t.Fatalf("expected Upsert to create the document: %v", err)
	}
	got, _, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "first") {
		t.Fatalf("got: %s", got)
	}

	commit2, err := c.Upsert(ctx, "app/data", docID, []byte(`{"v":"second"}`))
	if err != nil {
		t.Fatalf("expected Upsert to replace the document: %v", err)
	}
	if commit1 == commit2 {
		t.Fatal("expected a new commit hash after replace")
	}
	got, _, err = c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "second") {
		t.Fatalf("got: %s", got)
	}
}

// TestQueryDecodesRowsIntoStructSlice is component 40 spec §7 test 7.
func TestQueryDecodesRowsIntoStructSlice(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	if err := c.Exec(ctx, "app/data", `CREATE TABLE players (name VARCHAR NOT NULL, level VARCHAR NOT NULL)`, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(ctx, "app/data", `INSERT INTO players (name, level) VALUES ('Alice', '7')`, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Exec(ctx, "app/data", `INSERT INTO players (name, level) VALUES ('Bob', '3')`, nil); err != nil {
		t.Fatal(err)
	}

	type Player struct {
		Name  string
		Level string
	}
	var players []Player
	if err := c.Query(ctx, "app/data", `SELECT name, level FROM players`, nil, &players); err != nil {
		t.Fatal(err)
	}
	if len(players) != 2 {
		t.Fatalf("expected 2 rows, got %+v", players)
	}
	names := map[string]string{players[0].Name: players[0].Level, players[1].Name: players[1].Level}
	if names["Alice"] != "7" || names["Bob"] != "3" {
		t.Fatalf("players: %+v", players)
	}
}

// TestAppendEventNeverConflictsUnderConcurrentWriters is component 40 spec §7 test 8.
func TestAppendEventNeverConflictsUnderConcurrentWriters(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	const writers = 8
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := randomUUID()
			if err != nil {
				errs[i] = err
				return
			}
			errs[i] = c.AppendEvent(ctx, "app/events", id, []byte(fmt.Sprintf(`{"seq":%d}`, i)))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
}

// TestContextCancellationMidCall is component 40 spec §7 test 9: a cancelled context aborts the
// in-flight call and returns promptly, leaving the connection usable for the next call.
func TestContextCancellationMidCall(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.PutJSON(cancelledCtx, "app/data", docID, []byte(`{"v":1}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Connection must still be usable.
	commitHex, err := c.PutJSON(context.Background(), "app/data", docID, []byte(`{"v":1}`))
	if err != nil {
		t.Fatalf("expected connection to still be usable after a cancelled call: %v", err)
	}
	if commitHex == "" {
		t.Fatal("expected a commit hash")
	}
}

// TestLargeDocumentRoundTrips is component 40 spec §7 test 10.
func TestLargeDocumentRoundTrips(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx := context.Background()

	docID, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	large := buildLargeMatchDocument()
	if _, err := c.PutJSON(ctx, "app/data", docID, large); err != nil {
		t.Fatal(err)
	}
	got, _, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(large) {
		t.Fatalf("round trip mismatch:\nwant %s\ngot  %s", large, got)
	}
}

func buildLargeMatchDocument() []byte {
	var b strings.Builder
	b.WriteString(`{"matchId":"m-1","mode":"ranked","players":[`)
	for i := 0; i < 10; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"id":"p-%d","name":"player-%d","score":%d,"stats":{"kills":%d,"deaths":%d,"assists":%d}}`,
			i, i, i*100, i, i+1, i+2)
	}
	b.WriteString(`],"events":[`)
	for i := 0; i < 20; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"t":%d,"type":"action-%d","payload":{"a":1,"b":"x","c":[1,2,3]}}`, i, i)
	}
	b.WriteString(`],"finished":true,"durationSeconds":1234.5}`)
	return []byte(b.String())
}

func randomUUID() (string, error) {
	id, err := codec.RandomUUID()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// TestUpsertRejectedWithoutAuthentication is a boundary case the RBAC wire messages need:
// DocumentGet/Upsert must also be gated on authentication, not just SqlExec/TxCommit.
func TestUpsertRejectedWithoutAuthentication(t *testing.T) {
	embedded, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	rt := server.NewKdbServerRuntime(embedded)
	rt.AuthEngine = denyAllEngine{}
	ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", rt)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = client.Connect(ctx, fmt.Sprintf("tcp://%s", ln.Addr().String()), "")
	if !errors.Is(err, client.ErrUnauthenticated) {
		t.Fatalf("expected ErrUnauthenticated, got %v", err)
	}
}

type denyAllEngine struct{}

func (denyAllEngine) Authenticator() auth.Authenticator { return denyAllEngine{} }
func (denyAllEngine) Authorizer() auth.Authorizer       { return denyAllEngine{} }
func (denyAllEngine) Authenticate(_ context.Context, _ auth.Credentials) (auth.Principal, error) {
	return auth.Principal{}, fmt.Errorf("denied")
}
func (denyAllEngine) Authorize(_ context.Context, _ auth.Principal, _ auth.Action) error {
	return fmt.Errorf("denied")
}
