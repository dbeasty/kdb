package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

func keysOf(t *testing.T, body []byte) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decoding %q: %v", body, err)
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// A replace leaves exactly the keys it names - a merge never could drop one - and it does so in
// one operation, so there is no moment (and no crash point) where the document is missing.
func TestReplaceJSONDropsKeysAndSurvivesRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	const doc = "6f9619ff-8b86-d011-b42d-00cf4fc964ff"
	ctx := context.Background()

	serve := func() (*client.Client, func()) {
		rt, err := embed.OpenFileRuntime(root, "demo", "app/data", schema.None())
		if err != nil {
			t.Fatal(err)
		}
		srv := server.NewKdbServerRuntime(rt)
		ln, err := server.ListenSqlWire("tcp://127.0.0.1:0?bind=true", srv)
		if err != nil {
			t.Fatal(err)
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		c, err := client.Connect(cctx, "tcp://"+ln.Addr().String(), "")
		if err != nil {
			t.Fatal(err)
		}
		return c, func() { c.Close(); ln.Close(); rt.Close() }
	}

	c, stop := serve()
	if _, err := c.PutJSON(ctx, "app/data", doc, []byte(`{"a":1,"b":2,"c":3}`)); err != nil {
		t.Fatal(err)
	}
	// The merge default is unchanged: naming only d keeps a, b, c.
	if _, err := c.PutJSON(ctx, "app/data", doc, []byte(`{"d":4}`)); err != nil {
		t.Fatal(err)
	}
	body, _, err := c.GetJSON(ctx, "app/data", doc)
	if err != nil || keysOf(t, body) != "a,b,c,d" {
		t.Fatalf("merge: %s %v", body, err)
	}

	if _, err := c.ReplaceJSON(ctx, "app/data", doc, []byte(`{"a":9}`)); err != nil {
		t.Fatalf("replace: %v", err)
	}
	body, _, err = c.GetJSON(ctx, "app/data", doc)
	if err != nil || keysOf(t, body) != "a" {
		t.Fatalf("after replace want exactly {a}: %s %v", body, err)
	}
	stop()

	c, stop = serve()
	defer stop()
	body, _, err = c.GetJSON(ctx, "app/data", doc)
	if err != nil {
		t.Fatalf("the replaced document is missing after a restart: %v", err)
	}
	if keysOf(t, body) != "a" {
		t.Fatalf("shape changed across restart: %s", body)
	}
}

// ReplaceIf is the server-arbitrated claim: of several connections (separate sessions, as
// separate processes would be) racing on one content hash, exactly one wins, and the rest are
// told their premise was wrong rather than silently overwriting.
func TestReplaceIfExactlyOneClaimWins(t *testing.T) {
	addr, _ := startTestServer(t)
	ctx := context.Background()
	const doc = "7f9619ff-8b86-d011-b42d-00cf4fc964ff"
	seed := connectTestClient(t, addr)
	if _, err := seed.PutJSON(ctx, "app/data", doc, []byte(`{"owner":"","extra":true}`)); err != nil {
		t.Fatal(err)
	}
	_, hash, err := seed.GetJSONWithHash(ctx, "app/data", doc)
	if err != nil {
		t.Fatal(err)
	}

	const claimers = 8
	results := make([]error, claimers)
	var wg sync.WaitGroup
	for i := 0; i < claimers; i++ {
		c := connectTestClient(t, addr)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = c.ReplaceIf(ctx, "app/data", doc, []byte(fmt.Sprintf(`{"owner":"c%d"}`, i)), hash)
		}(i)
	}
	wg.Wait()

	wins := 0
	for i, err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, client.ErrPreconditionFailed), errors.Is(err, client.ErrConflict):
		default:
			t.Fatalf("claimer %d: unexpected error %v", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("%d claimers won, want exactly 1", wins)
	}
	body, _, err := seed.GetJSON(ctx, "app/data", doc)
	if err != nil || keysOf(t, body) != "owner" {
		t.Fatalf("winner's replace should have dropped the extra key: %s %v", body, err)
	}
}
