package control

import (
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
)

// The namespace status endpoint carries what open found in the delta log, so an operator (and a
// crash test's evidence) can see logged commits replay stored without applying.
func TestStatusReportsRecovery(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	rt, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := embed.PutJSONDocument(rt, "demo/users", `{"id":"a"}`); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	reopened, err := embed.OpenFileRuntime(root, "demo", "demo/users", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	cs, err := New(Options{
		Addr: "127.0.0.1:0", Runtime: server.NewKdbServerRuntime(reopened), Namespace: "demo/users", Version: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	_, st := get(t, "http://"+cs.Addr().String(), "/v1/ns/demo%2Fusers/status")
	rec, ok := st["recovery"].(map[string]any)
	if !ok {
		t.Fatalf("status has no recovery report: %v", st)
	}
	for _, k := range []string{"replayedInFull", "commitsNotApplied", "commitsOffMain"} {
		if _, ok := rec[k]; !ok {
			t.Fatalf("recovery report lacks %q: %v", k, rec)
		}
	}
	if rec["commitsOffMain"].(float64) != 0 {
		t.Fatalf("a single-writer log reported commits off main: %v", rec)
	}
}
