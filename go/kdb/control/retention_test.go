package control

import (
	"net/http"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage"
)

// The retention endpoints, which are what put the mode switch and the
// compaction in front of an operator rather than behind a Go API.

// retentionFixture serves one file-backed namespace with writes allowed, so the operations that
// change retention are reachable.
func retentionFixture(t *testing.T) (*embed.EmbeddedKdbRuntime, string) {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.DeltaMaxSegmentBytes = 4096
	opts.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	rt, err := embed.OpenFileRuntimeWithOptions(t.TempDir(), "demo", "demo/users", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rt.Close)
	srv := server.NewKdbServerRuntime(rt)
	cs, err := New(Options{
		Addr: "127.0.0.1:0", Runtime: srv, Namespace: "demo/users", Version: "test",
		AllowWrites: true,
		Namespaces:  StaticNamespaces(map[string]*server.KdbServerRuntime{"demo/users": srv}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return rt, "http://" + cs.Addr().String()
}

func writeDocs(t *testing.T, rt *embed.EmbeddedKdbRuntime, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		body := `{"id":"doc-` + itoa(i) + `","n":` + itoa(i) + `,"blob":"` + repeat("x", 512) + `"}`
		if _, err := embed.PutJSONDocument(rt, "demo/users", body); err != nil {
			t.Fatal(err)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func repeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
}

// The status endpoint is what a UI puts next to the buttons, so it has to report the state that
// matters: on manual, holding, with a number.
func TestRetentionStatusReportsHolding(t *testing.T) {
	rt, base := retentionFixture(t)
	writeDocs(t, rt, 60)

	// Switch to none, which lands on manual.
	res, body := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/retention/history-mode",
		`{"mode":"none"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("switching mode: %d (%v)", res.StatusCode, body)
	}
	if body["reclaimMode"] != "manual" {
		t.Fatalf("a switch to none landed on %v, want manual", body["reclaimMode"])
	}
	if body["historyLost"] != false {
		t.Error("switching to none reported history lost; it deletes nothing")
	}

	_, status := get(t, base, "/v1/ns/demo%2Fusers/retention")
	if status["historyMode"] != "none" {
		t.Fatalf("status reports mode %v", status["historyMode"])
	}
	if status["reclaimMode"] != "manual" {
		t.Fatalf("status reports reclaim %v", status["reclaimMode"])
	}
	if holding, _ := status["holding"].(bool); !holding {
		t.Error("a manual namespace with eligible segments does not report itself as holding, " +
			"which is the state an operator most needs to see")
	}
	if n, _ := status["eligibleSegments"].(float64); n == 0 {
		t.Error("status reports nothing eligible while holding")
	}
}

// Compacting is the destructive one, and the response has to say whether it was recoverable.
func TestCompactEndpointSaysWhetherItIsReversible(t *testing.T) {
	rt, base := retentionFixture(t)
	writeDocs(t, rt, 60)
	if res, body := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/retention/history-mode",
		`{"mode":"none"}`); res.StatusCode != http.StatusOK {
		t.Fatalf("switching mode: %d (%v)", res.StatusCode, body)
	}

	res, body := sendJSON(t, http.MethodPost, base, "/v1/ns/demo%2Fusers/retention/compact", `{}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("compacting: %d (%v)", res.StatusCode, body)
	}
	if n, _ := body["removed"].(float64); n == 0 {
		t.Fatal("the compaction reclaimed nothing")
	}
	// No archive is configured in this fixture, so it must say so plainly.
	if rev, _ := body["reversible"].(bool); rev {
		t.Error("a compaction with no archive reported itself reversible")
	}
	note, _ := body["note"].(string)
	if note == "" {
		t.Error("the compaction said nothing about what it did")
	}
}

// Switching back after a compaction has to report the loss rather than let "full" read as "the
// history is back".
func TestModeSwitchBackReportsLostHistory(t *testing.T) {
	rt, base := retentionFixture(t)
	writeDocs(t, rt, 60)
	sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/retention/history-mode", `{"mode":"none"}`)
	sendJSON(t, http.MethodPost, base, "/v1/ns/demo%2Fusers/retention/compact", `{}`)

	res, body := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/retention/history-mode",
		`{"mode":"full"}`)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("switching back: %d (%v)", res.StatusCode, body)
	}
	if lost, _ := body["historyLost"].(bool); !lost {
		t.Fatal("switching back after a compaction did not report the loss")
	}
}

// An unknown mode is refused rather than silently doing nothing.
func TestModeSwitchRejectsUnknownMode(t *testing.T) {
	_, base := retentionFixture(t)
	res, _ := sendJSON(t, http.MethodPut, base, "/v1/ns/demo%2Fusers/retention/history-mode",
		`{"mode":"partial"}`)
	if res.StatusCode == http.StatusOK {
		t.Fatal("an unknown history mode was accepted")
	}
}

// Restoring without an archive fails with a reason, not a bare error.
func TestRestoreEndpointRefusesWithoutArchive(t *testing.T) {
	_, base := retentionFixture(t)
	res, body := sendJSON(t, http.MethodPost, base, "/v1/ns/demo%2Fusers/retention/restore",
		`{"from":0,"to":2}`)
	if res.StatusCode == http.StatusOK {
		t.Fatal("restoring with no archive succeeded")
	}
	if msg, _ := body["message"].(string); msg == "" {
		if e, ok := body["error"].(map[string]any); ok {
			if m, _ := e["message"].(string); m == "" {
				t.Error("the refusal carried no message")
			}
		}
	}
}
