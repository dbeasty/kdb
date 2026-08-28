package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/schema"
)

func newAdminForTest(t *testing.T) (*AdminServer, *KdbServerRuntime) {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime(embed.CatalogFromNamespace("adm/ns"), "adm/ns", schema.None())
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	srv := NewKdbServerRuntime(rt)
	t.Cleanup(srv.Release)
	admin, err := NewAdminServer("127.0.0.1:0", srv)
	if err != nil {
		t.Fatalf("admin listen: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	return admin, srv
}

func adminGet(t *testing.T, admin *AdminServer, path string) (int, string) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s%s", admin.Addr(), path))
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s body: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

func TestAdminHealthzAlwaysOK(t *testing.T) {
	admin, _ := newAdminForTest(t)
	code, body := adminGet(t, admin, "/healthz")
	if code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", code)
	}
	if !strings.Contains(body, "version=") {
		t.Fatalf("healthz body missing version: %q", body)
	}
}

func TestAdminReadyzLifecycle(t *testing.T) {
	admin, srv := newAdminForTest(t)

	// Before SetReady: 503 with the "starting" reason.
	code, body := adminGet(t, admin, "/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "starting") {
		t.Fatalf("pre-ready readyz = %d %q, want 503 starting", code, body)
	}

	admin.SetReady(true, "")
	code, body = adminGet(t, admin, "/readyz")
	if code != http.StatusOK || !strings.Contains(body, "ready") {
		t.Fatalf("ready readyz = %d %q, want 200 ready", code, body)
	}

	// Draining flips readiness back to 503 even without SetReady(false) - the abort watchdog
	// calls BeginDraining directly, bypassing the signal path.
	srv.BeginDraining()
	code, body = adminGet(t, admin, "/readyz")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "draining") {
		t.Fatalf("draining readyz = %d %q, want 503 draining", code, body)
	}
}

func TestAdminMetricsExposition(t *testing.T) {
	admin, _ := newAdminForTest(t)
	metrics.Default.Record(metrics.StageFsyncWait, 3*time.Millisecond)

	code, body := adminGet(t, admin, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", code)
	}
	for _, want := range []string{
		`kdb_stage_ops_total{stage="fsync_wait"}`,
		`kdb_stage_latency_seconds{stage="fsync_wait",stat="p99"}`,
		"kdb_go_goroutines",
		"kdb_draining 0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestAdminPprofAndExpvarRespond(t *testing.T) {
	admin, _ := newAdminForTest(t)
	for _, path := range []string{"/debug/pprof/", "/debug/vars"} {
		code, _ := adminGet(t, admin, path)
		if code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", path, code)
		}
	}
}
