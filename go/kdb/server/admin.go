package server

import (
	"expvar"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	gometrics "runtime/metrics"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/metrics"
	"github.com/limidus/kdb/go/kdb/version"
)

// AdminServer serves the operational HTTP surface (kdb-finish-up-plan Phase 2.3) on a separate
// address from the data-plane listeners, so it can stay reachable (and unauthenticated -
// deployments should bind it to localhost or a private interface, never the public network)
// while the wire listeners carry TLS/RBAC:
//
//	GET /healthz      - liveness: 200 as long as the process can serve HTTP at all.
//	GET /readyz       - readiness: 200 once every configured listener is bound and the runtime
//	                    is not draining; 503 (with a reason) before startup completes and again
//	                    from the moment an orderly shutdown begins, so load balancers stop
//	                    routing new connections while in-flight work finishes.
//	GET /metrics      - Prometheus text exposition: the write-path stage latencies the
//	                    previously-unreachable metrics.Default recorder has always tracked,
//	                    plus basic Go process gauges.
//	GET /debug/vars   - expvar JSON.
//	GET /debug/pprof/ - net/http/pprof profiles.
type AdminServer struct {
	runtime *KdbServerRuntime
	httpSrv *http.Server
	ln      net.Listener
	ready   atomic.Bool
	// notReadyReason explains a 503 from /readyz ("starting" before SetReady(true),
	// "draining" once shutdown begins) - stored, not derived, so the handler stays lock-free.
	notReadyReason atomic.Value // string
}

// NewAdminServer binds addr (host:port; port 0 for ephemeral) and starts serving immediately.
// The server starts not-ready: call SetReady(true) once every data-plane listener is bound.
func NewAdminServer(addr string, rt *KdbServerRuntime) (*AdminServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("admin listen %s: %w", addr, err)
	}
	a := &AdminServer{runtime: rt, ln: ln}
	a.notReadyReason.Store("starting")

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/readyz", a.handleReadyz)
	mux.HandleFunc("/metrics", a.handleMetrics)
	mux.Handle("/debug/vars", expvar.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	a.httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = a.httpSrv.Serve(ln) }()
	return a, nil
}

// Addr returns the bound address (useful when addr's port was 0).
func (a *AdminServer) Addr() net.Addr { return a.ln.Addr() }

// SetReady flips /readyz between 200 and 503. Pass false with a reason at the start of an
// orderly shutdown, before draining begins, so traffic stops arriving while in-flight work
// completes.
func (a *AdminServer) SetReady(ready bool, reason string) {
	if reason != "" {
		a.notReadyReason.Store(reason)
	}
	a.ready.Store(ready)
}

// Close stops the admin listener. Safe to call more than once.
func (a *AdminServer) Close() error { return a.httpSrv.Close() }

func (a *AdminServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// key=value lines, one per line, so operators and scrapers can pin a live process to the
	// exact source it was built from without shelling into the container. commit is the full
	// SHA deliberately - the short form is for humans reading a banner, not for lookups.
	b := version.Get()
	fmt.Fprintf(w, "ok\nversion=%s\ncommit=%s\ncommit_dirty=%t\nbuild_date=%s\n",
		b.Version, b.Commit, b.Dirty, b.BuildDate)
}

func (a *AdminServer) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// Draining wins over the stored ready flag: BeginDraining can be triggered by the abort
	// watchdog without the signal path (which is what calls SetReady(false)) being involved.
	if a.runtime != nil && a.runtime.IsDraining() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "not ready: draining")
		return
	}
	if !a.ready.Load() {
		reason, _ := a.notReadyReason.Load().(string)
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "not ready: %s\n", reason)
		return
	}
	fmt.Fprintln(w, "ready")
}

// handleMetrics writes Prometheus text-format exposition (version 0.0.4) by hand - the format
// is simple enough that depending on the prometheus client library for four gauge families
// isn't worth a new module in go.mod.
func (a *AdminServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var b strings.Builder

	// The standard build-info pattern: a constant 1 whose labels carry the identity, so a
	// dashboard or alert can group by the commit a series came from and a rollout shows up as
	// two label sets rather than an unexplained step in some other metric.
	bi := version.Get()
	b.WriteString("# HELP kdb_build_info Build identity of the running binary; always 1.\n")
	b.WriteString("# TYPE kdb_build_info gauge\n")
	fmt.Fprintf(&b, "kdb_build_info{version=%q,commit=%q,dirty=\"%t\",build_date=%q,go_version=%q} 1\n",
		bi.Version, bi.Commit, bi.Dirty, bi.BuildDate, bi.GoVersion)

	snaps := metrics.Default.Snapshot()
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Stage < snaps[j].Stage })
	b.WriteString("# HELP kdb_stage_ops_total Total operations recorded per write-path stage.\n")
	b.WriteString("# TYPE kdb_stage_ops_total counter\n")
	for _, s := range snaps {
		fmt.Fprintf(&b, "kdb_stage_ops_total{stage=%q} %d\n", s.Stage, s.Count)
	}
	b.WriteString("# HELP kdb_stage_latency_seconds Write-path stage latency summary over recent samples.\n")
	b.WriteString("# TYPE kdb_stage_latency_seconds gauge\n")
	for _, s := range snaps {
		fmt.Fprintf(&b, "kdb_stage_latency_seconds{stage=%q,stat=\"mean\"} %g\n", s.Stage, s.Mean.Seconds())
		fmt.Fprintf(&b, "kdb_stage_latency_seconds{stage=%q,stat=\"p50\"} %g\n", s.Stage, s.P50.Seconds())
		fmt.Fprintf(&b, "kdb_stage_latency_seconds{stage=%q,stat=\"p99\"} %g\n", s.Stage, s.P99.Seconds())
		fmt.Fprintf(&b, "kdb_stage_latency_seconds{stage=%q,stat=\"max\"} %g\n", s.Stage, s.Max.Seconds())
	}

	// runtime/metrics, not runtime.ReadMemStats: ReadMemStats stops every goroutine for the
	// read - the exact stop-the-world cost the memory guard was rewritten to avoid
	// (kdb-spec-layer13 §2.6) - and a scrape every few seconds was reintroducing it.
	rms := []gometrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	gometrics.Read(rms)
	fmt.Fprintf(&b, "# HELP kdb_go_goroutines Current goroutine count.\n# TYPE kdb_go_goroutines gauge\nkdb_go_goroutines %d\n", runtime.NumGoroutine())
	fmt.Fprintf(&b, "# HELP kdb_go_heap_alloc_bytes Bytes of allocated heap objects.\n# TYPE kdb_go_heap_alloc_bytes gauge\nkdb_go_heap_alloc_bytes %d\n", sampleUint64(rms[0]))
	fmt.Fprintf(&b, "# HELP kdb_go_mem_total_bytes Total bytes of memory mapped by the Go runtime.\n# TYPE kdb_go_mem_total_bytes gauge\nkdb_go_mem_total_bytes %d\n", sampleUint64(rms[1]))
	fmt.Fprintf(&b, "# HELP kdb_go_gc_total Completed GC cycles.\n# TYPE kdb_go_gc_total counter\nkdb_go_gc_total %d\n", sampleUint64(rms[2]))

	draining := 0
	if a.runtime != nil && a.runtime.IsDraining() {
		draining = 1
	}
	fmt.Fprintf(&b, "# HELP kdb_draining Whether the runtime is refusing new writes for shutdown.\n# TYPE kdb_draining gauge\nkdb_draining %d\n", draining)

	a.writeGovernanceMetrics(&b)

	_, _ = w.Write([]byte(b.String()))
}

// writeGovernanceMetrics exposes the admission-control counters. §13: "a shedding server that
// cannot be observed shedding is indistinguishable from a broken one" - and §2.5's latching bug
// went unnoticed for exactly that reason. Until now none of these numbers left the process.
func (a *AdminServer) writeGovernanceMetrics(b *strings.Builder) {
	if a.runtime == nil {
		return
	}
	fmt.Fprintf(b, "# HELP kdb_memory_zone Current pressure zone (0=normal 1=elevated 2=high 3=critical).\n# TYPE kdb_memory_zone gauge\nkdb_memory_zone %d\n", int(a.runtime.MemoryZone()))
	adm := a.runtime.Admission()
	if adm == nil {
		return
	}
	stats := adm.Stats()
	b.WriteString("# HELP kdb_admission_granted_total Grants issued per operation class.\n# TYPE kdb_admission_granted_total counter\n")
	for c := OpClass(0); c < numOpClasses; c++ {
		fmt.Fprintf(b, "kdb_admission_granted_total{class=%q} %d\n", c, stats.Granted[c].Load())
	}
	b.WriteString("# HELP kdb_admission_denied_total Admissions denied, per class and reason.\n# TYPE kdb_admission_denied_total counter\n")
	for c := OpClass(0); c < numOpClasses; c++ {
		fmt.Fprintf(b, "kdb_admission_denied_total{class=%q,reason=\"zone\"} %d\n", c, stats.DeniedZone[c].Load())
		fmt.Fprintf(b, "kdb_admission_denied_total{class=%q,reason=\"capacity\"} %d\n", c, stats.DeniedCapacity[c].Load())
		fmt.Fprintf(b, "kdb_admission_denied_total{class=%q,reason=\"too_large\"} %d\n", c, stats.DeniedTooLarge[c].Load())
	}
	fmt.Fprintf(b, "# HELP kdb_admission_outstanding_bytes Bytes currently reserved by live grants.\n# TYPE kdb_admission_outstanding_bytes gauge\nkdb_admission_outstanding_bytes %d\n", adm.OutstandingBytes())
	fmt.Fprintf(b, "# HELP kdb_admission_floor_bytes Capacity withheld as the non-granted floor.\n# TYPE kdb_admission_floor_bytes gauge\nkdb_admission_floor_bytes %d\n", adm.FloorHeldBytes())
	fmt.Fprintf(b, "# HELP kdb_admission_scan_row_budget Current per-scan rows-examined budget.\n# TYPE kdb_admission_scan_row_budget gauge\nkdb_admission_scan_row_budget %d\n", adm.ScanRowBudget())
	fmt.Fprintf(b, "# HELP kdb_admission_zone_changes_total Pressure-zone transitions.\n# TYPE kdb_admission_zone_changes_total counter\nkdb_admission_zone_changes_total %d\n", stats.ZoneChanges.Load())
	fmt.Fprintf(b, "# HELP kdb_admission_critical_enters_total Entries into the critical zone.\n# TYPE kdb_admission_critical_enters_total counter\nkdb_admission_critical_enters_total %d\n", stats.CriticalEnters.Load())

	costs := adm.Costs()
	if costs == nil {
		return
	}
	b.WriteString("# HELP kdb_cost_estimate_accuracy_p95 p95 of actual/estimate per class; >1 means under-estimation at the tail.\n# TYPE kdb_cost_estimate_accuracy_p95 gauge\n")
	b.WriteString("# HELP kdb_cost_safety_multiplier Estimate scale-up applied while a class under-estimates.\n# TYPE kdb_cost_safety_multiplier gauge\n")
	for _, c := range []OpClass{ClassPointRead, ClassScan} {
		fmt.Fprintf(b, "kdb_cost_estimate_accuracy_p95{class=%q} %g\n", c, costs.AccuracyP95(c))
		fmt.Fprintf(b, "kdb_cost_safety_multiplier{class=%q} %g\n", c, costs.SafetyMultiplier(c))
	}
	fmt.Fprintf(b, "# HELP kdb_cost_learned_cells Learned shape cells currently held.\n# TYPE kdb_cost_learned_cells gauge\nkdb_cost_learned_cells %d\n", costs.LearnedCells())
}
