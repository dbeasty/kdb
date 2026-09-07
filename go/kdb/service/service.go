package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/index/stores"
	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transaction"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/version"
)

// KdbServiceMain is a skeleton service entrypoint mirroring dev.kdb.service.KdbServiceMain.
func main() {
	if len(os.Args) > 1 && os.Args[1] == "user" {
		os.Exit(runUserCommand(os.Args[2:]))
	}

	fs := flag.NewFlagSet("kdb-service", flag.ExitOnError)
	flagVals := config.DefaultServiceSettings()
	var configPath string
	var showVersion bool
	var peerConflictPolicy string
	var expireField string
	var expireGrace, expireInterval time.Duration
	fs.StringVar(&expireField, "expire-field", "", "document expiry (kdb-spec-layer16 §9.5): the top-level field (or dotted path) holding each document's expiry timestamp as an RFC 3339 string or epoch milliseconds. Documents whose timestamp has passed are hidden from reads at head and deleted by a periodic sweep; empty (default) disables expiry")
	fs.DurationVar(&expireGrace, "expire-grace", 0, "how long a document stays readable past its --expire-field timestamp before it counts as expired")
	fs.DurationVar(&expireInterval, "expire-interval", time.Duration(policy.DefaultSweepIntervalMillis)*time.Millisecond, "how often the expiry sweeper scans head and deletes expired documents (batches of at most 500 per commit, message \"expiry sweep\")")
	fs.StringVar(&peerConflictPolicy, "peer-conflict-policy", "strict", "how the peer-sync listener resolves a same-document divergence pushed by a peer: strict (report a conflict, never silently resolve - default) or last-write (later timestamp wins symmetrically on every node)")
	fs.StringVar(&configPath, "config", "", "JSON config file (see go/kdb/config's ServiceFile for the shape) - precedence is config file < KDB_* environment variables < explicitly-set flags")
	fs.StringVar(&flagVals.DataDir, "data-dir", flagVals.DataDir, "filesystem data root")
	fs.BoolVar(&flagVals.Memory, "memory", flagVals.Memory, "use in-memory runtime")
	fs.StringVar(&flagVals.Namespace, "namespace", flagVals.Namespace, "default namespace")
	fs.StringVar(&flagVals.SQLAddr, "sql-addr", flagVals.SQLAddr, "SQL wire listen address (empty to disable)")
	fs.StringVar(&flagVals.PeerAddr, "peer-addr", flagVals.PeerAddr, "peer sync (Mode 3 full-peer) wire listen address (empty to disable)")
	fs.StringVar(&flagVals.StreamAddr, "stream-addr", flagVals.StreamAddr, "stream (Mode 1 read-only / Mode 2 write-back) wire listen address (empty to disable)")
	fs.StringVar(&flagVals.WSAddr, "ws-addr", flagVals.WSAddr, "WebSocket SQL-wire listen address, ws:// or wss:// (empty to disable) - the only transport a browser can open")
	fs.BoolVar(&flagVals.RBAC, "rbac", flagVals.RBAC, "enable RBAC (in-memory user/role registry - create users via the Go API; no admin SQL surface yet)")
	fs.IntVar(&flagVals.MemoryBudgetMB, "memory-budget-mb", flagVals.MemoryBudgetMB, "memory budget that admission control governs against: operations reserve their estimated memory cost before running, and are refused with a typed, retryable error once the budget is committed, rather than the process being OOM-killed with no signal to the client. 0 (default) auto-detects - the cgroup/container memory limit where there is one, else 75% of host RAM - so governance is on by default; -1 disables it entirely; a positive value is an explicit budget in MiB. With Component 48's accounting this can be set at the container's real --memory limit, unlike the deprecated --memory-limit-mb it replaces")
	fs.IntVar(&flagVals.MemoryLimitMB, "memory-limit-mb", flagVals.MemoryLimitMB, "DEPRECATED alias for --memory-budget-mb, retained for existing configs. Its old meaning is preserved: an explicit 0 disables governance (whereas --memory-budget-mb 0 auto-detects). The old guidance to set this to only 60-80% of the container limit no longer applies - it was a workaround for the reactive sampler this replaces")
	fs.IntVar(&flagVals.MemoryReserveMB, "memory-reserve-mb", flagVals.MemoryReserveMB, "rescue reserve held back from the grant system and released on entry to the Critical pressure zone, so in-flight commits can finish, storage can be flushed, and typed rejections can be written instead of the process dying partway through (kdb-spec-layer13 Component 48 §5.6)")
	fs.IntVar(&flagVals.MaxConnections, "max-connections", flagVals.MaxConnections, "cap on concurrently-accepted connections per listener; connections past the cap are closed at accept time. Each accepted connection costs a goroutine stack and a frame buffer whether or not it sends anything, none of which admission control can see - 0 means unlimited (kdb-spec-layer13 Component 49 §6.5)")
	fs.IntVar(&flagVals.ScanRowBudget, "scan-row-budget", flagVals.ScanRowBudget, "maximum rows a single scan may examine (not merely return) before it is aborted with RESOURCE_EXHAUSTED; shrinks automatically as memory pressure rises. 0 means unlimited (kdb-spec-layer13 Component 48 §5.2)")
	fs.DurationVar(&flagVals.AbortAfter, "abort-after", flagVals.AbortAfter, "if memory pressure (see --memory-budget-mb) stays tripped for at least this long with no recovery, perform an orderly shutdown (stop accepting new work, flush/seal storage, exit 75) instead of staying up indefinitely rejecting writes - see kdb-spec-layer13 Component 50. Requires a process supervisor (Docker --restart=on-failure, systemd Restart=on-failure) to actually restart the service; this process never restarts itself. 0 disables (default) - this should be rare enough in practice that leaving it off is a reasonable default until you have evidence otherwise")
	fs.StringVar(&flagVals.TLSCert, "tls-cert", flagVals.TLSCert, "PEM certificate file - set together with --tls-key to require TLS on the SQL/peer-sync/stream listeners (each --*-addr's scheme is upgraded from tcp:// to tcps:// automatically)")
	fs.StringVar(&flagVals.TLSKey, "tls-key", flagVals.TLSKey, "PEM private key file, paired with --tls-cert")
	fs.StringVar(&flagVals.TLSCA, "tls-ca", flagVals.TLSCA, "PEM CA bundle to verify client certificates against - required by --tls-client-auth, optional (accept-but-don't-require) otherwise")
	fs.BoolVar(&flagVals.TLSClientAuth, "tls-client-auth", flagVals.TLSClientAuth, "require and verify a client certificate on every TLS connection (mTLS) - requires --tls-ca")
	fs.StringVar(&flagVals.AdminAddr, "admin-addr", flagVals.AdminAddr, "operational HTTP endpoint (host:port) serving /healthz, /readyz, /metrics (Prometheus), /debug/vars, /debug/pprof - plain HTTP with no auth, so bind it to localhost or a private interface, never the public network (empty to disable)")
	fs.DurationVar(&flagVals.DrainTimeout, "drain-timeout", flagVals.DrainTimeout, "on SIGTERM/SIGINT, how long to wait for already-admitted writes to finish before closing storage anyway - new writes are rejected immediately either way, and storage stays crash-consistent even when the deadline is hit (the WAL/delta replay path covers whatever didn't get flushed)")
	fs.StringVar(&flagVals.LogLevel, "log-level", flagVals.LogLevel, "minimum log level: debug, info, warn, error")
	fs.StringVar(&flagVals.LogFormat, "log-format", flagVals.LogFormat, "log output format: text or json")
	fs.StringVar(&flagVals.Durability, "durability", flagVals.Durability, "how much of the write-out a commit waits for: sync (default - an acknowledged write is fsynced; concurrent commits share the fsync via group commit, so this is no longer a physical sync per write), async (acknowledged once queued in memory - a crash can lose whatever had not been flushed), or memory (nothing is written to the delta log at all; everything is lost on restart - tests and throwaway workloads only)")
	fs.IntVar(&flagVals.AsyncSyncIntervalMS, "async-sync-interval-ms", flagVals.AsyncSyncIntervalMS, "background sync period under --durability=async; ignored otherwise")
	fs.StringVar(&flagVals.Compression, "compression", flagVals.Compression, "codec for newly-written delta frames and SSTable blocks: zstd (default) or none. Each frame records its own codec, so changing this leaves already-written segments readable")
	fs.StringVar(&flagVals.SyncMode, "sync-mode", flagVals.SyncMode, "physical sync primitive: full (default - data forced to media, F_FULLFSYNC on macOS, survives power loss) or fast (F_BARRIERFSYNC on macOS / fdatasync on Linux - survives process and OS crashes, an order of magnitude cheaper; power loss can lose what the drive cache held)")
	fs.BoolVar(&showVersion, "version", false, "print version and exit")
	_ = fs.Parse(os.Args[1:])

	if showVersion {
		fmt.Println(version.String())
		return
	}

	// Phase 2.6: file < env < flags. Flag values only win where the operator actually typed
	// them - fs.Visit only sees explicitly-set flags, so untouched flag defaults can't mask the
	// config file or environment.
	var fileCfg *config.ServiceFile
	var err error
	if configPath != "" {
		fileCfg, err = config.LoadServiceFile(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(2)
		}
	}
	explicitFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicitFlags[f.Name] = true })
	cfg, err := config.ResolveService(fileCfg, os.LookupEnv, func(name string) bool { return explicitFlags[name] }, flagVals)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(2)
	}
	dataDir, memory, namespace := cfg.DataDir, cfg.Memory, cfg.Namespace
	sqlAddr, peerAddr, streamAddr, adminAddr := cfg.SQLAddr, cfg.PeerAddr, cfg.StreamAddr, cfg.AdminAddr
	wsAddr := cfg.WSAddr
	rbac, abortAfter, drainTimeout := cfg.RBAC, cfg.AbortAfter, cfg.DrainTimeout

	logger, err := buildLogger(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(2)
	}
	slog.SetDefault(logger)

	if dataDir == "" && !memory {
		memory = true
	}
	if dataDir != "" && memory {
		fmt.Fprintln(os.Stderr, "use either --data-dir or --memory")
		os.Exit(2)
	}

	tlsSettings, err := tlsSettingsFromFlags(cfg.TLSCert, cfg.TLSKey, cfg.TLSCA, cfg.TLSClientAuth)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(2)
	}
	if tlsSettings != nil {
		sqlAddr = secureScheme(sqlAddr)
		peerAddr = secureScheme(peerAddr)
		streamAddr = secureScheme(streamAddr)
		wsAddr = secureScheme(wsAddr)
	}

	var rt *embed.EmbeddedKdbRuntime
	if memory {
		catalog := embed.CatalogFromNamespace(namespace)
		rt, err = embed.OpenMemoryRuntime(catalog, namespace, schema.None())
	} else {
		catalog := embed.CatalogFromNamespace(namespace)
		// ResolveService already validated both names, so these cannot fail here.
		durability, _ := config.ParseDurability(cfg.Durability)
		compression, _ := config.ParseCompression(cfg.Compression)
		syncMode, _ := config.ParseSyncMode(cfg.SyncMode)
		opts := embed.FileRuntimeOptionsFromEnv()
		opts.Storage = embed.StorageOptions{
			Durability:              durability,
			Compression:             &compression,
			AsyncSyncIntervalMillis: int64(cfg.AsyncSyncIntervalMS),
			SyncMode:                syncMode,
		}
		rt, err = embed.OpenFileRuntimeWithOptions(dataDir, catalog, namespace, schema.None(), opts)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	srv := server.NewKdbServerRuntime(rt)

	switch peerConflictPolicy {
	case "", "strict":
		// default: report same-document divergence, never silently resolve
	case "last-write":
		srv.PeerSyncConflictPolicy = transaction.ConflictPolicyLastWrite
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown --peer-conflict-policy %q (want strict or last-write)\n", peerConflictPolicy)
		os.Exit(2)
	}

	// Resource governance. Unlike every previous release this is on unless explicitly turned
	// off: the mechanism that keeps sustained write load from ending in an OOM kill was
	// previously inert in every deployment that did not know to ask for it by name.
	memoryLimitStatus := "disabled (--memory-budget-mb=-1)"
	if cfg.MemoryBudgetMB >= 0 {
		var budgetBytes uint64
		source := "explicit"
		if cfg.MemoryBudgetMB == 0 {
			budgetBytes = server.DetectMemoryBudgetBytes()
			source = "auto-detected"
		} else {
			budgetBytes = uint64(cfg.MemoryBudgetMB) * 1024 * 1024
		}
		if budgetBytes == 0 {
			memoryLimitStatus = "disabled (no cgroup limit and no readable host memory to auto-detect from)"
		} else {
			reserveBytes := int64(cfg.MemoryReserveMB) * 1024 * 1024
			srv.SetMemoryBudget(budgetBytes, 0.85, reserveBytes, int64(cfg.ScanRowBudget))
			// Warm the cost estimator with what a previous run learned. The value is for rare,
			// expensive scan shapes (executed once an hour, they would otherwise relearn from
			// scratch after every restart); write costs are compiled-in calibration and hot
			// shapes relearn in milliseconds regardless. Restore treats the file as a
			// discounted prior and silently ignores anything malformed or stale - a bad cost
			// file must never stop the server (P4, crash-only).
			if dataDir != "" {
				loadCostModelState(srv, filepath.Join(dataDir, costModelStateFile))
			}
			// Make the GC spend CPU before admission has to start refusing work - the first
			// response to a rising heap should be collecting harder, not shedding requests.
			goMemLimit := server.ApplyGoMemoryLimit(budgetBytes)
			// Report the reserve the admission system actually holds, not the configured
			// value - NewAdmission clamps it to a quarter of the budget.
			memoryLimitStatus = fmt.Sprintf("%dMB %s (zones at 70/85/93%%, reserve %dMB, GOMEMLIMIT %dMB)",
				budgetBytes/(1024*1024), source, srv.Admission().RescueReserveBytes()/(1024*1024), goMemLimit/(1024*1024))
		}
	}
	srv.MaxConnections = cfg.MaxConnections

	if expireField != "" {
		if expireGrace < 0 || expireInterval <= 0 {
			fmt.Fprintln(os.Stderr, "Error: --expire-grace must be >= 0 and --expire-interval > 0")
			os.Exit(2)
		}
		srv.SetDocumentExpiry(&policy.DocumentExpiryPolicy{
			FieldPath:           expireField,
			GraceMillis:         expireGrace.Milliseconds(),
			SweepIntervalMillis: expireInterval.Milliseconds(),
		})
	}

	// Index registry (kdb-spec-layer16 Components 67/68/69): loads any persisted full-text,
	// vector, hash and btree indexes, rebuilds stale ones, and wires them to the SQL planner
	// and to SEARCH. Without this the server still answers every query by full scan, so a
	// failure here is fatal rather than silent - a half-indexed server returns wrong answers.
	if _, err := srv.OpenIndexes(stores.Options{}); err != nil {
		fmt.Fprintf(os.Stderr, "Error: opening indexes: %v\n", err)
		os.Exit(1)
	}

	rbacStatus := "disabled"
	if rbac {
		if dataDir != "" {
			// Durable registry (Phase 2.7): users/roles live in the reserved _system/users and
			// _system/roles namespaces under --data-dir, persisted through the same delta-log
			// machinery as data namespaces. Bootstrap users with `kdb-service user create`
			// (service stopped - the data-dir lock enforces that) before first start.
			reg, err := embed.OpenFileAuthRegistry(dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: rbac registry: %v\n", err)
				os.Exit(1)
			}
			defer reg.Close()
			srv.AuthEngine = auth.NewRegistryAuthEngine(reg.Store)
			rbacStatus = "enabled (durable registry under --data-dir; bootstrap via `kdb-service user create`)"
		} else {
			usersDag, err := dag.NewInMemoryCommitDag(auth.UsersNamespace)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: rbac users dag: %v\n", err)
				os.Exit(1)
			}
			rolesDag, err := dag.NewInMemoryCommitDag(auth.RolesNamespace)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: rbac roles dag: %v\n", err)
				os.Exit(1)
			}
			store, err := auth.NewRegistryAuthStore(usersDag, rolesDag, mem.NewInMemoryStorageAdapter())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: rbac registry: %v\n", err)
				os.Exit(1)
			}
			srv.AuthEngine = auth.NewRegistryAuthEngine(store)
			rbacStatus = "enabled (in-memory registry - users vanish on restart; use --data-dir for durability)"
		}
	}

	peerStatus := "disabled"
	streamStatus := "disabled"
	sqlStatus := "disabled"
	var sqlListener *server.Listener
	if sqlAddr != "" {
		sqlListener, err = server.ListenSqlWireTLS(sqlAddr, srv, tlsSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: sql wire listen: %v\n", err)
			os.Exit(1)
		}
		defer sqlListener.Close()
		sqlStatus = fmt.Sprintf("enabled (%s)", sqlListener.Addr())
	}
	// The browser-reachable listener. Started alongside the TCP one rather than instead of it:
	// a deployment commonly serves native clients on raw TCP and browsers on WebSocket at the
	// same time, and both paths run the identical connection handler above the transport.
	wsStatus := "disabled"
	var wsListener *server.Listener
	if wsAddr != "" {
		wsListener, err = server.ListenSqlWireWSTLS(wsAddr, srv, tlsSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: websocket listen: %v\n", err)
			os.Exit(1)
		}
		defer wsListener.Close()
		wsStatus = fmt.Sprintf("enabled (%s)", wsListener.Addr())
	}
	var peerListener *server.Listener
	if peerAddr != "" {
		peerListener, err = server.ListenPeerSyncTLS(peerAddr, srv, namespace, tlsSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: peer sync listen: %v\n", err)
			os.Exit(1)
		}
		defer peerListener.Close()
		peerStatus = fmt.Sprintf("enabled (%s)", peerListener.Addr())
	}
	var streamListener *server.Listener
	if streamAddr != "" {
		var hub *server.StreamHub
		hub, streamListener, err = server.ListenStreamTLS(streamAddr, srv, namespace, tlsSettings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: stream listen: %v\n", err)
			os.Exit(1)
		}
		defer streamListener.Close()
		// The cross-write notification bridge (KdbServerRuntime.CommitListener's own doc
		// comment): without this, the stream hub would accept connections and handshakes but
		// never actually publish anything, since nothing would ever call hub.Publish.
		srv.CommitListener = func(ns string, commit document.Commit) {
			parentHash := codec.Hash{}
			if len(commit.ParentHashes) > 0 {
				parentHash = commit.ParentHashes[0]
			}
			hub.Publish(stream.PublishedCommit{
				CommitHash:      commit.Hash,
				ParentHash:      parentHash,
				Operations:      commit.Operations,
				TimestampMicros: commit.Timestamp.EpochMicros(),
			})
		}
		streamStatus = fmt.Sprintf("enabled (%s)", streamListener.Addr())
	}
	abortStatus := "disabled"
	var watchdog *server.AbortWatchdog
	if abortAfter > 0 {
		var closers multiCloser
		if sqlListener != nil {
			closers = append(closers, sqlListener)
		}
		if peerListener != nil {
			closers = append(closers, peerListener)
		}
		if streamListener != nil {
			closers = append(closers, streamListener)
		}
		var listenerCloser io.Closer
		if len(closers) > 0 {
			listenerCloser = closers
		}
		watchdog = server.NewAbortWatchdog(srv, listenerCloser, abortAfter)
		watchdog.Start()
		abortStatus = abortAfter.String()
	}

	var admin *server.AdminServer
	adminStatus := "disabled"
	if adminAddr != "" {
		admin, err = server.NewAdminServer(adminAddr, srv)
		if err != nil {
			slog.Error("admin listen failed", "error", err)
			os.Exit(1)
		}
		defer admin.Close()
		adminStatus = fmt.Sprintf("enabled (%s)", admin.Addr())
	}

	tlsStatus := "disabled (plaintext)"
	if tlsSettings != nil {
		tlsStatus = "enabled"
		if tlsSettings.RequireClientAuth {
			tlsStatus = "enabled (mTLS: client cert required)"
		}
	}
	build := version.Get()
	slog.Info("KDB service started",
		"version", build.Version,
		// Full SHA, not the short form: this line is how a running service is traced back to
		// the exact source it was built from.
		"commit", build.Commit,
		"commit_dirty", build.Dirty,
		"build_date", build.BuildDate,
		"peer", peerStatus,
		"stream", streamStatus,
		"sql", sqlStatus,
		"ws", wsStatus,
		"admin", adminStatus,
		"tls", tlsStatus,
		"rbac", rbacStatus,
		"memory_limit", memoryLimitStatus,
		"abort_after", abortStatus,
		"document_expiry", srv.ExpirySummary(),
		"namespace", namespace,
	)
	if admin != nil {
		admin.SetReady(true, "")
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	received := <-sig
	slog.Info("shutdown signal received, draining", "signal", received.String(), "drain_timeout", drainTimeout.String())

	// Orderly shutdown (kdb-finish-up-plan Phase 2.4), in this order:
	// 1. Flip /readyz to 503 so load balancers stop sending new connections.
	// 2. Stop the abort watchdog so it doesn't race this deliberate exit (see
	//    AbortWatchdog.Stop's own doc comment).
	// 3. BeginDraining - every new write is rejected immediately with *UnavailableError.
	// 4. Close the data-plane listeners - no new connections at all.
	// 5. Wait (bounded by --drain-timeout) for already-admitted writes to finish.
	// 6. Release - flush and seal the delta segment, close storage.
	// Storage stays crash-consistent even if step 5 times out: replay covers the rest.
	if admin != nil {
		admin.SetReady(false, "draining")
	}
	watchdog.Stop()
	srv.BeginDraining()
	if sqlListener != nil {
		_ = sqlListener.Close()
	}
	if peerListener != nil {
		_ = peerListener.Close()
	}
	if streamListener != nil {
		_ = streamListener.Close()
	}
	if srv.WaitForWritesToDrain(drainTimeout) {
		slog.Info("drain complete, closing storage")
	} else {
		slog.Warn("drain timeout hit with writes still in flight, closing storage anyway", "drain_timeout", drainTimeout.String())
	}
	if dataDir != "" {
		saveCostModelState(srv, filepath.Join(dataDir, costModelStateFile))
	}
	srv.Release()
	slog.Info("shutdown complete")
}

// costModelStateFile is where the learned cost-estimator state lives under --data-dir. It is a
// cache, not data: deleting it is always safe and merely costs relearning.
const costModelStateFile = "costmodel.json"

// costModelStateMaxAge caps how old a persisted cost state may be before it is ignored: a file
// from last month describes a workload and namespace scale that may no longer exist, and the
// structural estimator is a safer starting point than confidently-stale cells.
const costModelStateMaxAge = 7 * 24 * time.Hour

func loadCostModelState(srv *server.KdbServerRuntime, path string) {
	adm := srv.Admission()
	if adm == nil || adm.Costs() == nil {
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		return // no prior state - the common first-boot case
	}
	if time.Since(info.ModTime()) > costModelStateMaxAge {
		slog.Info("ignoring stale cost model state", "path", path, "age", time.Since(info.ModTime()).String())
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	adm.Costs().RestoreState(data)
	slog.Info("cost model state restored", "path", path, "learned_cells", adm.Costs().LearnedCells())
}

func saveCostModelState(srv *server.KdbServerRuntime, path string) {
	adm := srv.Admission()
	if adm == nil || adm.Costs() == nil {
		return
	}
	data, err := adm.Costs().SnapshotState()
	if err != nil {
		return
	}
	// Write-then-rename so a crash mid-write leaves either the old state or none - never a
	// torn file (which RestoreState would discard anyway, but why make it).
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		return
	}
	slog.Info("cost model state saved", "path", path, "learned_cells", adm.Costs().LearnedCells())
}

// buildLogger constructs the process-wide slog.Logger from --log-level/--log-format
// (kdb-finish-up-plan Phase 2.5). Text is the default for a human watching a terminal; json for
// log pipelines.
func buildLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return nil, fmt.Errorf("unknown --log-level %q (want debug, info, warn, or error)", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch strings.ToLower(format) {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("unknown --log-format %q (want text or json)", format)
	}
}

// tlsSettingsFromFlags turns --tls-cert/--tls-key/--tls-ca/--tls-client-auth into
// *core.TransportTlsSettings, or nil if TLS wasn't requested at all (--tls-cert and --tls-key
// both empty). It only validates the flag combination here - BuildTLSConfig (called from inside
// ListenSqlWireTLS/etc.) is what actually loads and validates the cert/key/CA files themselves,
// so a bad path surfaces as a listen error naming which listener failed, not a generic startup
// error before anything else has even been attempted.
func tlsSettingsFromFlags(certFile, keyFile, caFile string, requireClientAuth bool) (*core.TransportTlsSettings, error) {
	if certFile == "" && keyFile == "" && caFile == "" && !requireClientAuth {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("--tls-cert and --tls-key must both be set to enable TLS")
	}
	if requireClientAuth && caFile == "" {
		return nil, fmt.Errorf("--tls-client-auth requires --tls-ca, to verify a presented client certificate against")
	}
	return &core.TransportTlsSettings{
		Enabled:           true,
		CertFile:          certFile,
		KeyFile:           keyFile,
		CAFile:            caFile,
		RequireClientAuth: requireClientAuth,
	}, nil
}

// secureScheme upgrades a tcp://... or kdb+tcp://... listen URI to tcps://.../kdb+tcps://...
// so operators don't have to remember to change --sql-addr/--peer-addr/--stream-addr's scheme
// themselves whenever --tls-cert is set - the URI's scheme is still what
// tcp.Transport.ListenBound actually checks (see kdb/transport/tcp's ParseURI), this just keeps
// that in sync with the flags automatically instead of leaving a plaintext-by-typo footgun.
func secureScheme(addr string) string {
	switch {
	case strings.HasPrefix(addr, "kdb+tcp://"):
		return "kdb+tcps://" + strings.TrimPrefix(addr, "kdb+tcp://")
	case strings.HasPrefix(addr, "tcp://"):
		return "tcps://" + strings.TrimPrefix(addr, "tcp://")
	case strings.HasPrefix(addr, "ws://"):
		return "wss://" + strings.TrimPrefix(addr, "ws://")
	default:
		return addr
	}
}

// multiCloser closes every listener in order, returning the first error encountered (closing
// the rest regardless) - AbortWatchdog takes a single io.Closer, but an abort must stop accepting
// on every listener that's actually running, not just whichever one happened to be wired first.
type multiCloser []io.Closer

func (m multiCloser) Close() error {
	var first error
	for _, c := range m {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
