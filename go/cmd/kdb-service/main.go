package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/config"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/stream"
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
	fs.StringVar(&configPath, "config", "", "JSON config file (see go/kdb/config's ServiceFile for the shape) - precedence is config file < KDB_* environment variables < explicitly-set flags")
	fs.StringVar(&flagVals.DataDir, "data-dir", flagVals.DataDir, "filesystem data root")
	fs.BoolVar(&flagVals.Memory, "memory", flagVals.Memory, "use in-memory runtime")
	fs.StringVar(&flagVals.Namespace, "namespace", flagVals.Namespace, "default namespace")
	fs.StringVar(&flagVals.SQLAddr, "sql-addr", flagVals.SQLAddr, "SQL wire listen address (empty to disable)")
	fs.StringVar(&flagVals.PeerAddr, "peer-addr", flagVals.PeerAddr, "peer sync (Mode 3 full-peer) wire listen address (empty to disable)")
	fs.StringVar(&flagVals.StreamAddr, "stream-addr", flagVals.StreamAddr, "stream (Mode 1 read-only / Mode 2 write-back) wire listen address (empty to disable)")
	fs.BoolVar(&flagVals.RBAC, "rbac", flagVals.RBAC, "enable RBAC (in-memory user/role registry - create users via the Go API; no admin SQL surface yet)")
	fs.IntVar(&flagVals.MemoryLimitMB, "memory-limit-mb", flagVals.MemoryLimitMB, "reject new writes (rather than risk an OS OOM-kill under sustained load) once process memory nears this budget; rejection triggers at 85% of this value, so set this to 60-80% of the container's actual --memory limit - 80% carries no throughput cost over 60% but do not go above 80% until kdb-spec-layer13 Component 48's full admission control lands, since a burst of already-admitted writes between the guard's periodic samples can still outrun a trip point that close to the real ceiling - see docs/benchmarks/lightsail-sim/README.md for the numbers behind that guidance. 0 disables (default)")
	fs.DurationVar(&flagVals.AbortAfter, "abort-after", flagVals.AbortAfter, "if memory pressure (see --memory-limit-mb) stays tripped for at least this long with no recovery, perform an orderly shutdown (stop accepting new work, flush/seal storage, exit 75) instead of staying up indefinitely rejecting writes - see kdb-spec-layer13 Component 50. Requires a process supervisor (Docker --restart=on-failure, systemd Restart=on-failure) to actually restart the service; this process never restarts itself. 0 disables (default) - this should be rare enough in practice that leaving it off is a reasonable default until you have evidence otherwise")
	fs.StringVar(&flagVals.TLSCert, "tls-cert", flagVals.TLSCert, "PEM certificate file - set together with --tls-key to require TLS on the SQL/peer-sync/stream listeners (each --*-addr's scheme is upgraded from tcp:// to tcps:// automatically)")
	fs.StringVar(&flagVals.TLSKey, "tls-key", flagVals.TLSKey, "PEM private key file, paired with --tls-cert")
	fs.StringVar(&flagVals.TLSCA, "tls-ca", flagVals.TLSCA, "PEM CA bundle to verify client certificates against - required by --tls-client-auth, optional (accept-but-don't-require) otherwise")
	fs.BoolVar(&flagVals.TLSClientAuth, "tls-client-auth", flagVals.TLSClientAuth, "require and verify a client certificate on every TLS connection (mTLS) - requires --tls-ca")
	fs.StringVar(&flagVals.AdminAddr, "admin-addr", flagVals.AdminAddr, "operational HTTP endpoint (host:port) serving /healthz, /readyz, /metrics (Prometheus), /debug/vars, /debug/pprof - plain HTTP with no auth, so bind it to localhost or a private interface, never the public network (empty to disable)")
	fs.DurationVar(&flagVals.DrainTimeout, "drain-timeout", flagVals.DrainTimeout, "on SIGTERM/SIGINT, how long to wait for already-admitted writes to finish before closing storage anyway - new writes are rejected immediately either way, and storage stays crash-consistent even when the deadline is hit (the WAL/delta replay path covers whatever didn't get flushed)")
	fs.StringVar(&flagVals.LogLevel, "log-level", flagVals.LogLevel, "minimum log level: debug, info, warn, error")
	fs.StringVar(&flagVals.LogFormat, "log-format", flagVals.LogFormat, "log output format: text or json")
	fs.BoolVar(&showVersion, "version", false, "print version and exit")
	_ = fs.Parse(os.Args[1:])

	if showVersion {
		fmt.Println(version.Version)
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
	rbac, memoryLimitMB, abortAfter, drainTimeout := cfg.RBAC, cfg.MemoryLimitMB, cfg.AbortAfter, cfg.DrainTimeout

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
	}

	var rt *embed.EmbeddedKdbRuntime
	if memory {
		catalog := embed.CatalogFromNamespace(namespace)
		rt, err = embed.OpenMemoryRuntime(catalog, namespace, schema.None())
	} else {
		catalog := embed.CatalogFromNamespace(namespace)
		rt, err = embed.OpenFileRuntime(dataDir, catalog, namespace, schema.None())
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	srv := server.NewKdbServerRuntime(rt)

	memoryLimitStatus := "disabled"
	if memoryLimitMB > 0 {
		limitBytes := uint64(memoryLimitMB) * 1024 * 1024
		srv.SetMemoryLimit(limitBytes, 0.85)
		memoryLimitStatus = fmt.Sprintf("%dMB (reject at 85%%)", memoryLimitMB)
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
	slog.Info("KDB service started",
		"version", version.Version,
		"peer", peerStatus,
		"stream", streamStatus,
		"sql", sqlStatus,
		"admin", adminStatus,
		"tls", tlsStatus,
		"rbac", rbacStatus,
		"memory_limit", memoryLimitStatus,
		"abort_after", abortStatus,
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
	srv.Release()
	slog.Info("shutdown complete")
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
