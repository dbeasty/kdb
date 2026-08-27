package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/transport/core"
)

// KdbServiceMain is a skeleton service entrypoint mirroring dev.kdb.service.KdbServiceMain.
func main() {
	fs := flag.NewFlagSet("kdb-service", flag.ExitOnError)
	var (
		dataDir   string
		memory    bool
		namespace string
	)
	var sqlAddr string
	var peerAddr string
	var streamAddr string
	var rbac bool
	var memoryLimitMB int
	var abortAfter time.Duration
	var tlsCert, tlsKey, tlsCA string
	var tlsClientAuth bool
	fs.StringVar(&dataDir, "data-dir", "", "filesystem data root")
	fs.BoolVar(&memory, "memory", false, "use in-memory runtime")
	fs.StringVar(&namespace, "namespace", "demo/users", "default namespace")
	fs.StringVar(&sqlAddr, "sql-addr", "tcp://127.0.0.1:9090?bind=true", "SQL wire listen address (empty to disable)")
	fs.StringVar(&peerAddr, "peer-addr", "tcp://127.0.0.1:9091?bind=true", "peer sync (Mode 3 full-peer) wire listen address (empty to disable)")
	fs.StringVar(&streamAddr, "stream-addr", "tcp://127.0.0.1:9092?bind=true", "stream (Mode 1 read-only / Mode 2 write-back) wire listen address (empty to disable)")
	fs.BoolVar(&rbac, "rbac", false, "enable RBAC (in-memory user/role registry - create users via the Go API; no admin SQL surface yet)")
	fs.IntVar(&memoryLimitMB, "memory-limit-mb", 0, "reject new writes (rather than risk an OS OOM-kill under sustained load) once process memory nears this budget; rejection triggers at 85% of this value, so set this to 60-80% of the container's actual --memory limit - 80% carries no throughput cost over 60% but do not go above 80% until kdb-spec-layer13 Component 48's full admission control lands, since a burst of already-admitted writes between the guard's periodic samples can still outrun a trip point that close to the real ceiling - see docs/benchmarks/lightsail-sim/README.md for the numbers behind that guidance. 0 disables (default)")
	fs.DurationVar(&abortAfter, "abort-after", 0, "if memory pressure (see --memory-limit-mb) stays tripped for at least this long with no recovery, perform an orderly shutdown (stop accepting new work, flush/seal storage, exit 75) instead of staying up indefinitely rejecting writes - see kdb-spec-layer13 Component 50. Requires a process supervisor (Docker --restart=on-failure, systemd Restart=on-failure) to actually restart the service; this process never restarts itself. 0 disables (default) - this should be rare enough in practice that leaving it off is a reasonable default until you have evidence otherwise")
	fs.StringVar(&tlsCert, "tls-cert", "", "PEM certificate file - set together with --tls-key to require TLS on the SQL/peer-sync/stream listeners (each --*-addr's scheme is upgraded from tcp:// to tcps:// automatically)")
	fs.StringVar(&tlsKey, "tls-key", "", "PEM private key file, paired with --tls-cert")
	fs.StringVar(&tlsCA, "tls-ca", "", "PEM CA bundle to verify client certificates against - required by --tls-client-auth, optional (accept-but-don't-require) otherwise")
	fs.BoolVar(&tlsClientAuth, "tls-client-auth", false, "require and verify a client certificate on every TLS connection (mTLS) - requires --tls-ca")
	_ = fs.Parse(os.Args[1:])

	if dataDir == "" && !memory {
		memory = true
	}
	if dataDir != "" && memory {
		fmt.Fprintln(os.Stderr, "use either --data-dir or --memory")
		os.Exit(2)
	}

	tlsSettings, err := tlsSettingsFromFlags(tlsCert, tlsKey, tlsCA, tlsClientAuth)
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
		// In-memory registry only for now: file-backed durability (component 38 spec §7 test
		// 9) is proven directly against RegistryAuthStore in go/kdb/auth's own tests, but
		// wiring OpenFileRuntime's delta-log persistence into this CLI flag specifically is
		// left as follow-on plumbing rather than rushed in here.
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
		rbacStatus = "enabled (in-memory registry, no users yet - use the Go API to create one)"
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

	tlsStatus := "disabled (plaintext)"
	if tlsSettings != nil {
		tlsStatus = "enabled"
		if tlsSettings.RequireClientAuth {
			tlsStatus = "enabled (mTLS: client cert required)"
		}
	}
	fmt.Printf("KDB service peer=%s stream=%s sql=%s tls=%s rbac=%s memory-limit=%s abort-after=%s namespace=%s\n", peerStatus, streamStatus, sqlStatus, tlsStatus, rbacStatus, memoryLimitStatus, abortStatus, namespace)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	// An ordinary signal-driven shutdown, not a pressure-triggered abort: stop the watchdog
	// first so it doesn't race this deliberate exit (see AbortWatchdog.Stop's own doc comment).
	watchdog.Stop()
	srv.Release()
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
