package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/storage"
)

// This file implements docs/kdb-control-ui-plan.md §7.1/§7.2: a description of every service
// setting, its resolved value, and - the part that does not exist anywhere else - *where that
// value came from*.
//
// ResolveService already knows the precedence (file < env < explicitly-set flags) and already
// takes a flagWasSet predicate; it simply discards the provenance on the way out. Describe re-runs
// the same precedence rules over the same inputs and keeps it. The two must agree: the resolved
// Value on every descriptor is read from the ServiceSettings ResolveService returned, never
// recomputed here, so a divergence in the *value* is impossible by construction and only the
// Source attribution is this file's own responsibility.

// Source names where a setting's running value came from.
type Source string

const (
	SourceDefault    Source = "default"
	SourceConfigFile Source = "file"
	SourceEnv        Source = "env"
	SourceFlag       Source = "flag"
	// SourceAuto is a value the process derived at startup rather than read - today only the
	// memory budget, which auto-detects the cgroup limit (or 75% of host RAM) when left at 0.
	SourceAuto Source = "auto"
	// SourceRuntime is a value changed through the control plane since startup. Set by the
	// caller that applied the change, not by Describe.
	SourceRuntime Source = "runtime"
)

// Mutability says whether a setting can be changed on a running server, and at what cost. See
// the plan's §7.1 table - every class here is derived from what the code actually supports, not
// from what would be convenient.
type Mutability string

const (
	// MutabilityLive has a setter documented safe to call while serving traffic.
	MutabilityLive Mutability = "live"
	// MutabilityNewConnections takes effect only for connections or listeners created after the
	// change - e.g. MaxConnections, which is copied into the transport options at listener
	// construction (server/wire_listen.go, server/ws_listen.go).
	MutabilityNewConnections Mutability = "live-new-connections"
	// MutabilityNamespaceReopen needs the namespace closed and reopened, which embed.Host can do
	// without stopping the process.
	MutabilityNamespaceReopen Mutability = "namespace-reopen"
	// MutabilityRestart is startup-only.
	MutabilityRestart Mutability = "restart"
	// MutabilityImmutable disagrees with what is already on disk and is refused at open rather
	// than ignored - changing it is a migration (kdb-inspect migrate-history), not a setting.
	MutabilityImmutable Mutability = "immutable"
)

// Scope says what a setting applies to.
type Scope string

const (
	ScopeProcess   Scope = "process"
	ScopeNamespace Scope = "namespace"
	ScopeListener  Scope = "listener"
)

// SettingDescriptor is one setting as an operator needs to see it.
type SettingDescriptor struct {
	Key string `json:"key"`
	// Value is the resolved, running value. Read from the ServiceSettings that ResolveService
	// produced - never recomputed - so it cannot disagree with what the process is using.
	Value any `json:"value"`
	// Default is what the value would be with no file, no environment and no flags.
	Default any `json:"default"`
	// Source and SourceDetail say where Value came from, e.g. ("flag", "--memory-budget-mb") or
	// ("env", "KDB_WS_ADDR").
	Source       Source     `json:"source"`
	SourceDetail string     `json:"sourceDetail"`
	Scope        Scope      `json:"scope"`
	Mutability   Mutability `json:"mutability"`
	Unit         string     `json:"unit,omitempty"`
	// Sensitive marks a value that must be masked in responses and logs. Nothing in
	// ServiceSettings is sensitive today (TLS fields are paths, not key material), but the S3
	// credentials in the environment surface are - see EnvOnlyDescriptors.
	Sensitive bool   `json:"sensitive"`
	Help      string `json:"help"`
	// Warnings carries anything an operator would otherwise never learn - above all, a KDB_*
	// variable that was set and then silently discarded because it would not parse. See
	// EnvOnlyDescriptors.
	Warnings []string `json:"warnings,omitempty"`
}

// settingSpec is the static half of a descriptor: everything that does not depend on the values
// a particular process resolved.
type settingSpec struct {
	key        string
	flag       string
	env        string
	scope      Scope
	mutability Mutability
	unit       string
	help       string
	// value extracts this setting's value from a ServiceSettings.
	value func(ServiceSettings) any
	// inFile reports whether a config file set this setting.
	inFile func(*ServiceFile) bool
}

// serviceSpecs describes every field of ServiceSettings. Adding a field to ServiceSettings
// without adding it here is caught by TestEverySettingIsDescribed.
func serviceSpecs() []settingSpec {
	str := func(get func(ServiceSettings) string) func(ServiceSettings) any {
		return func(s ServiceSettings) any { return get(s) }
	}
	num := func(get func(ServiceSettings) int) func(ServiceSettings) any {
		return func(s ServiceSettings) any { return get(s) }
	}
	dur := func(get func(ServiceSettings) time.Duration) func(ServiceSettings) any {
		return func(s ServiceSettings) any { return get(s).String() }
	}
	yn := func(get func(ServiceSettings) bool) func(ServiceSettings) any {
		return func(s ServiceSettings) any { return get(s) }
	}
	return []settingSpec{
		{
			key: "storage.dataDir", flag: "data-dir", env: "KDB_DATA_DIR",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "filesystem data root",
			value:  str(func(s ServiceSettings) string { return s.DataDir }),
			inFile: func(f *ServiceFile) bool { return f.DataDir != nil },
		},
		{
			key: "storage.memory", flag: "memory", env: "KDB_MEMORY",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "use in-memory runtime (nothing is written to disk)",
			value:  yn(func(s ServiceSettings) bool { return s.Memory }),
			inFile: func(f *ServiceFile) bool { return f.Memory != nil },
		},
		{
			key: "namespace.default", flag: "namespace", env: "KDB_NAMESPACE",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "default namespace the data-plane listeners serve",
			value:  str(func(s ServiceSettings) string { return s.Namespace }),
			inFile: func(f *ServiceFile) bool { return f.Namespace != nil },
		},
		{
			key: "listener.sqlAddr", flag: "sql-addr", env: "KDB_SQL_ADDR",
			scope: ScopeListener, mutability: MutabilityRestart,
			help:   "SQL wire listen address (empty to disable)",
			value:  str(func(s ServiceSettings) string { return s.SQLAddr }),
			inFile: func(f *ServiceFile) bool { return f.SQLAddr != nil },
		},
		{
			key: "listener.peerAddr", flag: "peer-addr", env: "KDB_PEER_ADDR",
			scope: ScopeListener, mutability: MutabilityRestart,
			help:   "peer sync (Mode 3 full-peer) wire listen address (empty to disable)",
			value:  str(func(s ServiceSettings) string { return s.PeerAddr }),
			inFile: func(f *ServiceFile) bool { return f.PeerAddr != nil },
		},
		{
			key: "listener.streamAddr", flag: "stream-addr", env: "KDB_STREAM_ADDR",
			scope: ScopeListener, mutability: MutabilityRestart,
			help:   "stream (Mode 1 read-only / Mode 2 write-back) listen address (empty to disable)",
			value:  str(func(s ServiceSettings) string { return s.StreamAddr }),
			inFile: func(f *ServiceFile) bool { return f.StreamAddr != nil },
		},
		{
			key: "listener.wsAddr", flag: "ws-addr", env: "KDB_WS_ADDR",
			scope: ScopeListener, mutability: MutabilityRestart,
			help:   "WebSocket SQL-wire listen address - the only transport a browser can open",
			value:  str(func(s ServiceSettings) string { return s.WSAddr }),
			inFile: func(f *ServiceFile) bool { return f.WSAddr != nil },
		},
		{
			key: "listener.grpcAddr", flag: "grpc-addr", env: "KDB_GRPC_ADDR",
			scope: ScopeListener, mutability: MutabilityRestart,
			help:   "gRPC SQL-wire listen address - requires the kdb-service-grpc build",
			value:  str(func(s ServiceSettings) string { return s.GRPCAddr }),
			inFile: func(f *ServiceFile) bool { return f.GRPCAddr != nil },
		},
		{
			key: "listener.adminAddr", flag: "admin-addr", env: "KDB_ADMIN_ADDR",
			scope: ScopeListener, mutability: MutabilityRestart,
			help:   "operational HTTP endpoint serving /healthz, /readyz, /metrics, /debug/pprof - no auth, bind it privately",
			value:  str(func(s ServiceSettings) string { return s.AdminAddr }),
			inFile: func(f *ServiceFile) bool { return f.AdminAddr != nil },
		},
		{
			key: "auth.rbac", flag: "rbac", env: "KDB_RBAC",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "enable RBAC (in-memory user/role registry)",
			value:  yn(func(s ServiceSettings) bool { return s.RBAC }),
			inFile: func(f *ServiceFile) bool { return f.RBAC != nil },
		},
		{
			key: "memory.budgetMB", flag: "memory-budget-mb", env: "KDB_MEMORY_BUDGET_MB",
			scope: ScopeProcess, mutability: MutabilityLive, unit: "MiB",
			help:   "memory budget admission control governs against; 0 auto-detects the cgroup/container limit, -1 disables governance",
			value:  num(func(s ServiceSettings) int { return s.MemoryBudgetMB }),
			inFile: func(f *ServiceFile) bool { return f.MemoryBudgetMB != nil },
		},
		{
			key: "memory.limitMB", flag: "memory-limit-mb", env: "KDB_MEMORY_LIMIT_MB",
			scope: ScopeProcess, mutability: MutabilityLive, unit: "MiB",
			help:   "DEPRECATED alias for memory.budgetMB, retained for existing configs; an explicit 0 disables governance",
			value:  num(func(s ServiceSettings) int { return s.MemoryLimitMB }),
			inFile: func(f *ServiceFile) bool { return f.MemoryLimitMB != nil },
		},
		{
			key: "memory.reserveMB", flag: "memory-reserve-mb", env: "KDB_MEMORY_RESERVE_MB",
			scope: ScopeProcess, mutability: MutabilityLive, unit: "MiB",
			help:   "rescue reserve held back from the grant system and released on entry to the Critical pressure zone",
			value:  num(func(s ServiceSettings) int { return s.MemoryReserveMB }),
			inFile: func(f *ServiceFile) bool { return f.MemoryReserveMB != nil },
		},
		{
			key: "governance.maxConnections", flag: "max-connections", env: "KDB_MAX_CONNECTIONS",
			scope: ScopeListener, mutability: MutabilityNewConnections,
			help:   "cap on concurrently-accepted connections per listener; 0 is unlimited. Copied into transport options at listener construction, so a change reaches only listeners created afterwards",
			value:  num(func(s ServiceSettings) int { return s.MaxConnections }),
			inFile: func(f *ServiceFile) bool { return f.MaxConnections != nil },
		},
		{
			key: "governance.scanRowBudget", flag: "scan-row-budget", env: "KDB_SCAN_ROW_BUDGET",
			scope: ScopeProcess, mutability: MutabilityLive, unit: "rows",
			help:   "maximum rows a single scan may examine (not merely return) before RESOURCE_EXHAUSTED; shrinks as memory pressure rises. 0 is unlimited",
			value:  num(func(s ServiceSettings) int { return s.ScanRowBudget }),
			inFile: func(f *ServiceFile) bool { return f.ScanRowBudget != nil },
		},
		{
			key: "governance.abortAfter", flag: "abort-after", env: "KDB_ABORT_AFTER",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "if memory pressure stays tripped this long with no recovery, shut down orderly and exit 75. Requires a process supervisor. 0 disables",
			value:  dur(func(s ServiceSettings) time.Duration { return s.AbortAfter }),
			inFile: func(f *ServiceFile) bool { return f.AbortAfter != nil },
		},
		{
			key: "governance.drainTimeout", flag: "drain-timeout", env: "KDB_DRAIN_TIMEOUT",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "on SIGTERM/SIGINT, how long to wait for admitted writes to finish before closing storage anyway",
			value:  dur(func(s ServiceSettings) time.Duration { return s.DrainTimeout }),
			inFile: func(f *ServiceFile) bool { return f.DrainTimeout != nil },
		},
		{
			key: "tls.certFile", flag: "tls-cert", env: "KDB_TLS_CERT",
			scope: ScopeListener, mutability: MutabilityNewConnections,
			help:   "PEM certificate file - set with tls.keyFile to require TLS on the data-plane listeners",
			value:  str(func(s ServiceSettings) string { return s.TLSCert }),
			inFile: func(f *ServiceFile) bool { return f.TLS != nil && f.TLS.CertFile != nil },
		},
		{
			key: "tls.keyFile", flag: "tls-key", env: "KDB_TLS_KEY",
			scope: ScopeListener, mutability: MutabilityNewConnections,
			help:   "PEM private key file, paired with tls.certFile",
			value:  str(func(s ServiceSettings) string { return s.TLSKey }),
			inFile: func(f *ServiceFile) bool { return f.TLS != nil && f.TLS.KeyFile != nil },
		},
		{
			key: "tls.caFile", flag: "tls-ca", env: "KDB_TLS_CA",
			scope: ScopeListener, mutability: MutabilityNewConnections,
			help:   "PEM CA bundle to verify client certificates against",
			value:  str(func(s ServiceSettings) string { return s.TLSCA }),
			inFile: func(f *ServiceFile) bool { return f.TLS != nil && f.TLS.CAFile != nil },
		},
		{
			key: "tls.clientAuth", flag: "tls-client-auth", env: "KDB_TLS_CLIENT_AUTH",
			scope: ScopeListener, mutability: MutabilityNewConnections,
			help:   "require and verify a client certificate on every TLS connection (mTLS); requires tls.caFile",
			value:  yn(func(s ServiceSettings) bool { return s.TLSClientAuth }),
			inFile: func(f *ServiceFile) bool { return f.TLS != nil && f.TLS.ClientAuth != nil },
		},
		{
			key: "control.addr", flag: "control-addr", env: "KDB_CONTROL_ADDR",
			scope: ScopeListener, mutability: MutabilityRestart,
			help:   "control-plane HTTP listen address serving the JSON API and the control UI (empty to disable). Authenticates every request, unlike the admin endpoint",
			value:  str(func(s ServiceSettings) string { return s.ControlAddr }),
			inFile: func(f *ServiceFile) bool { return f.ControlAddr != nil },
		},
		{
			key: "control.write", flag: "control-write", env: "KDB_CONTROL_WRITE",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "allow the control plane's mutating endpoints. Off by default: enabling the control plane is not by itself a decision to let a browser write to the database",
			value:  yn(func(s ServiceSettings) bool { return s.ControlWrite }),
			inFile: func(f *ServiceFile) bool { return f.ControlWrite != nil },
		},
		{
			key: "control.ui", flag: "control-ui", env: "KDB_CONTROL_UI",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "serve the embedded single-page control UI on the control listener",
			value:  yn(func(s ServiceSettings) bool { return s.ControlUI }),
			inFile: func(f *ServiceFile) bool { return f.ControlUI != nil },
		},
		{
			key: "control.settingsPersist", flag: "control-settings-persist", env: "KDB_CONTROL_SETTINGS_PERSIST",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "allow a setting changed through the control plane to be written back to the config file. Off by default: that file belongs to whoever deploys, not to the server",
			value:  yn(func(s ServiceSettings) bool { return s.ControlSettingsPersist }),
			inFile: func(f *ServiceFile) bool { return f.ControlSettingsPersist != nil },
		},
		{
			key: "control.backupDir", flag: "control-backup-dir", env: "KDB_CONTROL_BACKUP_DIR",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "directory the control plane writes backups to (empty disables them). Keep it off the data volume: a backup sharing a disk with its source is not a backup",
			value:  str(func(s ServiceSettings) string { return s.ControlBackupDir }),
			inFile: func(f *ServiceFile) bool { return f.ControlBackupDir != nil },
		},
		{
			key: "control.stagingDir", flag: "control-staging-dir", env: "KDB_CONTROL_STAGING_DIR",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "directory the control plane restores backups into for inspection (empty disables staged restores). Keep it off the data volume: a restore is most needed exactly when that volume is the problem",
			value:  str(func(s ServiceSettings) string { return s.ControlStagingDir }),
			inFile: func(f *ServiceFile) bool { return f.ControlStagingDir != nil },
		},
		{
			key: "log.level", flag: "log-level", env: "KDB_LOG_LEVEL",
			scope: ScopeProcess, mutability: MutabilityLive,
			help:   "minimum log level: debug, info, warn, error",
			value:  str(func(s ServiceSettings) string { return s.LogLevel }),
			inFile: func(f *ServiceFile) bool { return f.LogLevel != nil },
		},
		{
			key: "log.format", flag: "log-format", env: "KDB_LOG_FORMAT",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help:   "log output format: text or json (the handler is built once at startup)",
			value:  str(func(s ServiceSettings) string { return s.LogFormat }),
			inFile: func(f *ServiceFile) bool { return f.LogFormat != nil },
		},
		{
			key: "storage.durability", flag: "durability", env: "KDB_DURABILITY",
			scope: ScopeNamespace, mutability: MutabilityRestart,
			help:   "how much of the write-out a commit waits for: sync, async, or memory. Deliberately not live-mutable - see the plan's §7.3",
			value:  str(func(s ServiceSettings) string { return s.Durability }),
			inFile: func(f *ServiceFile) bool { return f.Durability != nil },
		},
		{
			key: "storage.asyncSyncIntervalMS", flag: "async-sync-interval-ms", env: "KDB_ASYNC_SYNC_INTERVAL_MS",
			scope: ScopeNamespace, mutability: MutabilityRestart, unit: "ms",
			help:   "background sync period under durability=async; ignored otherwise",
			value:  num(func(s ServiceSettings) int { return s.AsyncSyncIntervalMS }),
			inFile: func(f *ServiceFile) bool { return f.AsyncSyncIntervalMS != nil },
		},
		{
			key: "storage.compression", flag: "compression", env: "KDB_COMPRESSION",
			scope: ScopeNamespace, mutability: MutabilityRestart,
			help:   "codec for newly-written delta frames and SSTable blocks: zstd or none. Each frame records its own codec, so changing this leaves existing segments readable",
			value:  str(func(s ServiceSettings) string { return s.Compression }),
			inFile: func(f *ServiceFile) bool { return f.Compression != nil },
		},
		{
			key: "storage.syncMode", flag: "sync-mode", env: "KDB_SYNC_MODE",
			scope: ScopeNamespace, mutability: MutabilityRestart,
			help:   "physical sync primitive: full (survives power loss) or fast (survives process/OS crash, an order of magnitude cheaper)",
			value:  str(func(s ServiceSettings) string { return s.SyncMode }),
			inFile: func(f *ServiceFile) bool { return f.SyncMode != nil },
		},
	}
}

// Describe returns one SettingDescriptor per service setting, attributing each resolved value to
// the layer that supplied it.
//
// The inputs are exactly ResolveService's, plus the ServiceSettings it returned: this re-runs the
// precedence decision, it does not re-run the resolution. lookupEnv and flagWasSet may be nil,
// which is read as "no environment" and "no flag was set" respectively - the shape a test or an
// embedded caller that never parsed a command line has.
func Describe(file *ServiceFile, lookupEnv func(string) (string, bool), flagWasSet func(string) bool, resolved ServiceSettings) []SettingDescriptor {
	if lookupEnv == nil {
		lookupEnv = func(string) (string, bool) { return "", false }
	}
	if flagWasSet == nil {
		flagWasSet = func(string) bool { return false }
	}
	defaults := DefaultServiceSettings()
	specs := serviceSpecs()
	out := make([]SettingDescriptor, 0, len(specs))
	for _, spec := range specs {
		d := SettingDescriptor{
			Key:        spec.key,
			Value:      spec.value(resolved),
			Default:    spec.value(defaults),
			Scope:      spec.scope,
			Mutability: spec.mutability,
			Unit:       spec.unit,
			Help:       spec.help,
		}
		// The same order ResolveService applies, read backwards: the last layer to write a value
		// is the one that owns it.
		switch {
		case flagWasSet(spec.flag):
			d.Source, d.SourceDetail = SourceFlag, "--"+spec.flag
		case envIsSet(lookupEnv, spec.env):
			d.Source, d.SourceDetail = SourceEnv, spec.env
		case file != nil && spec.inFile(file):
			d.Source, d.SourceDetail = SourceConfigFile, "config file"
		default:
			d.Source, d.SourceDetail = SourceDefault, "built-in default"
		}
		out = append(out, d)
	}
	return out
}

// MarkAutoDetected re-attributes a setting whose value the process derived rather than read. The
// memory budget is the only one today: left at 0 it means "auto-detect", and reporting that as
// "default" would tell an operator nothing about the number actually in force.
//
// actual is the value that was detected, in the descriptor's own unit.
func MarkAutoDetected(descriptors []SettingDescriptor, key string, actual any, detail string) {
	for i := range descriptors {
		if descriptors[i].Key != key {
			continue
		}
		if descriptors[i].Source != SourceDefault {
			// An operator named a value explicitly; auto-detection did not decide this.
			return
		}
		descriptors[i].Source, descriptors[i].SourceDetail = SourceAuto, detail
		descriptors[i].Value = actual
		return
	}
}

func envIsSet(lookupEnv func(string) (string, bool), name string) bool {
	if name == "" {
		return false
	}
	_, ok := lookupEnv(name)
	return ok
}

// envOnlySpec describes one of the KDB_* variables that reach the engine through
// embed.FileRuntimeOptionsFromEnv and have no flag and no config-file field (plan §2.4b).
type envOnlySpec struct {
	key        string
	env        string
	scope      Scope
	mutability Mutability
	unit       string
	sensitive  bool
	help       string
	// parse validates a set value and returns the effective value plus an error describing why a
	// value was discarded. Every one of these variables is read leniently by the engine - an
	// unparseable value silently becomes the default - so this is the only place the fact that a
	// value was thrown away can be surfaced at all.
	parse func(raw string) (any, error)
}

func envOnlySpecs() []envOnlySpec {
	bytes := func(raw string) (any, error) {
		n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil || n < 0 {
			return int64(0), fmt.Errorf("not a non-negative byte count")
		}
		return n, nil
	}
	count := func(raw string) (any, error) {
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || n < 0 {
			return 0, fmt.Errorf("not a non-negative count")
		}
		return n, nil
	}
	onOff := func(raw string) (any, error) {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "on", "true", "yes", "1":
			return true, nil
		case "off", "false", "no", "0", "":
			return false, nil
		}
		// envBool treats everything unrecognised as off, without saying so.
		return false, fmt.Errorf("not one of on/true/yes/1 or off/false/no/0")
	}
	return []envOnlySpec{
		{
			key: "history.strategy", env: "KDB_HISTORY_STRATEGY",
			scope: ScopeNamespace, mutability: MutabilityImmutable,
			help: "how this namespace serves reads at historical commits. Refused at open if it disagrees with what the namespace was built as - changing it is a migration (kdb-inspect migrate-history), not a setting",
			parse: func(raw string) (any, error) {
				s, err := storage.ParseHistoryStrategy(strings.ToLower(strings.TrimSpace(raw)))
				if err != nil {
					return nil, err
				}
				return fmt.Sprint(s), nil
			},
		},
		{
			key: "cache.documentBytes", env: "KDB_DOCUMENT_CACHE_BYTES",
			scope: ScopeNamespace, mutability: MutabilityNamespaceReopen, unit: "bytes",
			help:  "bytes of document versions kept resident before the oldest are evicted. 0 takes half the hot-tier budget",
			parse: bytes,
		},
		{
			key: "cache.commitOpsBytes", env: "KDB_COMMIT_OPS_BYTES",
			scope: ScopeNamespace, mutability: MutabilityLive, unit: "bytes",
			help:  "bytes of commit operations the DAG keeps resident. 0 takes a quarter of the hot-tier budget. Live via InMemoryCommitDag.SetOperationsBudget",
			parse: bytes,
		},
		{
			key: "cache.historyTreeBytes", env: "KDB_HISTORY_TREE_CACHE_BYTES",
			scope: ScopeNamespace, mutability: MutabilityNamespaceReopen, unit: "bytes",
			help:  "bytes of historical document trees kept resident. 0 takes a quarter of the hot-tier budget. The current tree is not subject to it",
			parse: bytes,
		},
		{
			key: "storage.checkpoints", env: "KDB_CHECKPOINTS",
			scope: ScopeNamespace, mutability: MutabilityNamespaceReopen,
			help:  "off disables the checkpoint that lets the next open skip the delta log; every open then replays it in full",
			parse: onOff,
		},
		{
			key: "graph.ancestryPruning", env: "KDB_ANCESTRY_PRUNING",
			scope: ScopeNamespace, mutability: MutabilityNamespaceReopen,
			help:  "give every commit a generation number so ancestry walks can stop descending early. Costs 8 bytes per resident commit",
			parse: onOff,
		},
		{
			key: "graph.file", env: "KDB_GRAPH_FILE",
			scope: ScopeNamespace, mutability: MutabilityNamespaceReopen,
			help:  "map the commit graph from a file instead of holding it in the heap. Accepted and reported but not yet implemented - GraphFileActive reports whether it is doing anything",
			parse: onOff,
		},
		{
			key: "graph.rebuildCommits", env: "KDB_GRAPH_REBUILD_COMMITS",
			scope: ScopeNamespace, mutability: MutabilityNamespaceReopen, unit: "commits",
			help:  "how many commits may accumulate in the resident tail before the graph file is rebuilt. Ignored when graph.file is off",
			parse: count,
		},
		{
			key: "history.anchorInterval", env: "KDB_HISTORY_ANCHOR_INTERVAL",
			scope: ScopeNamespace, mutability: MutabilityNamespaceReopen, unit: "commits",
			help:  "how many commits apart materialized document trees are written, bounding what a cold historical read has to fold. 0 disables anchors",
			parse: count,
		},
		{
			key: "s3.accessKeyID", env: "KDB_S3_ACCESS_KEY_ID",
			scope: ScopeProcess, mutability: MutabilityRestart, sensitive: true,
			help: "S3 replica tier access key",
		},
		{
			key: "s3.secretAccessKey", env: "KDB_S3_SECRET_ACCESS_KEY",
			scope: ScopeProcess, mutability: MutabilityRestart, sensitive: true,
			help: "S3 replica tier secret key",
		},
		{
			key: "s3.bucket", env: "KDB_S3_BUCKET",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help: "S3 replica tier bucket",
		},
		{
			key: "s3.endpoint", env: "KDB_S3_ENDPOINT",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help: "S3 replica tier endpoint (LocalStack, MinIO, or AWS)",
		},
		{
			key: "s3.region", env: "KDB_S3_REGION",
			scope: ScopeProcess, mutability: MutabilityRestart,
			help: "S3 replica tier region",
		},
	}
}

// EnvOnlyDescriptors describes the KDB_* variables that reach the storage engine directly and
// appear in no flag and no config file (plan §2.4b).
//
// The engine reads all of these leniently: an unparseable value is discarded and the default used,
// with no error and no log line. That is a deliberate choice there - for KDB_HISTORY_MODE the
// other reading "starts deleting segments" - and this does not change it. It only stops the fact
// being invisible: a variable that was set and thrown away comes back with the value the engine
// will actually use and a Warning saying what was ignored and why.
func EnvOnlyDescriptors(lookupEnv func(string) (string, bool)) []SettingDescriptor {
	if lookupEnv == nil {
		lookupEnv = func(name string) (string, bool) { return os.LookupEnv(name) }
	}
	specs := envOnlySpecs()
	out := make([]SettingDescriptor, 0, len(specs))
	for _, spec := range specs {
		d := SettingDescriptor{
			Key:          spec.key,
			Scope:        spec.scope,
			Mutability:   spec.mutability,
			Unit:         spec.unit,
			Sensitive:    spec.sensitive,
			Help:         spec.help,
			Source:       SourceDefault,
			SourceDetail: "built-in default",
		}
		raw, ok := lookupEnv(spec.env)
		switch {
		case !ok:
			// Left unset. Value stays nil rather than guessing at the engine's internal default,
			// which is derived from the hot-tier budget for most of these and is not a constant.
		case spec.parse == nil:
			d.Source, d.SourceDetail = SourceEnv, spec.env
			d.Value = raw
		default:
			effective, err := spec.parse(raw)
			if err != nil {
				d.Warnings = append(d.Warnings, fmt.Sprintf(
					"%s was set to %q and ignored (%v); the engine is using its default instead",
					spec.env, raw, err))
			} else {
				d.Source, d.SourceDetail = SourceEnv, spec.env
				d.Value = effective
			}
		}
		if d.Sensitive && d.Value != nil {
			d.Value = "********"
		}
		out = append(out, d)
	}
	return out
}

// Redact returns a copy of d safe to log or return over the wire.
func (d SettingDescriptor) Redact() SettingDescriptor {
	if d.Sensitive && d.Value != nil {
		d.Value = "********"
	}
	return d
}

// ParseLogLevel maps a log-level name onto slog's levels.
//
// Exported so the control plane validates a level change exactly as startup validates the flag -
// one parser, so "warn" cannot be accepted in one place and rejected in the other.
func ParseLogLevel(name string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (want debug, info, warn, or error)", name)
	}
}
