package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// ServiceSettings is kdb-service's fully-resolved effective configuration - one field per
// command-line flag. Zero value is not meaningful on its own; start from DefaultServiceSettings
// and overlay via ResolveService.
type ServiceSettings struct {
	DataDir    string
	Memory     bool
	Namespace  string
	SQLAddr    string
	PeerAddr   string
	StreamAddr string
	// WSAddr is the WebSocket SQL-wire listen address (ws:// or wss://). Empty disables it.
	//
	// Separate from SQLAddr rather than a scheme variant of it because a deployment commonly
	// wants both at once: native clients on raw TCP, browsers on WebSocket, same server.
	WSAddr    string
	AdminAddr string
	RBAC      bool
	// MemoryBudgetMB is the memory budget the pressure zones and grant capacity are computed
	// against. Three-valued on purpose: -1 disables resource governance entirely, 0 (the
	// default) auto-detects via server.DetectMemoryBudgetBytes, and any positive value is an
	// explicit budget in MiB.
	//
	// Auto-detect is the default because the previous default - off - meant the one mechanism
	// standing between sustained write load and an OOM kill was inert unless an operator already
	// knew to ask for it. Nothing in the Dockerfile, the systemd unit, or these defaults turned
	// it on.
	MemoryBudgetMB int
	// MemoryLimitMB is the deprecated alias for MemoryBudgetMB (kdb-spec-layer13 §13). Retained
	// so existing configs keep working; when set, it supplies the budget, and its old "0 means
	// disabled" meaning is preserved by mapping an explicit 0 to -1.
	MemoryLimitMB int
	// MemoryReserveMB is the rescue reserve held back for the abort sequence (§5.6).
	MemoryReserveMB int
	// MaxConnections caps concurrently-accepted connections per listener (§6.5); 0 is unlimited.
	MaxConnections int
	// ScanRowBudget caps rows examined per scan (§5.2), not merely rows returned.
	ScanRowBudget int
	AbortAfter    time.Duration
	DrainTimeout  time.Duration
	TLSCert       string
	TLSKey        string
	TLSCA         string
	TLSClientAuth bool
	LogLevel      string
	LogFormat     string

	// Storage-engine tunables. These reach storage.StorageEngineConfig via
	// embed.FileRuntimeOptions; before they existed the engine's Durability and
	// CompressionCodec were hardcoded at embed/file.go's construction site and
	// unreachable from any config surface, despite the engine already honoring
	// them.
	Durability          string
	AsyncSyncIntervalMS int
	Compression         string
	SyncMode            string
}

// DefaultServiceSettings mirrors the flag defaults declared in cmd/kdb-service.
func DefaultServiceSettings() ServiceSettings {
	return ServiceSettings{
		Namespace:  "demo/users",
		SQLAddr:    "tcp://127.0.0.1:9090?bind=true",
		PeerAddr:   "tcp://127.0.0.1:9091?bind=true",
		StreamAddr: "tcp://127.0.0.1:9092?bind=true",
		// Off by default: a browser-reachable port is a deliberate exposure decision, not
		// something an operator should discover already listening.
		WSAddr:       "",
		DrainTimeout: 30 * time.Second,
		LogLevel:     "info",
		LogFormat:    "text",
		// 0 = auto-detect the budget rather than run ungoverned - see MemoryBudgetMB.
		MemoryBudgetMB:  0,
		MemoryReserveMB: int(server.DefaultRescueReserveBytes >> 20),
		MaxConnections:  server.DefaultMaxConnections,
		ScanRowBudget:   int(server.DefaultScanRowBudget),
		// sync: an acknowledged write is on disk. Group commit (see
		// embed.commitLogWriter) amortizes the fsync across concurrent writers,
		// so this is the safe default without being the slow one it used to be.
		Durability:          "sync",
		AsyncSyncIntervalMS: 5,
		Compression:         "zstd",
		// full: every physical sync forces the drive cache to media
		// (F_FULLFSYNC on darwin). "fast" keeps process/OS-crash durability at
		// a fraction of the cost - see storage/io.SyncMode.
		SyncMode: "full",
	}
}

// ServiceFile is the JSON shape of a kdb-service config file (--config). Every field is
// optional - pointer fields distinguish "absent" from a real zero value, so a file can set
// exactly the fields it cares about and leave the rest to env/flags/defaults. Durations are Go
// duration strings ("30s", "2m"). Unknown fields are rejected, so a typo fails loudly at
// startup instead of silently configuring nothing.
type ServiceFile struct {
	DataDir         *string         `json:"dataDir"`
	Memory          *bool           `json:"memory"`
	Namespace       *string         `json:"namespace"`
	SQLAddr         *string         `json:"sqlAddr"`
	PeerAddr        *string         `json:"peerAddr"`
	StreamAddr      *string         `json:"streamAddr"`
	WSAddr          *string         `json:"wsAddr"`
	AdminAddr       *string         `json:"adminAddr"`
	RBAC            *bool           `json:"rbac"`
	MemoryBudgetMB  *int            `json:"memoryBudgetMb"`
	MemoryLimitMB   *int            `json:"memoryLimitMb"`
	MemoryReserveMB *int            `json:"memoryReserveMb"`
	MaxConnections  *int            `json:"maxConnections"`
	ScanRowBudget   *int            `json:"scanRowBudget"`
	AbortAfter      *string         `json:"abortAfter"`
	DrainTimeout    *string         `json:"drainTimeout"`
	TLS             *ServiceTLSFile `json:"tls"`
	LogLevel        *string         `json:"logLevel"`
	LogFormat       *string         `json:"logFormat"`

	Durability          *string `json:"durability"`
	AsyncSyncIntervalMS *int    `json:"asyncSyncIntervalMs"`
	Compression         *string `json:"compression"`
	SyncMode            *string `json:"syncMode"`
}

// ServiceTLSFile is ServiceFile's nested TLS block, matching the --tls-* flags.
type ServiceTLSFile struct {
	CertFile   *string `json:"certFile"`
	KeyFile    *string `json:"keyFile"`
	CAFile     *string `json:"caFile"`
	ClientAuth *bool   `json:"clientAuth"`
}

// LoadServiceFile reads and parses a --config JSON file, rejecting unknown fields.
func LoadServiceFile(path string) (*ServiceFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config file: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var f ServiceFile
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("config file %s: %w", path, err)
	}
	return &f, nil
}

// ResolveService merges the four configuration sources in ascending precedence -
// defaults < config file < environment (KDB_*) < explicitly-set flags - and returns the
// effective settings (kdb-finish-up-plan Phase 2.6).
//
// file may be nil (no --config given). lookupEnv is os.LookupEnv in production, injectable for
// tests. flagWasSet reports whether the named flag (its flag.FlagSet name, e.g. "sql-addr") was
// present on the command line; flags holds every flag's parsed value. Flag values only override
// when explicitly set - otherwise a flag's default would always mask the file and environment,
// which is the standard trap this signature exists to avoid.
func ResolveService(file *ServiceFile, lookupEnv func(string) (string, bool), flagWasSet func(string) bool, flags ServiceSettings) (ServiceSettings, error) {
	s := DefaultServiceSettings()
	// Tracks whether the budget was named explicitly, and by which spelling. The deprecated
	// --memory-limit-mb only folds into the budget when the modern key was not also given, and
	// its historical "0 means disabled" reading has to survive the fold - see the reconciliation
	// after the flag layer below.
	var budgetSet, limitSet bool

	// Layer 2: config file.
	if file != nil {
		setIf(&s.DataDir, file.DataDir)
		setIf(&s.Memory, file.Memory)
		setIf(&s.Namespace, file.Namespace)
		setIf(&s.SQLAddr, file.SQLAddr)
		setIf(&s.PeerAddr, file.PeerAddr)
		setIf(&s.StreamAddr, file.StreamAddr)
		setIf(&s.WSAddr, file.WSAddr)
		setIf(&s.AdminAddr, file.AdminAddr)
		setIf(&s.RBAC, file.RBAC)
		setIf(&s.MemoryBudgetMB, file.MemoryBudgetMB)
		setIf(&s.MemoryLimitMB, file.MemoryLimitMB)
		setIf(&s.MemoryReserveMB, file.MemoryReserveMB)
		setIf(&s.MaxConnections, file.MaxConnections)
		setIf(&s.ScanRowBudget, file.ScanRowBudget)
		budgetSet = budgetSet || file.MemoryBudgetMB != nil
		limitSet = limitSet || file.MemoryLimitMB != nil
		if err := setDurationIf(&s.AbortAfter, file.AbortAfter, "abortAfter"); err != nil {
			return s, err
		}
		if err := setDurationIf(&s.DrainTimeout, file.DrainTimeout, "drainTimeout"); err != nil {
			return s, err
		}
		if file.TLS != nil {
			setIf(&s.TLSCert, file.TLS.CertFile)
			setIf(&s.TLSKey, file.TLS.KeyFile)
			setIf(&s.TLSCA, file.TLS.CAFile)
			setIf(&s.TLSClientAuth, file.TLS.ClientAuth)
		}
		setIf(&s.LogLevel, file.LogLevel)
		setIf(&s.LogFormat, file.LogFormat)
		setIf(&s.Durability, file.Durability)
		setIf(&s.AsyncSyncIntervalMS, file.AsyncSyncIntervalMS)
		setIf(&s.Compression, file.Compression)
		setIf(&s.SyncMode, file.SyncMode)
	}

	// Layer 3: environment.
	envErr := func(name, val string, err error) error {
		return fmt.Errorf("environment %s=%q: %v", name, val, err)
	}
	envString := func(name string, dst *string) {
		if v, ok := lookupEnv(name); ok {
			*dst = v
		}
	}
	envBool := func(name string, dst *bool) error {
		v, ok := lookupEnv(name)
		if !ok {
			return nil
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return envErr(name, v, err)
		}
		*dst = b
		return nil
	}
	envInt := func(name string, dst *int) error {
		v, ok := lookupEnv(name)
		if !ok {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return envErr(name, v, err)
		}
		*dst = n
		return nil
	}
	envDuration := func(name string, dst *time.Duration) error {
		v, ok := lookupEnv(name)
		if !ok {
			return nil
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return envErr(name, v, err)
		}
		*dst = d
		return nil
	}
	envString("KDB_DATA_DIR", &s.DataDir)
	if err := envBool("KDB_MEMORY", &s.Memory); err != nil {
		return s, err
	}
	envString("KDB_NAMESPACE", &s.Namespace)
	envString("KDB_SQL_ADDR", &s.SQLAddr)
	envString("KDB_PEER_ADDR", &s.PeerAddr)
	envString("KDB_STREAM_ADDR", &s.StreamAddr)
	envString("KDB_WS_ADDR", &s.WSAddr)
	envString("KDB_ADMIN_ADDR", &s.AdminAddr)
	if err := envBool("KDB_RBAC", &s.RBAC); err != nil {
		return s, err
	}
	if err := envInt("KDB_MEMORY_BUDGET_MB", &s.MemoryBudgetMB); err != nil {
		return s, err
	}
	if _, ok := lookupEnv("KDB_MEMORY_BUDGET_MB"); ok {
		budgetSet = true
	}
	if err := envInt("KDB_MEMORY_LIMIT_MB", &s.MemoryLimitMB); err != nil {
		return s, err
	}
	if _, ok := lookupEnv("KDB_MEMORY_LIMIT_MB"); ok {
		limitSet = true
	}
	if err := envInt("KDB_MEMORY_RESERVE_MB", &s.MemoryReserveMB); err != nil {
		return s, err
	}
	if err := envInt("KDB_MAX_CONNECTIONS", &s.MaxConnections); err != nil {
		return s, err
	}
	if err := envInt("KDB_SCAN_ROW_BUDGET", &s.ScanRowBudget); err != nil {
		return s, err
	}
	if err := envDuration("KDB_ABORT_AFTER", &s.AbortAfter); err != nil {
		return s, err
	}
	if err := envDuration("KDB_DRAIN_TIMEOUT", &s.DrainTimeout); err != nil {
		return s, err
	}
	envString("KDB_TLS_CERT", &s.TLSCert)
	envString("KDB_TLS_KEY", &s.TLSKey)
	envString("KDB_TLS_CA", &s.TLSCA)
	if err := envBool("KDB_TLS_CLIENT_AUTH", &s.TLSClientAuth); err != nil {
		return s, err
	}
	envString("KDB_LOG_LEVEL", &s.LogLevel)
	envString("KDB_LOG_FORMAT", &s.LogFormat)
	envString("KDB_DURABILITY", &s.Durability)
	if err := envInt("KDB_ASYNC_SYNC_INTERVAL_MS", &s.AsyncSyncIntervalMS); err != nil {
		return s, err
	}
	envString("KDB_COMPRESSION", &s.Compression)
	envString("KDB_SYNC_MODE", &s.SyncMode)

	// Layer 4: explicitly-set flags.
	flagOverrides := []struct {
		name  string
		apply func()
	}{
		{"data-dir", func() { s.DataDir = flags.DataDir }},
		{"memory", func() { s.Memory = flags.Memory }},
		{"namespace", func() { s.Namespace = flags.Namespace }},
		{"sql-addr", func() { s.SQLAddr = flags.SQLAddr }},
		{"peer-addr", func() { s.PeerAddr = flags.PeerAddr }},
		{"stream-addr", func() { s.StreamAddr = flags.StreamAddr }},
		{"ws-addr", func() { s.WSAddr = flags.WSAddr }},
		{"admin-addr", func() { s.AdminAddr = flags.AdminAddr }},
		{"rbac", func() { s.RBAC = flags.RBAC }},
		{"memory-budget-mb", func() { s.MemoryBudgetMB = flags.MemoryBudgetMB; budgetSet = true }},
		{"memory-limit-mb", func() { s.MemoryLimitMB = flags.MemoryLimitMB; limitSet = true }},
		{"memory-reserve-mb", func() { s.MemoryReserveMB = flags.MemoryReserveMB }},
		{"max-connections", func() { s.MaxConnections = flags.MaxConnections }},
		{"scan-row-budget", func() { s.ScanRowBudget = flags.ScanRowBudget }},
		{"abort-after", func() { s.AbortAfter = flags.AbortAfter }},
		{"drain-timeout", func() { s.DrainTimeout = flags.DrainTimeout }},
		{"tls-cert", func() { s.TLSCert = flags.TLSCert }},
		{"tls-key", func() { s.TLSKey = flags.TLSKey }},
		{"tls-ca", func() { s.TLSCA = flags.TLSCA }},
		{"tls-client-auth", func() { s.TLSClientAuth = flags.TLSClientAuth }},
		{"log-level", func() { s.LogLevel = flags.LogLevel }},
		{"log-format", func() { s.LogFormat = flags.LogFormat }},
		{"durability", func() { s.Durability = flags.Durability }},
		{"async-sync-interval-ms", func() { s.AsyncSyncIntervalMS = flags.AsyncSyncIntervalMS }},
		{"compression", func() { s.Compression = flags.Compression }},
		{"sync-mode", func() { s.SyncMode = flags.SyncMode }},
	}
	for _, o := range flagOverrides {
		if flagWasSet(o.name) {
			o.apply()
		}
	}
	// Reconcile the deprecated --memory-limit-mb with --memory-budget-mb. The modern key wins
	// when both are given; otherwise the old one supplies the budget, and its historical meaning
	// is preserved exactly: under the old flag 0 meant "no governance", so an explicit 0 folds to
	// -1 (disabled) rather than to the new 0, which now means "auto-detect". Someone who wrote 0
	// meant off, and a silent upgrade to "on, with a budget we picked" would be the kind of
	// surprise a deprecation alias exists to prevent.
	if limitSet && !budgetSet {
		if s.MemoryLimitMB <= 0 {
			s.MemoryBudgetMB = -1
		} else {
			s.MemoryBudgetMB = s.MemoryLimitMB
		}
	}
	if s.MemoryBudgetMB < -1 {
		return s, fmt.Errorf("memory-budget-mb must be -1 (disabled), 0 (auto-detect), or a positive size in MiB, got %d", s.MemoryBudgetMB)
	}
	if s.MemoryReserveMB < 0 {
		return s, fmt.Errorf("memory-reserve-mb must be >= 0, got %d", s.MemoryReserveMB)
	}
	if s.MaxConnections < 0 {
		return s, fmt.Errorf("max-connections must be >= 0 (0 means unlimited), got %d", s.MaxConnections)
	}
	if s.ScanRowBudget < 0 {
		return s, fmt.Errorf("scan-row-budget must be >= 0 (0 means unlimited), got %d", s.ScanRowBudget)
	}

	// Validated once, here, rather than at the point of use: a typo in a
	// durability or compression name should fail at startup with the name it
	// saw, not silently fall back to a default the operator didn't ask for.
	if _, err := ParseDurability(s.Durability); err != nil {
		return s, err
	}
	if _, err := ParseCompression(s.Compression); err != nil {
		return s, err
	}
	if _, err := ParseSyncMode(s.SyncMode); err != nil {
		return s, err
	}
	return s, nil
}

// ParseDurability maps a configured durability name onto the engine's enum.
//
//   - sync:   an acknowledged write is fsynced. Concurrent commits share the
//     fsync via group commit, so this no longer costs a physical sync per write.
//   - async:  acknowledged once the record is in memory and queued; a crash can
//     lose whatever had not been flushed yet.
//   - memory: nothing is written to the delta log at all. Everything is lost on
//     restart - for tests and throwaway workloads only.
func ParseDurability(name string) (storage.Durability, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "sync":
		return storage.DurabilitySync, nil
	case "async":
		return storage.DurabilityAsync, nil
	case "memory", "memory-only", "memory_only":
		return storage.DurabilityMemoryOnly, nil
	default:
		return 0, fmt.Errorf("unknown durability %q (want sync, async, or memory)", name)
	}
}

// ParseCompression maps a configured codec name onto the engine's enum. Since
// the v2 KDBP page format records the codec per frame, changing this affects
// only newly-written frames - previously-written segments stay readable.
func ParseCompression(name string) (storage.CompressionCodec, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "zstd":
		return storage.CompressionZSTD, nil
	case "none", "off":
		return storage.CompressionNone, nil
	default:
		return 0, fmt.Errorf("unknown compression codec %q (want zstd or none)", name)
	}
}

// ParseSyncMode maps a configured sync-mode name onto the IO layer's enum.
//
//   - full: a physical sync forces data to media (F_FULLFSYNC on darwin) -
//     survives power loss. ~4ms per sync on Apple SSDs.
//   - fast: a physical sync reaches the storage device but not necessarily
//     media (F_BARRIERFSYNC on darwin, fdatasync on linux) - survives process
//     and OS crashes; power loss can lose what the drive cache held. The
//     tradeoff SQLite and PostgreSQL default to on macOS.
func ParseSyncMode(name string) (storio.SyncMode, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "full":
		return storio.SyncModeFull, nil
	case "fast", "barrier":
		return storio.SyncModeFast, nil
	default:
		return 0, fmt.Errorf("unknown sync mode %q (want full or fast)", name)
	}
}

func setIf[T any](dst *T, src *T) {
	if src != nil {
		*dst = *src
	}
}

func setDurationIf(dst *time.Duration, src *string, field string) error {
	if src == nil {
		return nil
	}
	d, err := time.ParseDuration(*src)
	if err != nil {
		return fmt.Errorf("config file %s=%q: %v", field, *src, err)
	}
	*dst = d
	return nil
}
