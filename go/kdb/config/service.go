package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/storage"
)

// ServiceSettings is kdb-service's fully-resolved effective configuration - one field per
// command-line flag. Zero value is not meaningful on its own; start from DefaultServiceSettings
// and overlay via ResolveService.
type ServiceSettings struct {
	DataDir       string
	Memory        bool
	Namespace     string
	SQLAddr       string
	PeerAddr      string
	StreamAddr    string
	AdminAddr     string
	RBAC          bool
	MemoryLimitMB int
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
}

// DefaultServiceSettings mirrors the flag defaults declared in cmd/kdb-service.
func DefaultServiceSettings() ServiceSettings {
	return ServiceSettings{
		Namespace:    "demo/users",
		SQLAddr:      "tcp://127.0.0.1:9090?bind=true",
		PeerAddr:     "tcp://127.0.0.1:9091?bind=true",
		StreamAddr:   "tcp://127.0.0.1:9092?bind=true",
		DrainTimeout: 30 * time.Second,
		LogLevel:     "info",
		LogFormat:    "text",
		// sync: an acknowledged write is on disk. Group commit (see
		// embed.commitLogWriter) amortizes the fsync across concurrent writers,
		// so this is the safe default without being the slow one it used to be.
		Durability:          "sync",
		AsyncSyncIntervalMS: 5,
		Compression:         "zstd",
	}
}

// ServiceFile is the JSON shape of a kdb-service config file (--config). Every field is
// optional - pointer fields distinguish "absent" from a real zero value, so a file can set
// exactly the fields it cares about and leave the rest to env/flags/defaults. Durations are Go
// duration strings ("30s", "2m"). Unknown fields are rejected, so a typo fails loudly at
// startup instead of silently configuring nothing.
type ServiceFile struct {
	DataDir       *string         `json:"dataDir"`
	Memory        *bool           `json:"memory"`
	Namespace     *string         `json:"namespace"`
	SQLAddr       *string         `json:"sqlAddr"`
	PeerAddr      *string         `json:"peerAddr"`
	StreamAddr    *string         `json:"streamAddr"`
	AdminAddr     *string         `json:"adminAddr"`
	RBAC          *bool           `json:"rbac"`
	MemoryLimitMB *int            `json:"memoryLimitMb"`
	AbortAfter    *string         `json:"abortAfter"`
	DrainTimeout  *string         `json:"drainTimeout"`
	TLS           *ServiceTLSFile `json:"tls"`
	LogLevel      *string         `json:"logLevel"`
	LogFormat     *string         `json:"logFormat"`

	Durability          *string `json:"durability"`
	AsyncSyncIntervalMS *int    `json:"asyncSyncIntervalMs"`
	Compression         *string `json:"compression"`
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

	// Layer 2: config file.
	if file != nil {
		setIf(&s.DataDir, file.DataDir)
		setIf(&s.Memory, file.Memory)
		setIf(&s.Namespace, file.Namespace)
		setIf(&s.SQLAddr, file.SQLAddr)
		setIf(&s.PeerAddr, file.PeerAddr)
		setIf(&s.StreamAddr, file.StreamAddr)
		setIf(&s.AdminAddr, file.AdminAddr)
		setIf(&s.RBAC, file.RBAC)
		setIf(&s.MemoryLimitMB, file.MemoryLimitMB)
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
	envString("KDB_ADMIN_ADDR", &s.AdminAddr)
	if err := envBool("KDB_RBAC", &s.RBAC); err != nil {
		return s, err
	}
	if err := envInt("KDB_MEMORY_LIMIT_MB", &s.MemoryLimitMB); err != nil {
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
		{"admin-addr", func() { s.AdminAddr = flags.AdminAddr }},
		{"rbac", func() { s.RBAC = flags.RBAC }},
		{"memory-limit-mb", func() { s.MemoryLimitMB = flags.MemoryLimitMB }},
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
	}
	for _, o := range flagOverrides {
		if flagWasSet(o.name) {
			o.apply()
		}
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
