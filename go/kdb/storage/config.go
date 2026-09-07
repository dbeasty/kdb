package storage

// Durability selects how strongly a namespace's writes are synced to
// disk before WriteBlob returns. See docs/benchmarks/phase0-baseline.md
// Phase 4 for the throughput/durability tradeoff this exists to make
// explicit and per-namespace rather than a single global default.
type Durability int

const (
	// DurabilitySync fsyncs (via group commit) before every write
	// acknowledgement. Default; matches pre-Phase-4 behavior.
	DurabilitySync Durability = iota
	// DurabilityAsync acknowledges writes once appended to the WAL in
	// memory, syncing on a background timer instead of per-write. A
	// crash can lose up to one sync interval of acknowledged writes.
	DurabilityAsync
	// DurabilityMemoryOnly never syncs the WAL to disk; durability is
	// whatever periodic checkpointing the caller layers on top. Intended
	// for namespaces that treat data as reconstructable/ephemeral.
	DurabilityMemoryOnly
)

// StorageEngineConfig bundles runtime limits with a platform I/O shim.
type StorageEngineConfig struct {
	PageTargetSizeBytes int64
	PageMaxSizeBytes    int64
	// GlobalMemoryBudgetBytes, if > 0, is used directly as the hot-tier
	// byte budget (block cache + memtable sizing). If <= 0, it is
	// resolved from HotTierMemory instead (see ResolveHotTierBytes) -
	// small by default, configurable via an absolute value or a
	// percentage of total system memory. Set this field directly only
	// when you want to bypass that resolution entirely.
	GlobalMemoryBudgetBytes int64
	// HotTierMemory configures the hot-tier budget when
	// GlobalMemoryBudgetBytes is left at zero. See Phase 5 of
	// docs/benchmarks/phase0-baseline.md.
	HotTierMemory         HotTierMemoryConfig
	CompressionCodec      CompressionCodec
	DefaultIndexRetention IndexRetention
	IOShim                PlatformIOShim
	// Durability controls WAL sync behavior for this namespace. Zero
	// value is DurabilitySync, preserving prior behavior.
	Durability Durability
	// AsyncSyncIntervalMillis is the background fsync period used when
	// Durability is DurabilityAsync. Zero uses a built-in default.
	AsyncSyncIntervalMillis int64
	// WalMaxSegmentBytes caps the active WAL segment; a write that would
	// exceed it rotates to a new segment first. Zero uses the spec
	// default (64 MiB). See docs/kdb-spec-layer4a-component10a-wal.md.
	WalMaxSegmentBytes int64
	// WalSkipCorruptRecords makes recovery skip records that fail their
	// checksum instead of failing the whole replay. Default false.
	WalSkipCorruptRecords bool
	// HistoryStrategy decides how reads at historical commits are served
	// after a restart - see HistoryStrategy. Resolved from the namespace's
	// own marker at open, so the zero value here is normal.
	HistoryStrategy HistoryStrategy
	// DocumentCacheBytes caps how many bytes of document versions stay
	// resident before the oldest are evicted and re-read on demand. Zero
	// derives it from the hot-tier budget; see ResolvedDocumentCacheBytes.
	DocumentCacheBytes int64
	// CommitOpsBytes caps how many bytes of commit operations the DAG
	// keeps resident. Zero derives it from the hot-tier budget; see
	// ResolvedCommitOpsBytes.
	CommitOpsBytes int64
	// TreeChainLimit is how many delta tree objects may stack up before a
	// full one is written, under the objects history strategy. Zero uses
	// DefaultTreeChainLimit. Lower means cheaper historical reads and more
	// bytes written; higher, the reverse.
	TreeChainLimit int
	// DisableCheckpoints stops this namespace writing the checkpoint that
	// lets the next open skip the delta log. Off by default. Worth turning
	// on to measure or debug the replay path, or where the checkpoint's
	// own write is not wanted; the cost is that every open replays the log
	// in full.
	DisableCheckpoints bool
}

// ResolvedGlobalMemoryBudgetBytes returns GlobalMemoryBudgetBytes if set,
// otherwise resolves HotTierMemory (falling back to DefaultHotTierBytes).
func (c StorageEngineConfig) ResolvedGlobalMemoryBudgetBytes() int64 {
	if c.GlobalMemoryBudgetBytes > 0 {
		return c.GlobalMemoryBudgetBytes
	}
	return ResolveHotTierBytes(c.HotTierMemory)
}
