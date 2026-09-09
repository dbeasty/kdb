package embed

import (
	"os"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

// FileRuntimeOptions configures file-backed embedded runtime storage.
type FileRuntimeOptions struct {
	// S3 enables an S3-compatible replica tier (LocalStack, MinIO, or AWS).
	// When nil, S3 is loaded from environment via s3io.ConfigFromEnv if set.
	S3 *s3io.Config
	// ReplicationPolicy controls whether replica failures fail the operation.
	ReplicationPolicy storio.ReplicationPolicy

	// Storage is the engine tuning this runtime opens with. The zero value is
	// the previous hardcoded behavior (DurabilitySync, CompressionZSTD), so
	// callers that don't care can leave it alone.
	Storage StorageOptions

	// ReadOnly opens the data directory for reading only, under a *shared* directory lock, so
	// several reader processes can attach at once alongside (but never during) a single writer's
	// exclusive hold. The runtime creates no WAL and no delta segment writer, and every write
	// path on it returns ErrReadOnly rather than failing somewhere deeper.
	//
	// Requires flock(2), so unix only - see acquireDirLockShared. A read-only runtime observes
	// the writer's commits as of the moment it opened; call Refresh to pick up newer ones.
	ReadOnly bool

	// forceHistoryStrategy skips the check that a namespace is being
	// opened as what it was built as. Reserved for
	// MigrateHistoryStrategy, which holds the exclusive maintenance lock
	// and is in the middle of making the marker true.
	forceHistoryStrategy bool

	// alreadyLocked says the caller already holds this data directory's
	// exclusive maintenance lock, so this open must not try to take it
	// again - flock would refuse a second acquisition, even from the
	// process holding it. Reserved for MigrateHistoryStrategy, which locks
	// the directory and then needs a runtime inside that lock.
	alreadyLocked bool
}

// StorageOptions carries the storage-engine settings a caller may override.
// Kept separate from the engine's own storage.StorageEngineConfig because that
// struct also holds wiring (the IO shim) that OpenFileRuntime owns and callers
// must not set.
type StorageOptions struct {
	// MemoryBudgetBytes caps the hot tier this runtime may hold in memory.
	// Zero keeps the historical default of 64 MiB — fine for one runtime,
	// but an application that opens several runtimes in one process is
	// multiplying that default by each open, and needs to hand each runtime
	// its slice of the real limit instead.
	MemoryBudgetBytes int64

	// Durability decides how much of the write-out a commit waits for. Zero
	// value is storage.DurabilitySync.
	Durability storage.Durability
	// Compression is the codec newly-written frames use. Zero value means
	// "unset" and resolves to storage.CompressionZSTD; the v2 page format
	// records the codec per frame, so changing this leaves existing segments
	// readable.
	Compression *storage.CompressionCodec
	// AsyncSyncIntervalMillis is the background sync period under
	// storage.DurabilityAsync. Zero uses the engine default.
	AsyncSyncIntervalMillis int64
	// HistoryStrategy selects how this namespace serves reads at
	// historical commits - see storage.HistoryStrategy. The zero value
	// means "whatever this namespace already is", which is what any
	// caller that does not care should pass; a value that disagrees with
	// the namespace's recorded strategy is refused at open rather than
	// silently ignored, because the two leave different things on disk.
	HistoryStrategy storage.HistoryStrategy

	// HistoryMode selects how long this namespace keeps the past - see
	// storage.HistoryMode. The zero value means "whatever this namespace
	// already is". Like HistoryStrategy, a value that disagrees with the
	// namespace's recorded mode is refused at open rather than silently
	// ignored, because the two do not leave the same bytes on disk.
	HistoryMode storage.HistoryMode
	// Retain bounds how much of the past storage.HistoryModeNone keeps.
	// Ignored under HistoryModeFull, which keeps everything. The zero
	// value resolves to storage.DefaultRetentionDuration.
	Retain storage.RetentionWindow

	// DocumentCacheBytes caps how many bytes of document versions stay
	// resident before the oldest are evicted and re-read on demand. Zero
	// takes half the hot-tier budget. Raising it trades memory for fewer
	// cold reads; lowering it, the reverse.
	DocumentCacheBytes int64
	// CommitOpsBytes caps how many bytes of commit operations the DAG
	// keeps resident. Zero takes a quarter of the hot-tier budget.
	CommitOpsBytes int64
	// HistoryTreeCacheBytes caps how many bytes of historical document
	// trees stay resident before the least recently used are evicted and
	// obtained again on demand. Zero takes a quarter of the hot-tier
	// budget. The current tree is not subject to it.
	HistoryTreeCacheBytes int64
	// DeltaMaxSegmentBytes caps the active delta segment, after which the
	// writer seals it and starts the next one. It matters beyond file
	// tidiness: retention only ever reclaims *sealed* segments, so without
	// rotation a process that never restarts holds every commit it has made
	// in one segment truncation is required to leave alone. Zero uses
	// delta.DefaultDeltaMaxSegmentBytes (64 MiB).
	DeltaMaxSegmentBytes int64

	// TreeChainLimit is how many delta tree objects may stack up before a
	// full one is written, under the objects history strategy. Zero uses
	// the built-in default of 32.
	TreeChainLimit int
	// DisableCheckpoints stops this namespace writing the checkpoint that
	// lets the next open skip the delta log; every open then replays it in
	// full. Off by default.
	DisableCheckpoints bool

	// Graph selects how much of the on-disk commit graph work is active
	// for this namespace - see docs/kdb-commit-graph-on-disk.md, and
	// dag.GraphSettings for what each field turns on.
	//
	// The zero value is the behaviour that predates all of it, so leaving
	// this alone changes nothing. Everything in here is off by default on
	// purpose: it is history and recovery machinery, the database serves
	// the latest dataset, and none of it may show up in a read benchmark.
	Graph dag.GraphSettings

	// SyncMode selects the physical sync primitive every flush uses. The zero
	// value is storio.SyncModeFull (F_FULLFSYNC on darwin) - the previous
	// hardcoded behavior; storio.SyncModeFast trades power-loss protection for
	// an order-of-magnitude cheaper sync (see the SyncMode docs).
	SyncMode storio.SyncMode
}

// envBytes reads a non-negative byte count, or zero when the variable is
// unset or unparseable. Unparseable is zero rather than an error for the
// same reason KDB_HISTORY_STRATEGY ignores a typo here: this function
// returns no error, and zero means "use the default", which is the safe
// reading of a value nobody can interpret.
func envBytes(name string) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// FileRuntimeOptionsFromEnv returns options with S3 config from KDB_S3_* env
// vars and the history strategy from KDB_HISTORY_STRATEGY.
//
// An unrecognized KDB_HISTORY_STRATEGY is ignored rather than fatal here,
// because this returns no error and a typo must not silently become "open
// this namespace as something else": leaving it unset means "whatever the
// namespace already is", which is the safe reading. Callers that want a
// typo reported should parse it themselves with
// storage.ParseHistoryStrategy.
func FileRuntimeOptionsFromEnv() FileRuntimeOptions {
	opts := FileRuntimeOptions{S3: s3io.ConfigFromEnv()}
	if raw := strings.ToLower(strings.TrimSpace(os.Getenv("KDB_HISTORY_STRATEGY"))); raw != "" {
		if s, err := storage.ParseHistoryStrategy(raw); err == nil {
			opts.Storage.HistoryStrategy = s
		}
	}
	if raw := strings.TrimSpace(os.Getenv("KDB_HISTORY_MODE")); raw != "" {
		// Unparseable is ignored for the same reason KDB_HISTORY_STRATEGY
		// ignores a typo: this returns no error, and "whatever the
		// namespace already is" is the only safe reading of a value
		// nobody can interpret. It is emphatically the safe reading here,
		// where the other answer starts deleting segments.
		if m, err := storage.ParseHistoryMode(raw); err == nil {
			opts.Storage.HistoryMode = m
		}
	}
	if raw := strings.TrimSpace(os.Getenv("KDB_RETAIN_DURATION")); raw != "" {
		if d, err := storage.ParseRetentionDuration(raw); err == nil {
			opts.Storage.Retain.Duration = d
		}
	}
	opts.Storage.Retain.Commits = envBytes("KDB_RETAIN_COMMITS")
	opts.Storage.DocumentCacheBytes = envBytes("KDB_DOCUMENT_CACHE_BYTES")
	opts.Storage.CommitOpsBytes = envBytes("KDB_COMMIT_OPS_BYTES")
	opts.Storage.HistoryTreeCacheBytes = envBytes("KDB_HISTORY_TREE_CACHE_BYTES")
	switch strings.ToLower(strings.TrimSpace(os.Getenv("KDB_CHECKPOINTS"))) {
	case "off", "false", "0":
		opts.Storage.DisableCheckpoints = true
	}
	opts.Storage.Graph = graphSettingsFromEnv()
	return opts
}

// graphSettingsFromEnv reads the commit-graph feature flags.
//
// Every one of them defaults off, so an unset or unparseable variable
// leaves the behaviour that predates this work. Same reading as envBytes
// and KDB_HISTORY_STRATEGY: this returns no error, and "what it did
// before" is the only safe interpretation of a value nobody can parse.
func graphSettingsFromEnv() dag.GraphSettings {
	return dag.GraphSettings{
		AncestryPruning: envBool("KDB_ANCESTRY_PRUNING"),
		FileEnabled:     envBool("KDB_GRAPH_FILE"),
		RebuildCommits:  envInt("KDB_GRAPH_REBUILD_COMMITS"),
		AnchorInterval:  envInt("KDB_HISTORY_ANCHOR_INTERVAL"),
	}
}

// envBool reads an on/off switch, defaulting to off. "on", "true", "yes"
// and "1" enable; everything else, including an unparseable value, leaves
// it off.
func envBool(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "on", "true", "yes", "1":
		return true
	}
	return false
}

// envInt reads a non-negative count, or zero when the variable is unset or
// unparseable - zero meaning "use the default", exactly as in envBytes.
func envInt(name string) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
