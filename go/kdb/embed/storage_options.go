package embed

import (
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
	// SyncMode selects the physical sync primitive every flush uses. The zero
	// value is storio.SyncModeFull (F_FULLFSYNC on darwin) - the previous
	// hardcoded behavior; storio.SyncModeFast trades power-loss protection for
	// an order-of-magnitude cheaper sync (see the SyncMode docs).
	SyncMode storio.SyncMode
}

// FileRuntimeOptionsFromEnv returns options with S3 config from KDB_S3_* env vars.
func FileRuntimeOptionsFromEnv() FileRuntimeOptions {
	return FileRuntimeOptions{S3: s3io.ConfigFromEnv()}
}
