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
}

// StorageOptions carries the storage-engine settings a caller may override.
// Kept separate from the engine's own storage.StorageEngineConfig because that
// struct also holds wiring (the IO shim, memory budgets) that OpenFileRuntime
// owns and callers must not set.
type StorageOptions struct {
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
}

// FileRuntimeOptionsFromEnv returns options with S3 config from KDB_S3_* env vars.
func FileRuntimeOptionsFromEnv() FileRuntimeOptions {
	return FileRuntimeOptions{S3: s3io.ConfigFromEnv()}
}
