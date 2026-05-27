package embed

import (
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
}

// FileRuntimeOptionsFromEnv returns options with S3 config from KDB_S3_* env vars.
func FileRuntimeOptionsFromEnv() FileRuntimeOptions {
	return FileRuntimeOptions{S3: s3io.ConfigFromEnv()}
}
