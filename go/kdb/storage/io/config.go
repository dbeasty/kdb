package io

// SyncMode selects the physical primitive a Flush with fsync=true uses.
type SyncMode int

const (
	// SyncModeFull forces data all the way to physical media before returning:
	// os.File.Sync, which on darwin issues F_FULLFSYNC (~4ms on Apple SSDs -
	// see docs/benchmarks/write-path-allocation-fix.md). The strongest and by
	// far the most expensive guarantee; survives power loss.
	SyncModeFull SyncMode = iota
	// SyncModeFast pushes data to the storage device without forcing the
	// device's own cache to media: F_BARRIERFSYNC on darwin, fdatasync on
	// linux, plain Sync elsewhere. Survives process and OS crashes; power loss
	// can lose what the drive cache held. This is the guarantee SQLite
	// (fullfsync off, its default) and PostgreSQL run with on macOS.
	SyncModeFast
)

// PlatformIOConfig configures file-backed platform I/O.
type PlatformIOConfig struct {
	RootDirectory  *string
	FsyncOnFlush   bool
	SyncMode       SyncMode
	MaxAppendBytes int
}

// DefaultPlatformIOConfig returns sensible defaults.
func DefaultPlatformIOConfig() PlatformIOConfig {
	return PlatformIOConfig{
		FsyncOnFlush:   true,
		MaxAppendBytes: 16 * 1024 * 1024,
	}
}

// SegmentHealthReport summarizes segment readability.
type SegmentHealthReport struct {
	SegmentName string
	SizeBytes   int64
	Readable    bool
	Error       *string
}
