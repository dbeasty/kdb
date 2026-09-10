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

	// PreallocateBytes creates each new delta segment at this size, with its
	// extents allocated and zero-filled, instead of letting it grow one append
	// at a time. Zero - the default - turns the whole thing off and restores
	// exactly the previous grow-as-you-go behavior.
	//
	// Why it exists: a growing file makes every sync commit the new file size,
	// which is filesystem metadata, so fdatasync cannot skip it and collapses
	// into fsync. That is why syncMode=fast measures the same as full on Linux.
	// A segment that never changes size takes data-only syncs.
	// See docs/kdb-segment-preallocation-plan.md.
	//
	// This is a physical-layout switch, not a format change, and the two
	// settings interoperate in both directions: a preallocated segment is a
	// normal frame sequence followed by zeros, and every reader already stops
	// at the first frame that does not parse (delta.ScanSegmentBytes' torn-tail
	// handling), so a process with preallocation off reads one correctly. In
	// the other direction there is nothing to notice - a non-preallocated
	// segment is just a smaller file. Segments are never reopened for appending
	// by either (delta.Factory.OpenWriter always starts a new one), which is
	// what keeps that true rather than merely usually true.
	PreallocateBytes int64
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
