package io

// PlatformIOConfig configures file-backed platform I/O.
type PlatformIOConfig struct {
	RootDirectory  *string
	FsyncOnFlush   bool
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
