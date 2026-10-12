package storage

import "errors"

// SegmentSizer is implemented by shims that can report a segment's length without reading it.
//
// Optional, like io.FileBackedPlatformIO's MaxAppendBytes, so a shim that cannot answer cheaply
// is not made to. A caller holding a shim without it - or getting ErrSegmentSizeUnknown back,
// which a forwarding shim returns when the store beneath it cannot say - has to find the size by
// reading. That fallback is what the SSTable reader used to do on every probe: read the whole
// file in 8 KB chunks to learn its length, so each lookup cost time proportional to the table
// (issue #43's 28-second crash-recovery open).
type SegmentSizer interface {
	SegmentSize(segmentName string) (int64, error)
}

// ErrSegmentSizeUnknown is returned by a SegmentSizer that forwards to a layer which cannot
// report a size. It means "read to find out", not "the segment is missing".
var ErrSegmentSizeUnknown = errors.New("segment size not available from this storage layer")

// SegmentStamp identifies a segment's current contents without reading them: its length and its
// last modification time. Two equal stamps mean nothing has been written to the segment between
// them - appended, repaired or damaged in place - as far as the filesystem can tell.
type SegmentStamp struct {
	Size         int64
	ModTimeNanos int64
}

// SegmentStamper is implemented by shims that can stamp a segment. ok is false when the shim
// cannot vouch for the segment staying put - it is open for appends in this process, or the layer
// beneath cannot say - and a caller must not cache anything about it.
//
// For delta.DefaultReader, which caches each sealed segment's scan under its stamp: listing the
// log used to walk every frame header of every segment on every call, several calls per
// checkpoint, which at 500,000 commits allocated half a gigabyte to write one checkpoint.
type SegmentStamper interface {
	SegmentStamp(segmentName string) (stamp SegmentStamp, ok bool, err error)
}

// PlatformIOShim is the platform I/O boundary for segment and snapshot storage.
type PlatformIOShim interface {
	AppendToSegment(segmentName string, bytes []byte) (int64, error)
	ReadFromSegment(segmentName string, offset int64, length int) ([]byte, error)
	FlushSegment(segmentName string) error
	SealSegment(segmentName string) error
	ListSegments(namespaceID string) ([]string, error)
	DeleteSegment(segmentName string) error
	AvailableBytes() (int64, error)
	ReadSnapshot(key string) ([]byte, error)
	WriteSnapshot(key string, data []byte) error
	DeleteSnapshot(key string) error
}
