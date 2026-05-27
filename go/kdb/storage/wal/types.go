package wal

import (
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/storage"
)

const (
	Magic      = 0x4B444257
	BatchMagic = 0x4B444242
)

// WriteAheadLog appends durable WAL records to a segment.
type WriteAheadLog interface {
	WalID() codec.UUID
	PartitionKey() string
	LastSequence() int64
	ActiveSegmentSizeBytes() int64
	Append(record Record) (AppendResult, error)
	AppendBatch(records []Record) (AppendResult, error)
	Sync() error
	Recover(handler func(Record) error) (RecoverySummary, error)
	Truncate(truncateThroughSequence int64) error
	Close() error
}

// Factory opens or creates a WAL for a partition.
type Factory interface {
	OpenOrCreate(partitionKey string, config storage.StorageEngineConfig, io storage.PlatformIOShim) (WriteAheadLog, error)
	ActiveSegmentName(partitionKey string, walID codec.UUID) string
}

// SegmentCatalog lists and deletes WAL segments.
type SegmentCatalog interface {
	ListSegments(partitionKey string) ([]SegmentInfo, error)
	DeleteSegment(segmentName string) error
}

// SegmentInfo describes one WAL segment file.
type SegmentInfo struct {
	SegmentName   string
	WalID         codec.UUID
	FirstSequence int64
	LastSequence  int64
	SizeBytes     int64
	IsActive      bool
}

// RecordKind classifies a WAL record payload.
type RecordKind int

const (
	RecordKindPutBlob RecordKind = iota
	RecordKindDeleteBlob
	RecordKindFlushCheckpoint
	RecordKindMarker
)

// Record is one WAL entry.
type Record struct {
	Sequence  int64
	Timestamp codec.Timestamp
	Kind      RecordKind
	Payload   []byte
}

// PutBlob is the decoded PutBlob payload.
type PutBlob struct {
	ContentHash codec.Hash
	Bytes       []byte
}

// FlushCheckpoint is the decoded flush-checkpoint payload.
type FlushCheckpoint struct {
	SsTableFileID codec.UUID
	MinKey        codec.Hash
	MaxKey        codec.Hash
	RecordCount   int64
	FileSizeBytes int64
}

// AppendResult is returned after a successful append.
type AppendResult struct {
	Sequence              int64
	SegmentOffset         int64
	SegmentSizeAfterBytes int64
}

// RecoverySummary summarizes a recover() pass.
type RecoverySummary struct {
	RecordsReplayed       int64
	RecordsSkippedCorrupt int64
	LastSequence          int64
	SegmentsScanned       int
}

// CorruptionError indicates a corrupt WAL frame.
type CorruptionError struct {
	Message     string
	PartitionKey string
	SegmentName string
	Offset      int64
	Cause       error
}

func (e *CorruptionError) Error() string { return e.Message }
func (e *CorruptionError) Unwrap() error { return e.Cause }

// ClosedError indicates the WAL was closed.
type ClosedError struct {
	Message      string
	PartitionKey string
}

func (e *ClosedError) Error() string { return e.Message }

// EncodePutBlob returns the PutBlob wire payload (hash || bytes).
func EncodePutBlob(p PutBlob) []byte {
	out := make([]byte, 32+len(p.Bytes))
	copy(out, p.ContentHash.Bytes[:])
	copy(out[32:], p.Bytes)
	return out
}

// DecodePutBlob decodes a PutBlob payload.
func DecodePutBlob(payload []byte) (PutBlob, error) {
	if len(payload) < 32 {
		return PutBlob{}, &CorruptionError{Message: "put blob payload too short"}
	}
	h, err := codec.HashFromBytes(payload[:32])
	if err != nil {
		return PutBlob{}, err
	}
	return PutBlob{ContentHash: h, Bytes: append([]byte(nil), payload[32:]...)}, nil
}

func (e *CorruptionError) Code() kdberr.Code { return kdberr.StorageTierError }
