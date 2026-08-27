package wal

import (
	"sort"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

// DefaultWriteAheadLog is a mutex-protected WAL over one active segment.
type DefaultWriteAheadLog struct {
	walID              codec.UUID
	partitionKey       string
	segmentName        string
	io                 storage.PlatformIOShim
	walMaxSegmentBytes int64
	skipCorrupt        bool

	mu              sync.Mutex
	sequenceCounter int64
	segmentSize     int64
	closed          bool
}

// DefaultFactory opens or creates WAL segments.
type DefaultFactory struct {
	WalMaxSegmentBytes int64
	SkipCorrupt        bool
}

// OpenOrCreate implements Factory.
func (f *DefaultFactory) OpenOrCreate(
	partitionKey string,
	_ storage.StorageEngineConfig,
	ioShim storage.PlatformIOShim,
) (WriteAheadLog, error) {
	maxSeg := f.WalMaxSegmentBytes
	if maxSeg <= 0 {
		maxSeg = 64 * 1024 * 1024
	}
	existing, err := ioShim.ListSegments(partitionKey)
	if err != nil {
		return nil, err
	}
	var walNames []string
	for _, seg := range existing {
		if strings.Contains(seg, "/wal/") {
			walNames = append(walNames, seg)
		}
	}
	if len(walNames) > 0 {
		name := walNames[len(walNames)-1]
		for _, n := range walNames {
			if n > name {
				name = n
			}
		}
		idStr := name[strings.LastIndex(name, "/")+1:]
		walID, err := codec.UUIDFromString(idStr)
		if err != nil {
			return nil, err
		}
		return &DefaultWriteAheadLog{
			walID: walID, partitionKey: partitionKey, segmentName: name,
			io: ioShim, walMaxSegmentBytes: maxSeg, skipCorrupt: f.SkipCorrupt,
		}, nil
	}
	walID, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	name := f.ActiveSegmentName(partitionKey, walID)
	if _, err := ioShim.AppendToSegment(name, nil); err != nil {
		return nil, err
	}
	return &DefaultWriteAheadLog{
		walID: walID, partitionKey: partitionKey, segmentName: name,
		io: ioShim, walMaxSegmentBytes: maxSeg, skipCorrupt: f.SkipCorrupt,
	}, nil
}

// ActiveSegmentName implements Factory.
func (f *DefaultFactory) ActiveSegmentName(partitionKey string, walID codec.UUID) string {
	return io.SegmentNameBuilder.WAL(partitionKey, walID.String())
}

func (w *DefaultWriteAheadLog) WalID() codec.UUID             { return w.walID }
func (w *DefaultWriteAheadLog) PartitionKey() string          { return w.partitionKey }
func (w *DefaultWriteAheadLog) LastSequence() int64           { return w.sequenceCounter }
func (w *DefaultWriteAheadLog) ActiveSegmentSizeBytes() int64 { return w.segmentSize }

func (w *DefaultWriteAheadLog) Append(record Record) (AppendResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpen(); err != nil {
		return AppendResult{}, err
	}
	seq := w.sequenceCounter + 1
	w.sequenceCounter = seq
	rec := record
	rec.Sequence = seq
	bytes := EncodeRecord(rec)
	offset := w.segmentSize
	newSize, err := w.io.AppendToSegment(w.segmentName, bytes)
	if err != nil {
		return AppendResult{}, err
	}
	w.segmentSize = newSize
	return AppendResult{Sequence: seq, SegmentOffset: offset, SegmentSizeAfterBytes: newSize}, nil
}

func (w *DefaultWriteAheadLog) AppendBatch(records []Record) (AppendResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpen(); err != nil {
		return AppendResult{}, err
	}
	var last AppendResult
	for _, r := range records {
		seq := w.sequenceCounter + 1
		w.sequenceCounter = seq
		rec := r
		rec.Sequence = seq
		bytes := EncodeRecord(rec)
		off := w.segmentSize
		newSize, err := w.io.AppendToSegment(w.segmentName, bytes)
		if err != nil {
			return AppendResult{}, err
		}
		w.segmentSize = newSize
		last = AppendResult{Sequence: seq, SegmentOffset: off, SegmentSizeAfterBytes: newSize}
	}
	return last, nil
}

func (w *DefaultWriteAheadLog) Sync() error {
	return w.io.FlushSegment(w.segmentName)
}

func (w *DefaultWriteAheadLog) Recover(handler func(Record) error) (RecoverySummary, error) {
	bytes, err := w.readFullSegment()
	if err != nil {
		return RecoverySummary{}, err
	}
	records, err := DecodeRecords(bytes, w.partitionKey, w.segmentName, w.skipCorrupt)
	if err != nil {
		return RecoverySummary{}, err
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Sequence < records[j].Sequence })
	var replayed, maxSeq int64
	for _, r := range records {
		if err := handler(r); err != nil {
			return RecoverySummary{}, err
		}
		replayed++
		if r.Sequence > maxSeq {
			maxSeq = r.Sequence
		}
	}
	w.mu.Lock()
	w.sequenceCounter = maxSeq
	w.segmentSize = int64(len(bytes))
	w.mu.Unlock()
	return RecoverySummary{
		RecordsReplayed: replayed,
		LastSequence:    maxSeq,
		SegmentsScanned: 1,
	}, nil
}

func (w *DefaultWriteAheadLog) Truncate(truncateThroughSequence int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if truncateThroughSequence >= w.sequenceCounter {
		if err := w.io.DeleteSegment(w.segmentName); err != nil {
			return err
		}
		w.segmentSize = 0
	}
	return nil
}

func (w *DefaultWriteAheadLog) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}

func (w *DefaultWriteAheadLog) checkOpen() error {
	if w.closed {
		return &ClosedError{Message: "WAL closed", PartitionKey: w.partitionKey}
	}
	return nil
}

func (w *DefaultWriteAheadLog) readFullSegment() ([]byte, error) {
	const chunk = 64 * 1024
	var out []byte
	var off int64
	for {
		part, err := w.io.ReadFromSegment(w.segmentName, off, chunk)
		if err != nil {
			return nil, err
		}
		if len(part) == 0 {
			break
		}
		out = append(out, part...)
		off += int64(len(part))
		if len(part) < chunk {
			break
		}
	}
	return out, nil
}
