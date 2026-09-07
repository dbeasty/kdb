package wal

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/io"
)

// defaultMaxSegmentBytes is the spec default for walMaxSegmentBytes (64 MiB).
const defaultMaxSegmentBytes = 64 * 1024 * 1024

// readChunkBytes is the window used to walk a segment when only its bytes (or its length) are
// needed; PlatformIOShim has no stat call.
const readChunkBytes = 64 * 1024

// walSegment is one file in a WAL's segment chain. Every segment of one WAL shares the walID;
// the file name carries the sequence its records start at, which is what orders the chain.
type walSegment struct {
	name          string
	firstSequence int64
}

// DefaultWriteAheadLog is a mutex-protected WAL over a chain of segments, the last of which is
// the active one that appends land in.
type DefaultWriteAheadLog struct {
	walID              codec.UUID
	partitionKey       string
	io                 storage.PlatformIOShim
	walMaxSegmentBytes int64
	skipCorrupt        bool

	mu              sync.Mutex
	segments        []walSegment
	segmentName     string
	sequenceCounter int64
	segmentSize     int64
	closed          bool
}

// DefaultFactory opens or creates WAL segments.
type DefaultFactory struct {
	WalMaxSegmentBytes int64
	SkipCorrupt        bool
}

// OpenOrCreate implements Factory. Settings come from the factory when set, otherwise from
// config (walMaxSegmentBytes / walSkipCorruptRecords are 10e-owned per the WAL spec), and
// otherwise from the spec defaults.
func (f *DefaultFactory) OpenOrCreate(
	partitionKey string,
	config storage.StorageEngineConfig,
	ioShim storage.PlatformIOShim,
) (WriteAheadLog, error) {
	maxSeg := f.WalMaxSegmentBytes
	if maxSeg <= 0 {
		maxSeg = config.WalMaxSegmentBytes
	}
	if maxSeg <= 0 {
		maxSeg = defaultMaxSegmentBytes
	}
	existing, err := ioShim.ListSegments(partitionKey)
	if err != nil {
		return nil, err
	}
	w := &DefaultWriteAheadLog{
		partitionKey:       partitionKey,
		io:                 ioShim,
		walMaxSegmentBytes: maxSeg,
		skipCorrupt:        f.SkipCorrupt || config.WalSkipCorruptRecords,
	}
	chain, walIDStr := latestWalChain(existing)
	if len(chain) == 0 {
		walID, err := codec.RandomUUID()
		if err != nil {
			return nil, err
		}
		name := f.ActiveSegmentName(partitionKey, walID)
		if _, err := ioShim.AppendToSegment(name, nil); err != nil {
			return nil, err
		}
		w.walID = walID
		w.segments = []walSegment{{name: name, firstSequence: 1}}
		w.segmentName = name
		return w, nil
	}
	walID, err := codec.UUIDFromString(walIDStr)
	if err != nil {
		return nil, err
	}
	w.walID = walID
	w.segments = chain
	w.segmentName = chain[len(chain)-1].name
	// The active segment's size decides when the next append rotates, so it has to be known
	// before any append - not only after a recover() pass, which is the one place that used to
	// set it (leaving every offset and every size check wrong on a re-opened WAL until then).
	size, err := segmentSizeOf(ioShim, w.segmentName)
	if err != nil {
		return nil, err
	}
	w.segmentSize = size
	return w, nil
}

// ActiveSegmentName implements Factory: the first segment of a WAL keeps the plain
// `wal/{partitionKey}/{walId}` name. Rotation appends `.{firstSequence}` to it (see
// rotatedSegmentName), so the chain sorts into sequence order by name.
func (f *DefaultFactory) ActiveSegmentName(partitionKey string, walID codec.UUID) string {
	return io.SegmentNameBuilder.WAL(partitionKey, walID.String())
}

// RotatedSegmentName names the segment that starts at firstSequence. The sequence is
// zero-padded so that lexicographic order (all ListSegments implementations sort that way)
// equals numeric order, matching the convention DeltaSequencedFileName already uses.
//
// The spec's `{walId}.{firstSeq}-{lastSeq}.log.sealed` names a segment by the range it ended
// up covering, which can only be assigned once the segment is full - i.e. it needs a rename,
// and PlatformIOShim has no rename (nor a copy that wouldn't rewrite the whole segment).
// Naming by start sequence carries the same ordering and truncation information: a sealed
// segment's last sequence is its successor's firstSequence - 1.
func RotatedSegmentName(partitionKey string, walID codec.UUID, firstSequence int64) string {
	return io.SegmentNameBuilder.WAL(partitionKey, fmt.Sprintf("%s.%020d", walID.String(), firstSequence))
}

// latestWalChain groups WAL segment names by walID and returns the newest group's segments in
// chain order. Only one WAL per partition is ever active; anything left over from an earlier
// walID is ignored here exactly as it was before rotation existed.
func latestWalChain(segmentNames []string) ([]walSegment, string) {
	groups := make(map[string][]walSegment)
	for _, name := range segmentNames {
		if !strings.Contains(name, "/wal/") {
			continue
		}
		fileName := name[strings.LastIndex(name, "/")+1:]
		walID, firstSeq, ok := ParseWalFileName(fileName)
		if !ok {
			continue
		}
		groups[walID] = append(groups[walID], walSegment{name: name, firstSequence: firstSeq})
	}
	var newest string
	for walID := range groups {
		if walID > newest {
			newest = walID
		}
	}
	chain := groups[newest]
	sort.Slice(chain, func(i, j int) bool { return chain[i].firstSequence < chain[j].firstSequence })
	return chain, newest
}

// ParseWalFileName splits a WAL segment file name into its walID and the sequence its records
// start at. A name with no sequence suffix is a WAL's first segment (and is also what every
// pre-rotation WAL wrote), so it starts at sequence 1.
//
// Exported because the name format is part of the cross-language on-disk contract, not an
// implementation detail: Kotlin's parseWalFileName must accept exactly the same names, and the
// physical-layer conformance suite pins that from outside this package.
func ParseWalFileName(fileName string) (walID string, firstSequence int64, ok bool) {
	id, suffix, hasSuffix := strings.Cut(fileName, ".")
	if id == "" {
		return "", 0, false
	}
	if !hasSuffix {
		return id, 1, true
	}
	seq, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil || seq < 1 {
		return "", 0, false
	}
	return id, seq, true
}

func (w *DefaultWriteAheadLog) WalID() codec.UUID    { return w.walID }
func (w *DefaultWriteAheadLog) PartitionKey() string { return w.partitionKey }

func (w *DefaultWriteAheadLog) LastSequence() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sequenceCounter
}

func (w *DefaultWriteAheadLog) ActiveSegmentSizeBytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentSize
}

// ActiveSegmentName returns the segment appends currently land in.
func (w *DefaultWriteAheadLog) ActiveSegmentName() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segmentName
}

// SegmentNames returns the WAL's segment chain in sequence order, active segment last.
func (w *DefaultWriteAheadLog) SegmentNames() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.segments))
	for _, s := range w.segments {
		out = append(out, s.name)
	}
	return out
}

func (w *DefaultWriteAheadLog) Append(record Record) (AppendResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpen(); err != nil {
		return AppendResult{}, err
	}
	seq := w.sequenceCounter + 1
	rec := record
	rec.Sequence = seq
	bytes := EncodeRecord(rec)
	if err := w.rotateIfFullLocked(int64(len(bytes)), seq); err != nil {
		return AppendResult{}, err
	}
	offset := w.segmentSize
	newSize, err := w.io.AppendToSegment(w.segmentName, bytes)
	if err != nil {
		return AppendResult{}, err
	}
	w.sequenceCounter = seq
	w.segmentSize = newSize
	return AppendResult{Sequence: seq, SegmentOffset: offset, SegmentSizeAfterBytes: newSize}, nil
}

func (w *DefaultWriteAheadLog) AppendBatch(records []Record) (AppendResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpen(); err != nil {
		return AppendResult{}, err
	}
	if len(records) == 0 {
		return AppendResult{Sequence: w.sequenceCounter, SegmentSizeAfterBytes: w.segmentSize}, nil
	}
	encoded := make([][]byte, 0, len(records))
	var total int64
	for i, r := range records {
		rec := r
		rec.Sequence = w.sequenceCounter + int64(i) + 1
		b := EncodeRecord(rec)
		encoded = append(encoded, b)
		total += int64(len(b))
	}
	// Rotate once, up front, on the batch's total size: a batch is replayed all-or-nothing, so
	// it must not be split across two segments.
	if err := w.rotateIfFullLocked(total, w.sequenceCounter+1); err != nil {
		return AppendResult{}, err
	}
	var last AppendResult
	for _, b := range encoded {
		seq := w.sequenceCounter + 1
		off := w.segmentSize
		newSize, err := w.io.AppendToSegment(w.segmentName, b)
		if err != nil {
			return AppendResult{}, err
		}
		w.sequenceCounter = seq
		w.segmentSize = newSize
		last = AppendResult{Sequence: seq, SegmentOffset: off, SegmentSizeAfterBytes: newSize}
	}
	return last, nil
}

// rotateIfFullLocked seals the active segment and opens a new one when the incoming write
// would push it past walMaxSegmentBytes. A write larger than the cap gets a segment to itself
// rather than being rejected. Before this existed, walMaxSegmentBytes was stored and never
// read: one WAL segment grew without limit for the lifetime of the partition, so nothing could
// ever be truncated short of deleting the whole log.
func (w *DefaultWriteAheadLog) rotateIfFullLocked(incomingBytes, firstSequence int64) error {
	if w.walMaxSegmentBytes <= 0 || w.segmentSize == 0 {
		return nil
	}
	if w.segmentSize+incomingBytes <= w.walMaxSegmentBytes {
		return nil
	}
	previous := w.segmentName
	name := RotatedSegmentName(w.partitionKey, w.walID, firstSequence)
	if _, err := w.io.AppendToSegment(name, nil); err != nil {
		return err
	}
	// Flush before sealing: the segment is about to stop being written to, and a shim that
	// treats sealing as final would otherwise leave its tail unflushed.
	if err := w.io.FlushSegment(previous); err != nil {
		return err
	}
	if err := w.io.SealSegment(previous); err != nil {
		return err
	}
	w.segments = append(w.segments, walSegment{name: name, firstSequence: firstSequence})
	w.segmentName = name
	w.segmentSize = 0
	return nil
}

// Sync flushes the active segment. Sealed segments were flushed as part of rotation.
func (w *DefaultWriteAheadLog) Sync() error {
	w.mu.Lock()
	name := w.segmentName
	w.mu.Unlock()
	return w.io.FlushSegment(name)
}

// Recover replays every segment in the chain, oldest first, in ascending sequence order.
func (w *DefaultWriteAheadLog) Recover(handler func(Record) error) (RecoverySummary, error) {
	w.mu.Lock()
	segments := append([]walSegment(nil), w.segments...)
	w.mu.Unlock()

	var replayed, skipped, maxSeq int64
	var activeSize int64
	scanned := 0
	for i, seg := range segments {
		bytes, err := readSegment(w.io, seg.name)
		if err != nil {
			return RecoverySummary{}, err
		}
		if i == len(segments)-1 {
			activeSize = int64(len(bytes))
		}
		scanned++
		decoded, err := DecodeRecords(bytes, w.partitionKey, seg.name, w.skipCorrupt)
		if err != nil {
			return RecoverySummary{}, err
		}
		skipped += decoded.SkippedCorrupt
		records := decoded.Records
		sort.Slice(records, func(a, b int) bool { return records[a].Sequence < records[b].Sequence })
		for _, r := range records {
			if err := handler(r); err != nil {
				return RecoverySummary{}, err
			}
			replayed++
			if r.Sequence > maxSeq {
				maxSeq = r.Sequence
			}
		}
	}
	w.mu.Lock()
	if maxSeq > w.sequenceCounter {
		w.sequenceCounter = maxSeq
	}
	w.segmentSize = activeSize
	w.mu.Unlock()
	return RecoverySummary{
		RecordsReplayed: replayed,
		// Reported rather than dropped: DecodeRecords now returns how many frames it skipped,
		// so a caller can tell a clean recovery from one that silently lost records. This field
		// was previously never assigned and so always read zero.
		RecordsSkippedCorrupt: skipped,
		LastSequence:          maxSeq,
		SegmentsScanned:       scanned,
	}, nil
}

// Truncate drops WAL bytes already reflected elsewhere: every sealed segment whose records all
// fall at or below truncateThroughSequence is deleted, and the active segment is emptied when it
// too is fully covered. When the cut falls *inside* the active segment, that segment is rewritten
// keeping only the records past the cut - the log is append-only with no in-place trim, so a
// rewrite is the only way to reclaim anything, and without it a partial truncate silently did
// nothing until every last record was superseded. The sequence counter is preserved in all three
// cases, so appends continue where they left off.
//
// Kotlin's DefaultWriteAheadLog.truncate implements the same three cases; the two must agree, or
// a partition truncated by one runtime looks like it still owes records to the other.
func (w *DefaultWriteAheadLog) Truncate(truncateThroughSequence int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.checkOpen(); err != nil {
		return err
	}
	kept := make([]walSegment, 0, len(w.segments))
	for i := 0; i < len(w.segments)-1; i++ {
		// A sealed segment's last sequence is one below where its successor starts.
		if w.segments[i+1].firstSequence-1 <= truncateThroughSequence {
			if err := w.io.DeleteSegment(w.segments[i].name); err != nil {
				return err
			}
			continue
		}
		kept = append(kept, w.segments[i])
	}
	active := w.segments[len(w.segments)-1]
	switch {
	case truncateThroughSequence >= w.sequenceCounter:
		if err := w.io.DeleteSegment(active.name); err != nil {
			return err
		}
		if _, err := w.io.AppendToSegment(active.name, nil); err != nil {
			return err
		}
		active.firstSequence = w.sequenceCounter + 1
		w.segmentSize = 0
	case truncateThroughSequence >= active.firstSequence:
		bytes, err := readSegment(w.io, active.name)
		if err != nil {
			return err
		}
		decoded, err := DecodeRecords(bytes, w.partitionKey, active.name, w.skipCorrupt)
		if err != nil {
			return err
		}
		var rewritten []byte
		for _, r := range decoded.Records {
			if r.Sequence > truncateThroughSequence {
				rewritten = append(rewritten, EncodeRecord(r)...)
			}
		}
		if err := w.io.DeleteSegment(active.name); err != nil {
			return err
		}
		if _, err := w.io.AppendToSegment(active.name, rewritten); err != nil {
			return err
		}
		active.firstSequence = truncateThroughSequence + 1
		w.segmentSize = int64(len(rewritten))
	}
	w.segments = append(kept, active)
	w.segmentName = active.name
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

// readSegment reads a whole segment; a missing or empty segment reads as no bytes.
func readSegment(shim storage.PlatformIOShim, name string) ([]byte, error) {
	var out []byte
	var off int64
	for {
		part, err := shim.ReadFromSegment(name, off, readChunkBytes)
		if err != nil {
			return nil, err
		}
		if len(part) == 0 {
			break
		}
		out = append(out, part...)
		off += int64(len(part))
		if len(part) < readChunkBytes {
			break
		}
	}
	return out, nil
}

// segmentSizeOf reports a segment's length. PlatformIOShim exposes no stat, so this walks the
// segment; it discards the bytes rather than accumulating them.
func segmentSizeOf(shim storage.PlatformIOShim, name string) (int64, error) {
	var size int64
	for {
		part, err := shim.ReadFromSegment(name, size, readChunkBytes)
		if err != nil {
			return 0, err
		}
		size += int64(len(part))
		if len(part) < readChunkBytes {
			return size, nil
		}
	}
}
