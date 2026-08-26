package delta

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// DefaultWriter appends framed commits to a delta segment.
type DefaultWriter struct {
	namespaceID string
	segmentID   codec.UUID
	// sequence is this segment's position in namespace-wide commit order -
	// see storage.DeltaSegmentRef.SequenceNumber's doc comment. It, not
	// segmentID, determines the segment's file name and therefore its
	// replay order.
	sequence int64
	shim     storage.PlatformIOShim
	config   storage.StorageEngineConfig

	mu          sync.Mutex
	segmentName string
	sizeBytes   int64
	sealed      bool
	firstCommit *codec.Hash
	lastCommit  *codec.Hash
	pageCodec   PageCodec
}

// NewDefaultWriter opens a new delta segment writer at sequence seq.
// Callers should obtain seq from Factory.OpenWriter, which assigns it by
// scanning existing segments for this namespace - never construct one by
// hand outside a test, or two writers can collide on the same file name.
func NewDefaultWriter(namespaceID string, segmentID codec.UUID, seq int64, shim storage.PlatformIOShim, config storage.StorageEngineConfig) *DefaultWriter {
	return &DefaultWriter{
		namespaceID: namespaceID,
		segmentID:   segmentID,
		sequence:    seq,
		shim:        shim,
		config:      config,
		segmentName: storio.SegmentNameBuilder.DeltaSequenced(namespaceID, seq),
	}
}

func (w *DefaultWriter) NamespaceID() string     { return w.namespaceID }
func (w *DefaultWriter) SegmentID() codec.UUID   { return w.segmentID }
func (w *DefaultWriter) CurrentSizeBytes() int64 { return w.sizeBytes }
func (w *DefaultWriter) IsSealed() bool          { return w.sealed }

func (w *DefaultWriter) Append(record storage.DeltaRecord) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return 0, fmt.Errorf("segment sealed")
	}
	if w.firstCommit == nil {
		h := record.CommitHash
		w.firstCommit = &h
	}
	h := record.CommitHash
	w.lastCommit = &h
	frame, err := w.pageCodec.Frame(record.CommitPayload, w.config.CompressionCodec)
	if err != nil {
		return 0, err
	}
	offset := w.sizeBytes
	newSize, err := w.shim.AppendToSegment(w.segmentName, frame)
	if err != nil {
		return 0, err
	}
	w.sizeBytes = newSize
	return offset, nil
}

func (w *DefaultWriter) Flush() error {
	return w.shim.FlushSegment(w.segmentName)
}

func (w *DefaultWriter) Seal() (storage.DeltaSegmentRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return storage.DeltaSegmentRef{}, fmt.Errorf("already sealed")
	}
	w.sealed = true
	if err := w.shim.SealSegment(w.segmentName); err != nil {
		return storage.DeltaSegmentRef{}, err
	}
	zero, _ := codec.HashFromBytes(make([]byte, 32))
	first, last := zero, zero
	if w.firstCommit != nil {
		first = *w.firstCommit
	}
	if w.lastCommit != nil {
		last = *w.lastCommit
	}
	return storage.DeltaSegmentRef{
		SegmentID:       w.segmentID,
		NamespaceID:     w.namespaceID,
		SequenceNumber:  w.sequence,
		FirstCommitHash: first,
		LastCommitHash:  last,
		SizeBytes:       w.sizeBytes,
		Compression:     w.config.CompressionCodec,
	}, nil
}

// DefaultReader reads delta segments for a namespace.
type DefaultReader struct {
	namespaceID string
	shim        storage.PlatformIOShim
	config      storage.StorageEngineConfig
}

// NewDefaultReader returns a delta segment reader.
func NewDefaultReader(namespaceID string, shim storage.PlatformIOShim, config storage.StorageEngineConfig) *DefaultReader {
	return &DefaultReader{namespaceID: namespaceID, shim: shim, config: config}
}

func (r *DefaultReader) NamespaceID() string { return r.namespaceID }

// ReadAll scans segment's bytes into records. On a *CorruptFrameError
// (see ScanSegmentBytes), it still returns every record scanned
// successfully before the corrupt frame, alongside the error - callers
// that want torn-tail-tolerant replay behavior (kdb-spec-layer13
// Component 47 §4.3) use that partial slice instead of discarding it.
func (r *DefaultReader) ReadAll(segment storage.DeltaSegmentRef) ([]storage.DeltaRecord, error) {
	segmentName := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	raw, err := r.readFullSegment(segmentName, segment.SizeBytes)
	if err != nil {
		return nil, err
	}
	scanned, scanErr := ScanSegmentBytes(raw, segment.Compression)
	out := make([]storage.DeltaRecord, 0, len(scanned))
	for _, s := range scanned {
		payload, err := s.Commit.ToPayloadBytes()
		if err != nil {
			if scanErr == nil {
				scanErr = err
			}
			break
		}
		out = append(out, storage.DeltaRecord{
			CommitHash:    s.CommitHash,
			NamespaceID:   segment.NamespaceID,
			Authorship:    storage.DeltaAuthorshipEnvelope{Principal: "unknown", Timestamp: s.Commit.Timestamp},
			CommitPayload: payload,
		})
	}
	return out, scanErr
}

func (r *DefaultReader) ReadRange(segment storage.DeltaSegmentRef, sinceCommit, untilCommit codec.Hash) ([]storage.DeltaRecord, error) {
	all, err := r.ReadAll(segment)
	if err != nil {
		return nil, err
	}
	var out []storage.DeltaRecord
	pastSince := false
	for _, rec := range all {
		if rec.CommitHash == sinceCommit {
			pastSince = true
		}
		if pastSince && rec.CommitHash != untilCommit {
			out = append(out, rec)
		}
	}
	return out, nil
}

// ListSegments returns this namespace's delta segments **in sequence
// (commit) order** - the order the underlying shim.ListSegments gives
// back is already sequence order, because delta segment file names are
// zero-padded decimal sequence numbers (see
// io.SegmentNameBuilder.DeltaSequenced), which sort lexicographically the
// same as numerically. Callers (see embed.replayDeltaNamespace) must
// preserve this order rather than re-sorting by SegmentID - that was
// exactly the bug Component 47 fixes (kdb-spec-layer13 §4.1).
func (r *DefaultReader) ListSegments() ([]storage.DeltaSegmentRef, error) {
	prefix := storio.SegmentNameBuilder.NamespacePrefix(r.namespaceID) + "delta/"
	names, err := r.shim.ListSegments(r.namespaceID)
	if err != nil {
		return nil, err
	}
	var out []storage.DeltaSegmentRef
	for _, name := range names {
		if len(name) < len(prefix) || name[:len(prefix)] != prefix {
			continue
		}
		fileName := name[len(prefix):]
		seq, ok := storio.ParseDeltaSequencedFileName(fileName)
		if !ok {
			// A pre-Layer-13 (random-UUID) or otherwise-unrecognized
			// segment name. Factory.OpenWriter refuses to open a namespace
			// with any of these present (see scanExistingDeltaSequence), so
			// reaching this in ListSegments means something modified the
			// data directory after a successful open - surface it the same
			// way rather than silently skipping possibly-unread commits.
			return nil, &LegacySegmentFormatError{NamespaceID: r.namespaceID, Names: []string{name}}
		}
		ref, err := r.scanSegmentRef(name, seq)
		if err != nil || ref == nil {
			continue
		}
		out = append(out, *ref)
	}
	return out, nil
}

func (r *DefaultReader) readFullSegment(segmentName string, sizeBytes int64) ([]byte, error) {
	if sizeBytes <= 0 {
		return []byte{}, nil
	}
	return r.shim.ReadFromSegment(segmentName, 0, int(sizeBytes))
}

func (r *DefaultReader) scanSegmentRef(segmentName string, seq int64) (*storage.DeltaSegmentRef, error) {
	raw, err := r.shim.ReadFromSegment(segmentName, 0, 1<<28)
	if err != nil {
		return nil, err
	}
	scanned, scanErr := ScanSegmentBytes(raw, r.config.CompressionCodec)
	var corrupt *CorruptFrameError
	if scanErr != nil && !errors.As(scanErr, &corrupt) {
		return nil, scanErr
	}
	zero, _ := codec.HashFromBytes(make([]byte, 32))
	if len(scanned) == 0 {
		return &storage.DeltaSegmentRef{
			NamespaceID: r.namespaceID, SequenceNumber: seq,
			FirstCommitHash: zero, LastCommitHash: zero,
			SizeBytes: int64(len(raw)), Compression: r.config.CompressionCodec,
		}, nil
	}
	return &storage.DeltaSegmentRef{
		NamespaceID: r.namespaceID, SequenceNumber: seq,
		FirstCommitHash: scanned[0].CommitHash,
		LastCommitHash:  scanned[len(scanned)-1].CommitHash,
		SizeBytes:       int64(len(raw)),
		Compression:     r.config.CompressionCodec,
	}, nil
}

// LegacySegmentFormatError reports a data directory containing delta
// segments named by the pre-Layer-13 scheme (random UUIDs) rather than
// the current monotonic-sequence scheme. Sorting those by name is not
// sorting by commit order, which is exactly the bug that made a
// multi-segment namespace permanently unopenable (kdb-spec-layer13
// Component 47 §4.1, §2.1) - rather than guess at their true order, this
// is returned so the caller can run the repair command
// (kdb-inspect repair-segments) instead.
type LegacySegmentFormatError struct {
	NamespaceID string
	Names       []string
}

func (e *LegacySegmentFormatError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q has %d delta segment(s) in the pre-Layer-13 random-name format, "+
			"whose on-disk order cannot be trusted as commit order - run "+
			"'kdb-inspect repair-segments -data-dir <dir> -namespace %s' to migrate this "+
			"namespace before opening it (see kdb-spec-layer13-resource-governance.md §4.1)",
		e.NamespaceID, len(e.Names), e.NamespaceID)
}

// Factory opens delta writers and readers.
type Factory struct {
	Config storage.StorageEngineConfig
}

// OpenWriter opens a writer for namespaceID's next delta segment: the
// sequence number one past the highest existing sequenced segment (0 if
// none exist yet). Deliberately always starts a *new* segment rather than
// resuming a previous run's last (possibly unsealed) one - continuing an
// existing segment across a restart would need persisted seal-state this
// system doesn't have, and an extra near-empty segment per restart is a
// cheap, safe trade for not needing it (kdb-spec-layer13 §4.1).
//
// Returns a *LegacySegmentFormatError, without opening anything, if any
// pre-Layer-13 random-name segment is present - see that error's doc
// comment.
func (f Factory) OpenWriter(namespaceID string) (*DefaultWriter, error) {
	nextSeq, legacy, err := scanExistingDeltaSequence(f.Config.IOShim, namespaceID)
	if err != nil {
		return nil, err
	}
	if len(legacy) > 0 {
		return nil, &LegacySegmentFormatError{NamespaceID: namespaceID, Names: legacy}
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	return NewDefaultWriter(namespaceID, id, nextSeq, f.Config.IOShim, f.Config), nil
}

func (f Factory) OpenReader(namespaceID string) *DefaultReader {
	return NewDefaultReader(namespaceID, f.Config.IOShim, f.Config)
}

func scanExistingDeltaSequence(shim storage.PlatformIOShim, namespaceID string) (nextSeq int64, legacyNames []string, err error) {
	names, err := shim.ListSegments(namespaceID)
	if err != nil {
		return 0, nil, err
	}
	prefix := storio.SegmentNameBuilder.NamespacePrefix(namespaceID) + "delta/"
	maxSeq := int64(-1)
	for _, name := range names {
		if len(name) < len(prefix) || !strings.HasPrefix(name, prefix) {
			continue
		}
		fileName := name[len(prefix):]
		seq, ok := storio.ParseDeltaSequencedFileName(fileName)
		if !ok {
			legacyNames = append(legacyNames, name)
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq + 1, legacyNames, nil
}
