package delta

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
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

// SequenceNumber implements storage.DeltaSegmentSequencer: it reports the
// segment this writer is appending to *now*. OpenWriter always starts a
// fresh segment rather than resuming one, but the number is no longer fixed
// for the writer's life - RotateIfNeeded advances it. Anything pairing this
// with a frame offset must read it and append with no rotation in between;
// see RotateIfNeeded.
func (w *DefaultWriter) SequenceNumber() int64 { return w.sequence }
func (w *DefaultWriter) IsSealed() bool        { return w.sealed }

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

// DefaultDeltaMaxSegmentBytes is how large the active delta segment may get
// before RotateIfNeeded starts a new one. Matches the WAL's own default:
// large enough that rotation is rare, small enough that one segment is not
// the entire namespace.
const DefaultDeltaMaxSegmentBytes = 64 * 1024 * 1024

// RotateIfNeeded seals the active segment and starts the next one once it
// has grown past its cap, and reports whether it did.
//
// This exists because retention can only reclaim a *sealed* segment: the
// truncation planner refuses to touch the one being written to, on the
// grounds that its contents are not final. Without rotation that exclusion
// covers everything the running process has ever written - so a server that
// stays up for weeks cannot reclaim any of its own commits however often
// maintenance runs, and can only ever reclaim what earlier sessions sealed
// on their way out. Rotation is what makes history=none bounded for a
// process that does not restart.
//
// Call this only *between* batches, never inside one. The caller records
// each commit's location as (segment sequence, frame offset) and reads the
// sequence once for the batch it is about to write (see the commit log's
// appendBatch); rotating midway would file the records written after the
// rotation under the sequence of the segment before it, and a later cold
// read would look for a frame in a segment that never held it. Checking at
// the boundary keeps that pairing true by construction, at the cost of
// overshooting the cap by at most one batch - the same "floor, not ceiling"
// trade the retention window makes.
func (w *DefaultWriter) RotateIfNeeded() (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return false, nil
	}
	max := w.config.DeltaMaxSegmentBytes
	if max <= 0 {
		max = DefaultDeltaMaxSegmentBytes
	}
	// An empty segment never rotates, whatever the cap. Rotating on size
	// alone would let a cap smaller than a single frame produce an endless
	// run of empty segments, each sealed the moment it was created.
	if w.sizeBytes == 0 || w.sizeBytes < max {
		return false, nil
	}
	// Flush before sealing, for the reason the WAL gives at the same point:
	// the segment is about to stop being written to, and a shim that treats
	// sealing as final would otherwise leave its tail unflushed.
	if err := w.shim.FlushSegment(w.segmentName); err != nil {
		return false, err
	}
	if _, err := w.sealLocked(); err != nil {
		return false, err
	}
	id, err := codec.RandomUUID()
	if err != nil {
		// The old segment is already sealed, so this writer can take no
		// further append. Report it rather than leaving the caller to
		// discover a sealed writer on its next Append.
		return false, fmt.Errorf(
			"delta: sealed segment %d but could not name its successor: %w", w.sequence, err)
	}
	w.segmentID = id
	w.sequence++
	w.segmentName = storio.SegmentNameBuilder.DeltaSequenced(w.namespaceID, w.sequence)
	w.sizeBytes = 0
	w.firstCommit = nil
	w.lastCommit = nil
	w.sealed = false
	return true, nil
}

func (w *DefaultWriter) Seal() (storage.DeltaSegmentRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sealLocked()
}

// sealLocked is Seal's body, split out so RotateIfNeeded can seal the
// segment it is rotating away from without releasing the writer's mutex -
// a rotation that let go halfway would let another append land in a
// segment that is on its way to being sealed.
func (w *DefaultWriter) sealLocked() (storage.DeltaSegmentRef, error) {
	if w.sealed {
		return storage.DeltaSegmentRef{}, fmt.Errorf("already sealed")
	}
	w.sealed = true
	// Nothing was ever appended, so there is nothing to seal - and sealing
	// anyway is actively harmful. The next open picks its sequence from the
	// segment file names present, so it reuses this number; if the shim has
	// marked it sealed, that open's first append is refused and the
	// namespace cannot be written to at all. Reached by opening a namespace,
	// closing it without writing, and opening it again, which is exactly
	// what a reopen does when it changes a setting before any traffic
	// arrives.
	if w.sizeBytes == 0 {
		return w.refLocked(), nil
	}
	if err := w.shim.SealSegment(w.segmentName); err != nil {
		return storage.DeltaSegmentRef{}, err
	}
	return w.refLocked(), nil
}

// refLocked describes the segment this writer is on. Callers hold w.mu.
func (w *DefaultWriter) refLocked() storage.DeltaSegmentRef {
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
	}
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
	scanned, scanErr := ScanSegmentBytes(raw)
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

// maxScannedSegmentBytes bounds a single scanSegmentRef read. PlatformIOShim
// exposes no segment-size call, so "read the whole segment" has to be expressed
// as an upper bound; the byte stores clamp the request down to the file's real
// size, so this is a ceiling rather than an allocation. Segments are sealed well
// below this (wal/delta segment sizing is measured in tens of MiB), so hitting
// it means something is wrong - reported rather than silently truncated, since a
// truncated scan would yield a wrong LastCommitHash for the segment.
const maxScannedSegmentBytes = 1 << 28 // 256MiB

// segmentScanWindowBytes bounds how much of a segment is held in memory
// while its ends are located. A window, not the whole file: this runs once
// per segment on every ListSegments, and ListSegments runs several times
// during a single open, so reading each segment whole meant opening a
// namespace allocated - and, measurably, retained - a full copy of its
// delta log. On a 20,000-commit namespace that was 9.17MB of a 21.11MB
// heap, the single largest thing in it, and proportional to the log rather
// than to anything live.
const segmentScanWindowBytes = 1 << 20

func (r *DefaultReader) scanSegmentRef(segmentName string, seq int64) (*storage.DeltaSegmentRef, error) {
	// Locate the ends by walking frame headers a window at a time, then
	// decode only the two frames that turn out to matter. A ref needs two
	// commit hashes; it never needed the segment in memory.
	var (
		firstOffset = int64(-1)
		lastOffset  = int64(-1)
		total       int64
		pos         int64
		window      = int64(segmentScanWindowBytes)
	)
	for {
		raw, err := r.shim.ReadFromSegment(segmentName, pos, int(window))
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			break
		}
		walk, walkErr := WalkFrames(raw, pos)
		var corrupt *CorruptFrameError
		if walkErr != nil && !errors.As(walkErr, &corrupt) {
			return nil, walkErr
		}
		if walk.FirstOffset >= 0 && firstOffset < 0 {
			firstOffset = walk.FirstOffset
		}
		if walk.LastOffset >= 0 {
			lastOffset = walk.LastOffset
		}
		if walkErr != nil {
			// A corrupt frame ends the scan here, exactly as a whole-segment
			// scan would: the ends reported are what was readable before it.
			total = walk.ConsumedEnd
			break
		}
		if walk.ConsumedEnd == pos {
			// Nothing complete in this window. Either the segment ends here
			// or one frame is larger than the window; grow and retry so a
			// large frame cannot stall the scan.
			if int64(len(raw)) < window {
				total = pos
				break
			}
			window *= 2
			continue
		}
		total = walk.ConsumedEnd
		if int64(len(raw)) < window {
			// Short read: the file ended inside this window.
			break
		}
		pos = walk.ConsumedEnd
		window = int64(segmentScanWindowBytes)
	}

	zero, _ := codec.HashFromBytes(make([]byte, 32))
	if firstOffset < 0 {
		return &storage.DeltaSegmentRef{
			NamespaceID: r.namespaceID, SequenceNumber: seq,
			FirstCommitHash: zero, LastCommitHash: zero,
			SizeBytes: total, Compression: r.config.CompressionCodec,
		}, nil
	}
	first, err := r.commitAtOffset(segmentName, firstOffset)
	if err != nil {
		return nil, err
	}
	last := first
	if lastOffset != firstOffset {
		last, err = r.commitAtOffset(segmentName, lastOffset)
		if err != nil {
			return nil, err
		}
	}
	return &storage.DeltaSegmentRef{
		NamespaceID: r.namespaceID, SequenceNumber: seq,
		FirstCommitHash: first.Hash,
		LastCommitHash:  last.Hash,
		SizeBytes:       total,
		Compression:     r.config.CompressionCodec,
	}, nil
}

// commitAtOffset decodes the single frame starting at offset, reading only
// that frame's bytes.
func (r *DefaultReader) commitAtOffset(segmentName string, offset int64) (document.Commit, error) {
	header, err := r.shim.ReadFromSegment(segmentName, offset, PageFrameHeaderSize)
	if err != nil {
		return document.Commit{}, err
	}
	if len(header) < PageFrameHeaderSize {
		return document.Commit{}, &CorruptFrameError{Offset: int(offset), Reason: "segment ends inside the frame header"}
	}
	raw, err := r.shim.ReadFromSegment(segmentName, offset, PageFrameHeaderSize+readIntBE(header, 8))
	if err != nil {
		return document.Commit{}, err
	}
	return DecodeFrame(raw, int(offset))
}

// StreamCommits implements storage.DeltaCommitStreamer: it hands segment's
// commits to fn one at a time as they are decoded, so nothing holds the
// whole segment's worth of documents at once.
//
// This is ReadAll without the round-trip. ReadAll has to return
// storage.DeltaRecords, which carry the commit as encoded payload bytes,
// so a caller that wants commits back - replay is the only one - pays a
// decode here, an encode into the record, and a decode again at the far
// end, three full materializations of every version in the segment. Here
// the commit that ScanSegmentFrames already decoded goes straight to fn.
//
// Torn-tail semantics are ReadAll's: on *CorruptFrameError every commit
// before the fault has already reached fn, and the caller decides whether
// a fault in this particular segment is an expected unclean shutdown.
func (r *DefaultReader) StreamCommits(segment storage.DeltaSegmentRef, fn func(document.Commit, int64) error) error {
	segmentName := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	raw, err := r.readFullSegment(segmentName, segment.SizeBytes)
	if err != nil {
		return err
	}
	return ScanSegmentFrames(raw, func(s ScannedCommit) error {
		return fn(s.Commit, int64(s.FrameOffset))
	})
}

// ReadCommitAt implements storage.DeltaCommitStreamer: it decodes just the
// commit framed at frameOffset. Reads only that frame's bytes - its length
// is in its own header - so recovering one commit out of a large segment
// costs one commit, not the segment.
func (r *DefaultReader) ReadCommitAt(segment storage.DeltaSegmentRef, frameOffset int64) (document.Commit, error) {
	segmentName := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	header, err := r.shim.ReadFromSegment(segmentName, frameOffset, PageFrameHeaderSize)
	if err != nil {
		return document.Commit{}, err
	}
	if len(header) < PageFrameHeaderSize {
		return document.Commit{}, &CorruptFrameError{
			Offset: int(frameOffset), Reason: "segment ends inside the frame header",
		}
	}
	frameLen := PageFrameHeaderSize + readIntBE(header, 8)
	raw, err := r.shim.ReadFromSegment(segmentName, frameOffset, frameLen)
	if err != nil {
		return document.Commit{}, err
	}
	return DecodeFrame(raw, int(frameOffset))
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
