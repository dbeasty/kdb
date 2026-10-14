package delta

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/bufpool"
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
	// batchBuf is AppendBatch's frame buffer, kept between batches (see maxRetainedBatchBuf).
	batchBuf []byte
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

// AppendBatch appends records as consecutive frames - the same frames, at the same offsets, that
// one Append per record writes - but hands them to the platform in as few writes as its append
// cap allows, usually one. It returns the offset of every record it wrote, in order; on an error,
// the records before the returned offsets' length were written and the rest were not.
//
// Why: the commit log drains up to maxLogBatch queued records at a time but wrote each with its
// own Append, one positional write per commit. A peer-sync adoption queues a whole fetched range
// at once, and at 200,000 commits that was about 20 s of write syscalls on the receiver - most of
// the sync's time, waited for by the adoption. Frame bytes on disk are unchanged, so readers, the
// torn-tail scan and the Kotlin implementation see exactly what they saw before.
func (w *DefaultWriter) AppendBatch(records []storage.DeltaRecord) ([]int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return nil, fmt.Errorf("segment sealed")
	}
	limit := 0
	if l, ok := w.shim.(interface{ MaxAppendBytes() int }); ok {
		limit = l.MaxAppendBytes()
	}
	offsets := make([]int64, 0, len(records))
	buf := w.batchBuf[:0]
	pending := 0 // records framed into buf, not yet written
	write := func() error {
		if len(buf) == 0 {
			return nil
		}
		newSize, err := w.shim.AppendToSegment(w.segmentName, buf)
		if err != nil {
			offsets = offsets[:len(offsets)-pending]
			return err
		}
		w.sizeBytes = newSize
		buf, pending = buf[:0], 0
		return nil
	}
	defer func() {
		// Keep the buffer for the next batch unless one outsized batch grew it past reason.
		if cap(buf) <= maxRetainedBatchBuf {
			w.batchBuf = buf[:0]
		} else {
			w.batchBuf = nil
		}
	}()
	next := w.sizeBytes
	for _, record := range records {
		// Framed straight onto the end of buf (PageCodec.AppendFrame).
		start := len(buf)
		var err error
		buf, err = w.pageCodec.AppendFrame(buf, record.CommitPayload, w.config.CompressionCodec)
		if err != nil {
			buf = buf[:start]
			if werr := write(); werr != nil {
				return offsets, werr
			}
			return offsets, err
		}
		frameLen := len(buf) - start
		if limit > 0 && start > 0 && len(buf) > limit {
			// This frame would take the write past the cap: write the frames before it, and
			// start the next write with it.
			frame := append([]byte(nil), buf[start:]...)
			buf = buf[:start]
			if err := write(); err != nil {
				return offsets, err
			}
			buf = append(buf, frame...)
		}
		if w.firstCommit == nil {
			h := record.CommitHash
			w.firstCommit = &h
		}
		h := record.CommitHash
		w.lastCommit = &h
		offsets = append(offsets, next)
		next += int64(frameLen)
		pending++
	}
	if err := write(); err != nil {
		return offsets, err
	}
	return offsets, nil
}

// maxRetainedBatchBuf is the largest AppendBatch buffer a writer keeps between batches.
const maxRetainedBatchBuf = 4 << 20

func (w *DefaultWriter) Flush() error {
	return w.shim.FlushSegment(w.segmentName)
}

// DefaultDeltaMaxSegmentBytes is how large the active delta segment may get
// before RotateIfNeeded starts a new one. Matches the WAL's own default:
// large enough that rotation is rare, small enough that one segment is not
// the entire namespace.
const DefaultDeltaMaxSegmentBytes = 64 * 1024 * 1024

// EffectiveMaxSegmentBytes resolves a configured cap (zero meaning "default")
// to the size a segment actually rotates at.
//
// It exists so that the rotation threshold has exactly one definition. Segment
// preallocation has to create a segment at precisely this size, and the two
// numbers are only ever correct when they are equal: preallocate less than the
// cap and the segment grows past the preallocated region, silently losing the
// benefit on every segment's tail; preallocate more and the excess is wasted on
// every segment. Deriving both from here makes disagreement impossible rather
// than merely unlikely - see docs/kdb-segment-preallocation-plan.md.
func EffectiveMaxSegmentBytes(configured int64) int64 {
	if configured <= 0 {
		return DefaultDeltaMaxSegmentBytes
	}
	return configured
}

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
	max := EffectiveMaxSegmentBytes(w.config.DeltaMaxSegmentBytes)
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

	// scans caches each segment's ref under the stamp it had when scanned, for shims that can
	// stamp (storage.SegmentStamper). A sealed segment does not change, so scanning it again on
	// every ListSegments - several times per checkpoint, the whole log each time - bought
	// nothing; the segment still open for appends is never stamped and so always rescanned.
	scansMu sync.Mutex
	scans   map[string]cachedScan
	// fps are fingerprints learned without a scan - from a checkpoint whose stamps still match -
	// keyed by sequence. Guarded by scansMu. See RememberFingerprint.
	fps map[int64]rememberedFP
	// frames caches decompressed frame payloads for single-frame reads. See SetFrameCache.
	frames atomic.Pointer[FrameCache]
}

type rememberedFP struct {
	stamp storage.SegmentStamp
	size  int64
	last  codec.Hash
}

type cachedScan struct {
	stamp storage.SegmentStamp
	ref   storage.DeltaSegmentRef
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
//
// A segment it cannot scan is an *UnreadableSegmentError, never a silent omission: leaving it out
// would drop every commit in it from replay, checkpoints and history. A segment whose intact
// frames stop early still lists, up to the stop, with Tail saying what follows it: zero padding
// (clean), a torn tail, or damage that intact frames follow (storage.TailKind). Listing is not
// where damage is refused, because a checkpoint may already hold everything a damaged segment
// recorded (embed's damagedInPlace); replay refuses a damaged segment it actually has to read.
func (r *DefaultReader) ListSegments() ([]storage.DeltaSegmentRef, error) {
	return r.listSegments(-1)
}

// ListSegmentsAfter is ListSegments limited to sequence numbers above afterSequence. Segments at
// or below it are not scanned at all.
func (r *DefaultReader) ListSegmentsAfter(afterSequence int64) ([]storage.DeltaSegmentRef, error) {
	return r.listSegments(afterSequence)
}

var _ storage.DeltaSegmentRangeLister = (*DefaultReader)(nil)

// StampSegment stamps a segment by sequence number, for a caller that recorded a stamp and wants
// to know whether the segment is still what it was then.
func (r *DefaultReader) StampSegment(sequence int64) (storage.SegmentStamp, bool, error) {
	stamper, can := r.shim.(storage.SegmentStamper)
	if !can {
		return storage.SegmentStamp{}, false, nil
	}
	return stamper.SegmentStamp(storio.SegmentNameBuilder.DeltaSequenced(r.namespaceID, sequence))
}

var _ storage.DeltaSegmentStamper = (*DefaultReader)(nil)

// RememberFingerprint records what a segment's fingerprint was under stamp - from a checkpoint
// open trusted on its stamps, or one just written - so the next checkpoint need not scan the
// segment again while its stamp is unchanged.
func (r *DefaultReader) RememberFingerprint(sequence int64, stamp storage.SegmentStamp, sizeBytes int64, lastCommit codec.Hash) {
	r.scansMu.Lock()
	defer r.scansMu.Unlock()
	if r.fps == nil {
		r.fps = make(map[int64]rememberedFP)
	}
	r.fps[sequence] = rememberedFP{stamp: stamp, size: sizeBytes, last: lastCommit}
}

// RememberedFingerprint returns a segment's fingerprint without reading it, when one was
// remembered or scanned under the stamp the segment still has.
func (r *DefaultReader) RememberedFingerprint(sequence int64) (storage.SegmentStamp, int64, codec.Hash, bool) {
	now, ok, err := r.StampSegment(sequence)
	if err != nil || !ok {
		return storage.SegmentStamp{}, 0, codec.Hash{}, false
	}
	name := storio.SegmentNameBuilder.DeltaSequenced(r.namespaceID, sequence)
	r.scansMu.Lock()
	defer r.scansMu.Unlock()
	if fp, ok := r.fps[sequence]; ok && fp.stamp == now {
		return now, fp.size, fp.last, true
	}
	if c, ok := r.scans[name]; ok && c.stamp == now {
		return now, c.ref.SizeBytes, c.ref.LastCommitHash, true
	}
	return storage.SegmentStamp{}, 0, codec.Hash{}, false
}

// SegmentRef scans one segment by sequence number, through the scan cache.
func (r *DefaultReader) SegmentRef(sequence int64) (*storage.DeltaSegmentRef, error) {
	name := storio.SegmentNameBuilder.DeltaSequenced(r.namespaceID, sequence)
	ref, err := r.cachedScanSegmentRef(name, sequence)
	if err == nil && ref == nil {
		err = errors.New("scan produced no segment reference")
	}
	return ref, err
}

var _ storage.DeltaFingerprintMemo = (*DefaultReader)(nil)

func (r *DefaultReader) listSegments(afterSequence int64) ([]storage.DeltaSegmentRef, error) {
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
		if seq <= afterSequence {
			continue
		}
		ref, err := r.cachedScanSegmentRef(name, seq)
		if err == nil && ref == nil {
			err = errors.New("scan produced no segment reference")
		}
		if err != nil {
			// A segment deleted between the listing above and this read - truncation running
			// alongside a reader - is not damage: it is simply no longer part of the log.
			// Anything else is. Skipping it used to drop every commit in it from replay,
			// checkpoints and history without a word (issue #43, U6c).
			if gone, listErr := r.segmentGone(name); listErr == nil && gone {
				continue
			}
			return nil, &UnreadableSegmentError{NamespaceID: r.namespaceID, Sequence: seq, Name: name, Err: err}
		}
		out = append(out, *ref)
	}
	return out, nil
}

// SegmentSequences lists the namespace's delta segment sequence numbers, ascending, from their
// names alone - no segment is read. For callers that need the lowest or highest and nothing else.
func (r *DefaultReader) SegmentSequences() ([]int64, error) {
	prefix := storio.SegmentNameBuilder.NamespacePrefix(r.namespaceID) + "delta/"
	names, err := r.shim.ListSegments(r.namespaceID)
	if err != nil {
		return nil, err
	}
	var out []int64
	for _, name := range names {
		if len(name) < len(prefix) || name[:len(prefix)] != prefix {
			continue
		}
		seq, ok := storio.ParseDeltaSequencedFileName(name[len(prefix):])
		if !ok {
			return nil, &LegacySegmentFormatError{NamespaceID: r.namespaceID, Names: []string{name}}
		}
		out = append(out, seq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

var _ storage.DeltaSegmentSequenceLister = (*DefaultReader)(nil)

// cachedScanSegmentRef is scanSegmentRef, answered from the cache when the segment's stamp is
// unchanged since it was last scanned.
func (r *DefaultReader) cachedScanSegmentRef(name string, seq int64) (*storage.DeltaSegmentRef, error) {
	stamper, can := r.shim.(storage.SegmentStamper)
	if !can {
		return r.scanSegmentRef(name, seq)
	}
	stamp, ok, err := stamper.SegmentStamp(name)
	if err != nil || !ok {
		r.forgetScan(name)
		return r.scanSegmentRef(name, seq)
	}
	r.scansMu.Lock()
	c, hit := r.scans[name]
	r.scansMu.Unlock()
	if hit && c.stamp == stamp {
		ref := c.ref
		return &ref, nil
	}
	ref, err := r.scanSegmentRef(name, seq)
	if err != nil || ref == nil {
		r.forgetScan(name)
		return ref, err
	}
	// Stamp again after the scan: a write that landed during it must not be cached as the
	// state the scan saw.
	if after, ok2, err2 := stamper.SegmentStamp(name); err2 == nil && ok2 && after == stamp {
		r.scansMu.Lock()
		if r.scans == nil {
			r.scans = make(map[string]cachedScan)
		}
		r.scans[name] = cachedScan{stamp: stamp, ref: *ref}
		r.scansMu.Unlock()
	}
	return ref, nil
}

func (r *DefaultReader) forgetScan(name string) {
	r.scansMu.Lock()
	delete(r.scans, name)
	r.scansMu.Unlock()
}

// segmentGone reports whether name has disappeared from the namespace's listing.
func (r *DefaultReader) segmentGone(name string) (bool, error) {
	names, err := r.shim.ListSegments(r.namespaceID)
	if err != nil {
		return false, err
	}
	for _, n := range names {
		if n == name {
			return false, nil
		}
	}
	return true, nil
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
		pos         int64
		window      = int64(segmentScanWindowBytes)
		// stopped is set when the walk ended somewhere other than the end of the file - at a
		// torn tail, zero padding or damage. stopReason says why, when the walk knows.
		stopped    bool
		stopReason string
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
			// A frame failing its CRC ends the walk; what follows it decides whether that
			// is a torn tail or damage.
			pos, stopped, stopReason = walk.ConsumedEnd, true, corrupt.Reason
			break
		}
		eof := int64(len(raw)) < window
		rest := raw[walk.ConsumedEnd-pos:]
		pos = walk.ConsumedEnd
		if len(rest) == 0 {
			if eof {
				break
			}
			window = int64(segmentScanWindowBytes)
			continue
		}
		if !eof {
			// The window may simply have ended inside a frame (or its header). Read on from
			// it, with a window large enough for the whole frame, before concluding anything.
			if len(rest) < frameHeaderSize {
				window = int64(segmentScanWindowBytes)
				continue
			}
			if n, ok := validFrameHeader(rest[:frameHeaderSize]); ok && frameHeaderSize+n > len(rest) {
				window = int64(segmentScanWindowBytes)
				if need := int64(frameHeaderSize + n); need > window {
					window = need
				}
				continue
			}
		}
		stopped, stopReason = true, describeStop(rest)
		break
	}

	tail := storage.SegmentTail{Offset: pos, NextIntactFrame: -1}
	if stopped {
		var err error
		tail, err = r.tailAfter(segmentName, pos, stopReason)
		if err != nil {
			return nil, err
		}
	}
	total := pos

	zero, _ := codec.HashFromBytes(make([]byte, 32))
	if firstOffset < 0 {
		return &storage.DeltaSegmentRef{
			NamespaceID: r.namespaceID, SequenceNumber: seq,
			FirstCommitHash: zero, LastCommitHash: zero,
			SizeBytes: total, Compression: r.config.CompressionCodec,
			Tail: tail,
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
		Tail:            tail,
	}, nil
}

// tailAfter classifies the bytes from stop - where a segment's intact frames end - to the end of
// its file (see storage.TailKind), a window at a time. Only zero bytes there is a clean end: a
// preallocated segment's unused space. Anything else is a torn tail, unless an intact frame
// starts somewhere in it, which makes it damage.
//
// Searching for intact frames costs a pass over the rest of the file, which for a preallocated
// segment is its zero-filled remainder; the walk this replaces read that remainder too, in
// doubling windows. A candidate is checked only where the frame magic appears.
func (r *DefaultReader) tailAfter(segmentName string, stop int64, reason string) (storage.SegmentTail, error) {
	tail := storage.SegmentTail{Offset: stop, Reason: reason, NextIntactFrame: -1}
	nonZero := false
	at := stop
	window := int64(segmentScanWindowBytes)
	for {
		raw, err := r.shim.ReadFromSegment(segmentName, at, int(window))
		if err != nil {
			return tail, err
		}
		if len(raw) == 0 {
			break
		}
		if !nonZero && !allZero(raw) {
			nonZero = true
		}
		for i := 0; ; {
			j := bytes.Index(raw[i:], kdbpMagic)
			if j < 0 {
				break
			}
			c := at + int64(i+j)
			if c > stop {
				ok, err := r.intactFrameAtOffset(segmentName, c)
				if err != nil {
					return tail, err
				}
				if ok {
					tail.Kind = storage.TailDamaged
					tail.NextIntactFrame = c
					return tail, nil
				}
			}
			i += j + 1
		}
		if int64(len(raw)) < window {
			break
		}
		// Overlap by less than a magic's length, so one that straddles two windows is
		// still seen whole.
		at += int64(len(raw)) - int64(len(kdbpMagic)-1)
	}
	if nonZero {
		tail.Kind = storage.TailTorn
	} else {
		tail.Reason = ""
	}
	return tail, nil
}

// intactFrameAtOffset is intactFrameAt for a frame in a segment file, reading only that frame.
func (r *DefaultReader) intactFrameAtOffset(segmentName string, offset int64) (bool, error) {
	header, err := r.shim.ReadFromSegment(segmentName, offset, frameHeaderSize)
	if err != nil {
		return false, err
	}
	n, ok := validFrameHeader(header)
	if !ok {
		return false, nil
	}
	frame, err := r.shim.ReadFromSegment(segmentName, offset, frameHeaderSize+n)
	if err != nil {
		return false, err
	}
	_, ok = intactFrameAt(frame, 0)
	return ok, nil
}

// commitAtOffset decodes the single frame starting at offset, reading only
// that frame's bytes.
func (r *DefaultReader) commitAtOffset(segmentName string, offset int64) (document.Commit, error) {
	return r.readCommitFrame(segmentName, offset)
}

// frameProbeBytes is how much readFrame reads on its first try: enough for the header and,
// for most commits, the whole frame, so one read usually suffices where two always did.
const frameProbeBytes = 4 << 10

// readCommitFrame decodes the single frame at offset, reading only that frame's bytes - or none,
// when the frame cache holds its payload.
func (r *DefaultReader) readCommitFrame(segmentName string, offset int64) (document.Commit, error) {
	var commit document.Commit
	err := r.withFramePayload(segmentName, offset, func(payload []byte) error {
		c, err := document.FromPayloadBytes(payload)
		if err != nil {
			return &CorruptFrameError{Offset: int(offset), Reason: err.Error()}
		}
		commit = c
		return nil
	})
	return commit, err
}

// withFramePayload calls fn with the decompressed payload of the frame at offset in segmentName.
// The payload is valid only during the call: nothing fn keeps may alias it, which FromPayloadBytes
// and ScanCommitWrites' callers both honour (see PageCodec.parseCommit).
//
// The payload comes from the frame cache when it holds this frame of this incarnation of the
// segment (see FrameCache), and otherwise from disk: one frame read, its CRC checked, its body
// decompressed - after which a cache-enabled reader keeps a copy for the next read. A frame that
// fails its CRC or will not parse as a frame is reported as a *CorruptFrameError and never cached.
func (r *DefaultReader) withFramePayload(segmentName string, offset int64, fn func(payload []byte) error) error {
	cache := r.frames.Load()
	var key frameKey
	cacheable := false
	if cache != nil {
		if g, ok := r.shim.(storage.SegmentGenerationer); ok {
			if gen, ok := g.SegmentGeneration(segmentName); ok {
				key, cacheable = frameKey{gen: gen, off: offset}, true
				if bp, hit := cache.get(key); hit {
					err := fn(*bp)
					bufpool.Put(bp)
					return err
				}
			}
		}
	}
	frame, frameBuf, err := r.readFrame(segmentName, offset)
	if err != nil {
		return err
	}
	defer bufpool.Put(frameBuf)
	frameEnd, ok := frameBounds(frame, 0)
	if !ok {
		return &CorruptFrameError{Offset: int(offset), Reason: "no complete frame at this offset"}
	}
	frame = frame[:frameEnd]
	if err := verifyFrameCRC(frame, int(offset)); err != nil {
		return err
	}
	payload, payloadBuf, err := framePayload(frame)
	if err != nil {
		return &CorruptFrameError{Offset: int(offset), Reason: err.Error()}
	}
	defer bufpool.Put(payloadBuf)
	if cacheable {
		cache.put(key, payload)
	}
	return fn(payload)
}

// readFrame reads the frame at offset: its header and as much of the body as the header declares
// and the segment holds. Whether that is a complete frame is the caller's to check.
//
// Through a shim that can read into the caller's buffer (storage.SegmentIntoReader) the frame
// is read into a pooled buffer, returned as buf for the caller to bufpool.Put once done - and
// usually in one read, since the first one fetches frameProbeBytes and most frames fit. Otherwise,
// and for a frame too large to pool, it reads the header and then the frame into fresh memory,
// and buf is nil.
func (r *DefaultReader) readFrame(segmentName string, offset int64) (frame []byte, buf *[]byte, err error) {
	into, ok := r.shim.(storage.SegmentIntoReader)
	if !ok {
		frame, err := r.readFrameAlloc(segmentName, offset)
		return frame, nil, err
	}
	bp := bufpool.Get(frameProbeBytes)
	n, err := into.ReadFromSegmentInto(segmentName, offset, *bp)
	if err != nil {
		bufpool.Put(bp)
		return nil, nil, err
	}
	if n < PageFrameHeaderSize {
		bufpool.Put(bp)
		return nil, nil, &CorruptFrameError{Offset: int(offset), Reason: "segment ends inside the frame header"}
	}
	frameLen := PageFrameHeaderSize + readIntBE(*bp, 8)
	if frameLen > n && n == len(*bp) {
		// The frame continues past the probe.
		if frameLen > bufpool.MaxPooled {
			bufpool.Put(bp)
			frame, err := r.readFrameAlloc(segmentName, offset)
			return frame, nil, err
		}
		big := bufpool.Get(frameLen)
		copy(*big, (*bp)[:n])
		bufpool.Put(bp)
		bp = big
		m, err := into.ReadFromSegmentInto(segmentName, offset+int64(n), (*bp)[n:])
		if err != nil {
			bufpool.Put(bp)
			return nil, nil, err
		}
		n += m
	}
	if n > frameLen {
		n = frameLen
	}
	return (*bp)[:n], bp, nil
}

// readFrameAlloc is readFrame through plain ReadFromSegment: a header read, then the frame into
// a fresh buffer.
func (r *DefaultReader) readFrameAlloc(segmentName string, offset int64) ([]byte, error) {
	header, err := r.shim.ReadFromSegment(segmentName, offset, PageFrameHeaderSize)
	if err != nil {
		return nil, err
	}
	if len(header) < PageFrameHeaderSize {
		return nil, &CorruptFrameError{Offset: int(offset), Reason: "segment ends inside the frame header"}
	}
	return r.shim.ReadFromSegment(segmentName, offset, PageFrameHeaderSize+readIntBE(header, 8))
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
	return r.readCommitFrame(segmentName, frameOffset)
}

// ReadPayloadAt implements storage.DeltaPayloadReader: the frame's decompressed payload, through
// the frame cache, with no decode.
func (r *DefaultReader) ReadPayloadAt(segment storage.DeltaSegmentRef, frameOffset int64, fn func(payload []byte) error) error {
	segmentName := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	return r.withFramePayload(segmentName, frameOffset, fn)
}

// ReadDocumentAt implements storage.DeltaDocumentReader: it calls fn with the patch of every write
// of docID in the commit framed at frameOffset, in order, until fn returns false - without
// decoding the rest of the commit (see document.ScanCommitWrites). The patch is valid only during
// the call; a caller that keeps it copies it.
//
// This is the cold point read: one version out of a commit that may hold hundreds. It costs the
// frame read and decompression ReadCommitAt does (both skipped when the frame cache holds the
// frame) but none of the decode.
func (r *DefaultReader) ReadDocumentAt(segment storage.DeltaSegmentRef, frameOffset int64, docID codec.UUID, fn func(patch []byte) bool) error {
	segmentName := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	return r.withFramePayload(segmentName, frameOffset, func(payload []byte) error {
		if err := document.ScanCommitWrites(payload, docID, fn); err != nil {
			return &CorruptFrameError{Offset: int(frameOffset), Reason: err.Error()}
		}
		return nil
	})
}

// SetFrameCache puts c in front of this reader's single-frame reads (ReadCommitAt, ReadDocumentAt).
// nil removes it. A cache may be shared by readers of different namespaces: keys name segment
// incarnations, which are unique across the process. Only shims that report segment generations
// (storage.SegmentGenerationer) are cached; through any other, reads go to disk as before.
func (r *DefaultReader) SetFrameCache(c *FrameCache) { r.frames.Store(c) }

// FrameCache reports the cache SetFrameCache installed, or nil.
func (r *DefaultReader) FrameCache() *FrameCache { return r.frames.Load() }

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

// UnreadableSegmentError reports a delta segment ListSegments found but could not read: an I/O
// error, or a frame that passes its CRC and still will not decode (a damaged header field, which
// the body CRC does not cover). Neither is the shape of a torn tail - a write cut short leaves a
// frame that is missing bytes or fails its CRC, and the scan stops cleanly before it - so this
// is reported wherever the segment is, newest or not. Detect with errors.As; the cause stays
// reachable through Unwrap.
type UnreadableSegmentError struct {
	NamespaceID string
	Sequence    int64
	Name        string
	Err         error
}

func (e *UnreadableSegmentError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q: delta segment %d (%s) cannot be read, so the commits in it cannot be "+
			"recovered from this log - run 'kdb-inspect verify' to classify the damage and "+
			"'kdb-inspect repair-segments' to repair it: %v",
		e.NamespaceID, e.Sequence, e.Name, e.Err)
}

func (e *UnreadableSegmentError) Unwrap() error { return e.Err }

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

// SkimSegment implements storage.DamageTolerantReader: it reads segment's whole file - not just
// the readable prefix its ref records - and reports its physical size, the last commit any intact
// frame holds, and the offsets of frames that are damaged.
func (r *DefaultReader) SkimSegment(segment storage.DeltaSegmentRef) (storage.SegmentSkim, error) {
	segmentName := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	raw, err := r.shim.ReadFromSegment(segmentName, 0, maxScannedSegmentBytes)
	if err != nil {
		return storage.SegmentSkim{}, err
	}
	skim := storage.SegmentSkim{PhysicalSize: int64(len(raw))}
	lastGood := -1
	skipped, framesEnd, err := scanSkippingCorrupt(raw, func(s ScannedCommit) error {
		skim.LastCommitHash = s.CommitHash
		lastGood = s.FrameOffset
		return nil
	})
	skim.FramesEnd = int64(framesEnd)
	skim.TrailingUnreadable = framesEnd < len(raw) && !allZero(raw[framesEnd:])
	if len(skipped) > 0 && skipped[len(skipped)-1] > lastGood {
		skim.LastFrameDamaged = true
	}
	for _, o := range skipped {
		skim.DamagedFrames = append(skim.DamagedFrames, int64(o))
	}
	return skim, err
}

// StreamCommitsPastDamage implements storage.DamageTolerantReader: StreamCommits over the whole
// file, skipping damaged frames instead of stopping at the first.
func (r *DefaultReader) StreamCommitsPastDamage(segment storage.DeltaSegmentRef, fn func(document.Commit, int64) error) error {
	segmentName := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	raw, err := r.shim.ReadFromSegment(segmentName, 0, maxScannedSegmentBytes)
	if err != nil {
		return err
	}
	_, err = ScanSkippingCorrupt(raw, func(s ScannedCommit) error { return fn(s.Commit, int64(s.FrameOffset)) })
	return err
}
