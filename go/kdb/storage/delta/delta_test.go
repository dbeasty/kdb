package delta

import (
	"errors"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/compression"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

const testNS = "delta/ns"

// newTestShim builds the same file-backed shim production uses (see
// backup_test.go / integrity's testdata_test.go for the shared pattern),
// rooted in a per-test temp dir so torn-tail and corruption tests exercise
// the real on-disk byte path rather than a mock.
func newTestShim(t *testing.T) storage.PlatformIOShim {
	t.Helper()
	root := t.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store)
}

func newConfig(shim storage.PlatformIOShim, comp storage.CompressionCodec) storage.StorageEngineConfig {
	return storage.StorageEngineConfig{CompressionCodec: comp, IOShim: shim}
}

// buildCommit constructs one valid, hash-consistent commit. parent is nil
// for a genesis commit. Mirrors integrity's testdata_test.go so segment
// contents parse through the real document.FromPayloadBytes path.
func buildCommit(t *testing.T, parent *codec.Hash) document.Commit {
	t.Helper()
	tx, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	author, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	var parents []codec.Hash
	if parent != nil {
		parents = []codec.Hash{*parent}
	}
	c := document.Commit{
		ParentHashes:     parents,
		NamespaceID:      testNS,
		TransactionID:    tx,
		Timestamp:        codec.TimestampNow(),
		AuthorNodeID:     author,
		Operations:       []document.Op{document.WriteOp{DocID: docID, Patch: "{}"}},
		DocumentTreeHash: document.EmptyDocumentTree().TreeHash,
		Message:          "test",
	}
	h, err := document.ComputeCommitHash(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Hash = h
	return c
}

// buildChain builds n commits, each the sole parent of the next.
func buildChain(t *testing.T, n int) []document.Commit {
	t.Helper()
	commits := make([]document.Commit, 0, n)
	var parent *codec.Hash
	for i := 0; i < n; i++ {
		c := buildCommit(t, parent)
		commits = append(commits, c)
		h := c.Hash
		parent = &h
	}
	return commits
}

// rawFrame encodes one commit as a KDBP frame with the given codec, for
// tests that need exact control over segment bytes.
func rawFrame(t *testing.T, c document.Commit, comp storage.CompressionCodec) []byte {
	t.Helper()
	payload, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := (PageCodec{}).Frame(payload, comp)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func record(t *testing.T, c document.Commit) storage.DeltaRecord {
	t.Helper()
	payload, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	return storage.DeltaRecord{CommitHash: c.Hash, NamespaceID: testNS, CommitPayload: payload}
}

// writeSealedSegment writes commits through a real writer at seq and seals
// it, returning the ref Seal produced.
func writeSealedSegment(t *testing.T, cfg storage.StorageEngineConfig, seq int64, commits ...document.Commit) storage.DeltaSegmentRef {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	w := NewDefaultWriter(testNS, id, seq, cfg.IOShim, cfg)
	for _, c := range commits {
		if _, err := w.Append(record(t, c)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	ref, err := w.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func zeroHash(t *testing.T) codec.Hash {
	t.Helper()
	z, err := codec.HashFromBytes(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	return z
}

// TestWriteThenReadAllRoundTrip is the core durability contract: commits
// appended through the writer come back byte- and hash-identical through
// the reader, for both supported compression codecs.
func TestWriteThenReadAllRoundTrip(t *testing.T) {
	codecs := map[string]storage.CompressionCodec{
		"none": storage.CompressionNone,
		"zstd": storage.CompressionZSTD,
	}
	for name, comp := range codecs {
		t.Run(name, func(t *testing.T) {
			cfg := newConfig(newTestShim(t), comp)
			commits := buildChain(t, 3)
			ref := writeSealedSegment(t, cfg, 0, commits...)

			if ref.SequenceNumber != 0 {
				t.Errorf("SequenceNumber = %d, want 0", ref.SequenceNumber)
			}
			if ref.NamespaceID != testNS {
				t.Errorf("NamespaceID = %q, want %q", ref.NamespaceID, testNS)
			}
			if ref.FirstCommitHash != commits[0].Hash {
				t.Errorf("FirstCommitHash = %v, want first appended commit's hash", ref.FirstCommitHash)
			}
			if ref.LastCommitHash != commits[2].Hash {
				t.Errorf("LastCommitHash = %v, want last appended commit's hash", ref.LastCommitHash)
			}
			if ref.SizeBytes <= 0 {
				t.Errorf("SizeBytes = %d, want > 0", ref.SizeBytes)
			}
			if ref.Compression != comp {
				t.Errorf("Compression = %v, want %v", ref.Compression, comp)
			}

			records, err := NewDefaultReader(testNS, cfg.IOShim, cfg).ReadAll(ref)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if len(records) != len(commits) {
				t.Fatalf("ReadAll returned %d records, want %d", len(records), len(commits))
			}
			for i, rec := range records {
				if rec.CommitHash != commits[i].Hash {
					t.Errorf("record %d hash = %v, want %v", i, rec.CommitHash, commits[i].Hash)
				}
				// The payload must be replayable: parse it back and confirm
				// the reconstructed commit is hash-consistent with the input.
				c, err := document.FromPayloadBytes(rec.CommitPayload)
				if err != nil {
					t.Fatalf("record %d payload does not parse: %v", i, err)
				}
				if c.Hash != commits[i].Hash {
					t.Errorf("record %d reparsed hash = %v, want %v", i, c.Hash, commits[i].Hash)
				}
			}
		})
	}
}

// TestWriterAppendOffsetsAndSize pins the offset contract callers index
// on: Append returns the frame's start offset, i.e. the segment size just
// before the append, and CurrentSizeBytes tracks the running total.
func TestWriterAppendOffsetsAndSize(t *testing.T) {
	cfg := newConfig(newTestShim(t), storage.CompressionNone)
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	w := NewDefaultWriter(testNS, id, 0, cfg.IOShim, cfg)
	if got := w.CurrentSizeBytes(); got != 0 {
		t.Fatalf("CurrentSizeBytes before any append = %d, want 0", got)
	}
	if w.NamespaceID() != testNS {
		t.Errorf("NamespaceID() = %q, want %q", w.NamespaceID(), testNS)
	}
	if w.SegmentID() != id {
		t.Errorf("SegmentID() = %v, want %v", w.SegmentID(), id)
	}
	for i, c := range buildChain(t, 3) {
		wantOffset := w.CurrentSizeBytes()
		frameLen := int64(len(rawFrame(t, c, storage.CompressionNone)))
		offset, err := w.Append(record(t, c))
		if err != nil {
			t.Fatal(err)
		}
		if offset != wantOffset {
			t.Errorf("append %d: offset = %d, want %d", i, offset, wantOffset)
		}
		if got := w.CurrentSizeBytes(); got != wantOffset+frameLen {
			t.Errorf("append %d: CurrentSizeBytes = %d, want %d", i, got, wantOffset+frameLen)
		}
	}
}

// TestSealSemantics: sealing is terminal - it flips IsSealed, refuses
// further appends, and cannot be repeated (a second Seal would re-run the
// shim-side seal and hand out a second ref for the same segment).
func TestSealSemantics(t *testing.T) {
	cfg := newConfig(newTestShim(t), storage.CompressionNone)
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	w := NewDefaultWriter(testNS, id, 0, cfg.IOShim, cfg)
	c := buildCommit(t, nil)
	if _, err := w.Append(record(t, c)); err != nil {
		t.Fatal(err)
	}
	if w.IsSealed() {
		t.Fatal("IsSealed = true before Seal")
	}
	ref, err := w.Seal()
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if !w.IsSealed() {
		t.Error("IsSealed = false after Seal")
	}
	if ref.SegmentID != id {
		t.Errorf("ref.SegmentID = %v, want writer's segment ID %v", ref.SegmentID, id)
	}
	if _, err := w.Append(record(t, buildCommit(t, nil))); err == nil {
		t.Error("Append after Seal succeeded, want error")
	}
	if _, err := w.Seal(); err == nil {
		t.Error("second Seal succeeded, want error")
	}
}

// TestSealEmptySegment: a writer sealed without appends (which
// Factory.OpenWriter can produce on every restart, by design) must yield
// a well-formed ref - zero-hash bounds, zero size - and read back empty.
func TestSealEmptySegment(t *testing.T) {
	cfg := newConfig(newTestShim(t), storage.CompressionNone)
	ref := writeSealedSegment(t, cfg, 0)
	zero := zeroHash(t)
	if ref.FirstCommitHash != zero || ref.LastCommitHash != zero {
		t.Errorf("empty segment ref hashes = %v/%v, want zero hashes", ref.FirstCommitHash, ref.LastCommitHash)
	}
	if ref.SizeBytes != 0 {
		t.Errorf("empty segment SizeBytes = %d, want 0", ref.SizeBytes)
	}
	records, err := NewDefaultReader(testNS, cfg.IOShim, cfg).ReadAll(ref)
	if err != nil {
		t.Fatalf("ReadAll on empty segment: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("ReadAll on empty segment returned %d records, want 0", len(records))
	}
}

// TestScanSegmentBytesTornTail: a frame whose declared length runs past
// the end of the segment is the expected shape of an unclean shutdown, so
// the scanner must stop cleanly (no error) with everything before it.
func TestScanSegmentBytesTornTail(t *testing.T) {
	commits := buildChain(t, 3)
	var seg []byte
	for _, c := range commits[:2] {
		seg = append(seg, rawFrame(t, c, storage.CompressionNone)...)
	}
	third := rawFrame(t, commits[2], storage.CompressionNone)
	// Cut the third frame after its header: length says "big body follows"
	// but the body never fully made it to disk.
	seg = append(seg, third[:frameHeaderSize+3]...)

	scanned, err := ScanSegmentBytes(seg, storage.CompressionNone)
	if err != nil {
		t.Fatalf("torn tail must not be an error, got: %v", err)
	}
	if len(scanned) != 2 {
		t.Fatalf("scanned %d commits, want the 2 complete ones", len(scanned))
	}
	for i, s := range scanned {
		if s.CommitHash != commits[i].Hash {
			t.Errorf("scanned commit %d hash = %v, want %v", i, s.CommitHash, commits[i].Hash)
		}
	}
}

// TestScanSegmentBytesShortHeaderTail: a tail shorter than one frame
// header (the write died mid-header) also stops cleanly.
func TestScanSegmentBytesShortHeaderTail(t *testing.T) {
	c := buildCommit(t, nil)
	frame := rawFrame(t, c, storage.CompressionNone)
	seg := append(append([]byte(nil), frame...), frame[:7]...)
	scanned, err := ScanSegmentBytes(seg, storage.CompressionNone)
	if err != nil {
		t.Fatalf("short header tail must not be an error, got: %v", err)
	}
	if len(scanned) != 1 || scanned[0].CommitHash != c.Hash {
		t.Fatalf("scanned = %d commits, want just the complete first one", len(scanned))
	}
	if scanned[0].FrameOffset != 0 {
		t.Errorf("FrameOffset = %d, want 0", scanned[0].FrameOffset)
	}
}

// TestScanSegmentBytesCorruptCRC: a frame that fully fits but whose body
// bytes contradict the stored CRC is *not* a silent stop - it must
// surface as a typed *CorruptFrameError carrying the frame's offset,
// while still returning every commit scanned before it (the torn-tail-
// tolerant replay contract on ScanSegmentBytes' doc comment).
func TestScanSegmentBytesCorruptCRC(t *testing.T) {
	commits := buildChain(t, 2)
	good := rawFrame(t, commits[0], storage.CompressionNone)
	bad := rawFrame(t, commits[1], storage.CompressionNone)
	// Flip one body byte: the declared length still fits, so only the CRC
	// check can catch this.
	bad[frameHeaderSize+2] ^= 0xFF
	seg := append(append([]byte(nil), good...), bad...)

	scanned, err := ScanSegmentBytes(seg, storage.CompressionNone)
	var corrupt *CorruptFrameError
	if !errors.As(err, &corrupt) {
		t.Fatalf("err = %v, want *CorruptFrameError", err)
	}
	if corrupt.Offset != len(good) {
		t.Errorf("CorruptFrameError.Offset = %d, want %d (start of the flipped frame)", corrupt.Offset, len(good))
	}
	if !strings.Contains(corrupt.Reason, "crc mismatch") {
		t.Errorf("Reason = %q, want a crc mismatch reason", corrupt.Reason)
	}
	// The rendered message must carry the offset - it's the only lead an
	// operator gets for locating the damage in the segment file.
	if !strings.Contains(corrupt.Error(), "offset") || !strings.Contains(corrupt.Error(), "crc mismatch") {
		t.Errorf("Error() = %q, want offset and reason in the message", corrupt.Error())
	}
	if len(scanned) != 1 || scanned[0].CommitHash != commits[0].Hash {
		t.Fatalf("partial result = %d commits, want the 1 commit before the corruption", len(scanned))
	}
}

// TestScanSegmentBytesUnparsableBody: a frame whose CRC matches but whose
// body is not a valid commit payload (e.g. the writer framed garbage) is
// corruption too, not a clean EOF.
func TestScanSegmentBytesUnparsableBody(t *testing.T) {
	frame, err := (PageCodec{}).Frame([]byte("not a commit payload"), storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	scanned, scanErr := ScanSegmentBytes(frame, storage.CompressionNone)
	var corrupt *CorruptFrameError
	if !errors.As(scanErr, &corrupt) {
		t.Fatalf("err = %v, want *CorruptFrameError", scanErr)
	}
	if corrupt.Offset != 0 {
		t.Errorf("Offset = %d, want 0", corrupt.Offset)
	}
	if len(scanned) != 0 {
		t.Errorf("scanned %d commits from a garbage-only segment, want 0", len(scanned))
	}
}

// TestScanSegmentBytesBadMagic: bytes after the last valid frame that
// don't start with the KDBP magic terminate the scan cleanly - the
// scanner never guesses at resynchronization.
func TestScanSegmentBytesBadMagic(t *testing.T) {
	c := buildCommit(t, nil)
	seg := append(rawFrame(t, c, storage.CompressionNone), []byte("XXXXXXXXXXXXXXXXXXXX")...)
	scanned, err := ScanSegmentBytes(seg, storage.CompressionNone)
	if err != nil {
		t.Fatalf("bad-magic tail must not be an error, got: %v", err)
	}
	if len(scanned) != 1 || scanned[0].CommitHash != c.Hash {
		t.Fatalf("scanned = %d commits, want just the valid first one", len(scanned))
	}
}

// TestScanSegmentBytesEmpty: an empty (or all-truncated) segment is not
// an error - it's what a freshly opened, never-appended segment looks
// like.
func TestScanSegmentBytesEmpty(t *testing.T) {
	for _, seg := range [][]byte{nil, {}, {0x4B, 0x44, 0x42}} {
		scanned, err := ScanSegmentBytes(seg, storage.CompressionNone)
		if err != nil {
			t.Errorf("ScanSegmentBytes(%d bytes) err = %v, want nil", len(seg), err)
		}
		if len(scanned) != 0 {
			t.Errorf("ScanSegmentBytes(%d bytes) = %d commits, want 0", len(seg), len(scanned))
		}
	}
}

// TestReadAllPartialOnCorruption: the reader-level contract layered on
// the scanner's - ReadAll returns both the intact prefix AND the typed
// error, so replay callers can keep the good commits (kdb-spec-layer13
// Component 47 §4.3).
func TestReadAllPartialOnCorruption(t *testing.T) {
	shim := newTestShim(t)
	cfg := newConfig(shim, storage.CompressionNone)
	commits := buildChain(t, 2)
	good := rawFrame(t, commits[0], storage.CompressionNone)
	bad := rawFrame(t, commits[1], storage.CompressionNone)
	bad[frameHeaderSize] ^= 0xFF
	name := storio.SegmentNameBuilder.DeltaSequenced(testNS, 0)
	var size int64
	var err error
	for _, f := range [][]byte{good, bad} {
		if size, err = shim.AppendToSegment(name, f); err != nil {
			t.Fatal(err)
		}
	}
	ref := storage.DeltaSegmentRef{NamespaceID: testNS, SequenceNumber: 0, SizeBytes: size, Compression: storage.CompressionNone}

	records, err := NewDefaultReader(testNS, shim, cfg).ReadAll(ref)
	var corrupt *CorruptFrameError
	if !errors.As(err, &corrupt) {
		t.Fatalf("ReadAll err = %v, want *CorruptFrameError", err)
	}
	if len(records) != 1 || records[0].CommitHash != commits[0].Hash {
		t.Fatalf("ReadAll partial = %d records, want the 1 intact commit", len(records))
	}
}

// TestListSegmentsSequenceOrder: refs must come back in sequence order
// regardless of creation order, purely because zero-padded file names
// sort lexicographically as numbers - including across the 9->10 digit
// boundary that unpadded names would misorder. This ordering IS the
// replay order, so getting it wrong is data corruption (the Component 47
// bug).
func TestListSegmentsSequenceOrder(t *testing.T) {
	cfg := newConfig(newTestShim(t), storage.CompressionNone)
	// Creation order is deliberately shuffled; 9,10,11 crosses the digit
	// boundary where "10" < "9" lexicographically without padding.
	seqs := []int64{11, 9, 10}
	bySeq := make(map[int64][]document.Commit)
	for _, seq := range seqs {
		commits := buildChain(t, 2)
		bySeq[seq] = commits
		writeSealedSegment(t, cfg, seq, commits...)
	}

	refs, err := NewDefaultReader(testNS, cfg.IOShim, cfg).ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("ListSegments returned %d refs, want 3", len(refs))
	}
	for i, wantSeq := range []int64{9, 10, 11} {
		ref := refs[i]
		if ref.SequenceNumber != wantSeq {
			t.Fatalf("refs[%d].SequenceNumber = %d, want %d (sequence order)", i, ref.SequenceNumber, wantSeq)
		}
		commits := bySeq[wantSeq]
		if ref.FirstCommitHash != commits[0].Hash || ref.LastCommitHash != commits[1].Hash {
			t.Errorf("seq %d ref commit bounds don't match the segment's first/last commits", wantSeq)
		}
	}
}

// TestListSegmentsIgnoresOtherKindsAndHandlesEmpty: non-delta segments
// under the same namespace are invisible, and a zero-length delta
// segment (created but never appended to) lists as a zero-hash,
// zero-size ref rather than an error.
func TestListSegmentsIgnoresOtherKindsAndHandlesEmpty(t *testing.T) {
	shim := newTestShim(t)
	cfg := newConfig(shim, storage.CompressionNone)
	c := buildCommit(t, nil)
	writeSealedSegment(t, cfg, 0, c)
	// A WAL segment in the same namespace must not show up as a delta ref.
	if _, err := shim.AppendToSegment(storio.SegmentNameBuilder.WAL(testNS, "wal-0"), []byte("wal bytes")); err != nil {
		t.Fatal(err)
	}
	// An empty delta segment: file exists, zero bytes.
	if _, err := shim.AppendToSegment(storio.SegmentNameBuilder.DeltaSequenced(testNS, 1), nil); err != nil {
		t.Fatal(err)
	}

	refs, err := NewDefaultReader(testNS, shim, cfg).ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("ListSegments returned %d refs, want 2 (delta segments only)", len(refs))
	}
	if refs[0].SequenceNumber != 0 || refs[0].FirstCommitHash != c.Hash {
		t.Errorf("refs[0] = seq %d, want the populated seq-0 segment", refs[0].SequenceNumber)
	}
	zero := zeroHash(t)
	if refs[1].SequenceNumber != 1 || refs[1].SizeBytes != 0 || refs[1].FirstCommitHash != zero {
		t.Errorf("refs[1] = {seq %d, size %d}, want empty seq-1 segment with zero hashes", refs[1].SequenceNumber, refs[1].SizeBytes)
	}
}

// TestFactoryOpenWriterAssignsNextSequence: each OpenWriter must pick one
// past the highest existing sequence (0 for a fresh namespace), even
// across gaps - reusing a sequence number would collide two segments on
// one file name.
func TestFactoryOpenWriterAssignsNextSequence(t *testing.T) {
	shim := newTestShim(t)
	cfg := newConfig(shim, storage.CompressionNone)
	factory := Factory{Config: cfg}

	w0, err := factory.OpenWriter(testNS)
	if err != nil {
		t.Fatalf("OpenWriter on fresh namespace: %v", err)
	}
	if _, err := w0.Append(record(t, buildCommit(t, nil))); err != nil {
		t.Fatal(err)
	}
	ref0, err := w0.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if ref0.SequenceNumber != 0 {
		t.Fatalf("first writer sequence = %d, want 0", ref0.SequenceNumber)
	}

	// OpenReader is a trivial constructor, but pin that it binds the
	// namespace it was asked for.
	if r := factory.OpenReader(testNS); r.NamespaceID() != testNS {
		t.Errorf("OpenReader namespace = %q, want %q", r.NamespaceID(), testNS)
	}

	w1, err := factory.OpenWriter(testNS)
	if err != nil {
		t.Fatalf("second OpenWriter: %v", err)
	}
	ref1, err := w1.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if ref1.SequenceNumber != 1 {
		t.Fatalf("second writer sequence = %d, want 1", ref1.SequenceNumber)
	}

	// A gap (e.g. manual segment copy, or deleted middle segments) must
	// still yield max+1, not first-free - order is what matters.
	if _, err := shim.AppendToSegment(storio.SegmentNameBuilder.DeltaSequenced(testNS, 7), rawFrame(t, buildCommit(t, nil), storage.CompressionNone)); err != nil {
		t.Fatal(err)
	}
	w8, err := factory.OpenWriter(testNS)
	if err != nil {
		t.Fatal(err)
	}
	ref8, err := w8.Seal()
	if err != nil {
		t.Fatal(err)
	}
	if ref8.SequenceNumber != 8 {
		t.Fatalf("post-gap writer sequence = %d, want 8", ref8.SequenceNumber)
	}
}

// TestLegacySegmentRefusal: a namespace containing any pre-Layer-13
// (random-UUID-named) delta segment must refuse both writing and listing
// with the typed *LegacySegmentFormatError pointing at the repair tool -
// silently ordering those segments by name was the original
// unopenable-namespace bug (kdb-spec-layer13 Component 47 §4.1).
func TestLegacySegmentRefusal(t *testing.T) {
	shim := newTestShim(t)
	cfg := newConfig(shim, storage.CompressionNone)
	legacyName := storio.SegmentNameBuilder.Delta(testNS, "3f8a2c44-9d1e-4b7a-8c55-1f0e6d2ab901.seg")
	if _, err := shim.AppendToSegment(legacyName, rawFrame(t, buildCommit(t, nil), storage.CompressionNone)); err != nil {
		t.Fatal(err)
	}

	_, err := Factory{Config: cfg}.OpenWriter(testNS)
	var legacy *LegacySegmentFormatError
	if !errors.As(err, &legacy) {
		t.Fatalf("OpenWriter err = %v, want *LegacySegmentFormatError", err)
	}
	if legacy.NamespaceID != testNS || len(legacy.Names) != 1 || legacy.Names[0] != legacyName {
		t.Errorf("error identifies %q/%v, want %q/[%q]", legacy.NamespaceID, legacy.Names, testNS, legacyName)
	}
	if !strings.Contains(legacy.Error(), "repair-segments") {
		t.Errorf("Error() = %q, want it to name the repair command", legacy.Error())
	}

	_, err = NewDefaultReader(testNS, shim, cfg).ListSegments()
	if !errors.As(err, &legacy) {
		t.Fatalf("ListSegments err = %v, want *LegacySegmentFormatError", err)
	}
}

// TestReadRange pins ReadRange's boundary semantics as implemented:
// inclusive of sinceCommit, exclusive of untilCommit.
func TestReadRange(t *testing.T) {
	cfg := newConfig(newTestShim(t), storage.CompressionNone)
	commits := buildChain(t, 4)
	ref := writeSealedSegment(t, cfg, 0, commits...)

	records, err := NewDefaultReader(testNS, cfg.IOShim, cfg).ReadRange(ref, commits[1].Hash, commits[3].Hash)
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("ReadRange returned %d records, want 2 ([since, until))", len(records))
	}
	if records[0].CommitHash != commits[1].Hash || records[1].CommitHash != commits[2].Hash {
		t.Errorf("ReadRange window = [%v, %v], want commits 1 and 2", records[0].CommitHash, records[1].CommitHash)
	}
}

// TestPageCodecFrameLayout pins the on-disk frame format itself - magic,
// big-endian compressed/uncompressed lengths, CRC of the body - since
// the Kotlin side must produce and consume identical bytes.
func TestPageCodecFrameLayout(t *testing.T) {
	payload := []byte("frame layout payload")
	frame, err := (PageCodec{}).Frame(payload, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if string(frame[:4]) != "KDBP" {
		t.Errorf("magic = %q, want KDBP", frame[:4])
	}
	if got := readIntBE(frame, 4); got != len(payload) {
		t.Errorf("compressed-size field = %d, want %d (uncompressed body under CompressionNone)", got, len(payload))
	}
	if got := readIntBE(frame, 8); got != len(payload) {
		t.Errorf("uncompressed-size field = %d, want %d", got, len(payload))
	}
	if got := uint32(readIntBE(frame, 12)); got != compression.CRC32All(payload) {
		t.Errorf("crc field = %08x, want %08x", got, compression.CRC32All(payload))
	}
	if string(frame[frameHeaderSize:]) != string(payload) {
		t.Errorf("body = %q, want payload verbatim", frame[frameHeaderSize:])
	}

	// ZSTD frames must parse back to the exact payload, and record the
	// true uncompressed size for Parse's decompression bound.
	zframe, err := (PageCodec{}).Frame(payload, storage.CompressionZSTD)
	if err != nil {
		t.Fatal(err)
	}
	if got := readIntBE(zframe, 8); got != len(payload) {
		t.Errorf("zstd uncompressed-size field = %d, want %d", got, len(payload))
	}
	back, err := (PageCodec{}).Parse(zframe, storage.CompressionZSTD)
	if err != nil {
		t.Fatalf("Parse(zstd frame): %v", err)
	}
	if string(back) != string(payload) {
		t.Errorf("zstd Parse = %q, want %q", back, payload)
	}
}

// TestPageCodecUnknownCodecFallsBackToCompression pins the current
// behavior for codec values this build doesn't know: Frame compresses
// (same as zstd) and Parse decompresses, so the two stay symmetric even
// for a future codec id round-tripping through an older binary's default
// branch.
func TestPageCodecUnknownCodecFallsBackToCompression(t *testing.T) {
	payload := []byte("payload under an unknown codec id")
	unknown := storage.CompressionCodec(99)
	frame, err := (PageCodec{}).Frame(payload, unknown)
	if err != nil {
		t.Fatal(err)
	}
	back, err := (PageCodec{}).Parse(frame, unknown)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if string(back) != string(payload) {
		t.Errorf("round trip = %q, want %q", back, payload)
	}
}
