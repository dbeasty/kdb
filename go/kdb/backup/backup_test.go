package backup

import (
	"context"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/recovery"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

const ns = "bk/ns"

func newTestShim(t *testing.T) storage.PlatformIOShim {
	t.Helper()
	root := t.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store)
}

func buildCommit(t *testing.T, parent *codec.Hash) document.Commit {
	t.Helper()
	tx, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	docID, _ := codec.RandomUUID()
	var parents []codec.Hash
	if parent != nil {
		parents = []codec.Hash{*parent}
	}
	c := document.Commit{
		ParentHashes:     parents,
		NamespaceID:      ns,
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

func rawFrame(t *testing.T, c document.Commit) []byte {
	t.Helper()
	payload, err := c.ToPayloadBytes()
	if err != nil {
		t.Fatal(err)
	}
	frame, err := (delta.PageCodec{}).Frame(payload, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func appendSegment(t *testing.T, shim storage.PlatformIOShim, seq int64, commits ...document.Commit) {
	t.Helper()
	name := storio.SegmentNameBuilder.DeltaSequenced(ns, seq)
	for _, c := range commits {
		if _, err := shim.AppendToSegment(name, rawFrame(t, c)); err != nil {
			t.Fatal(err)
		}
	}
	if err := shim.FlushSegment(name); err != nil {
		t.Fatal(err)
	}
}

// chain builds n linked commits.
func chain(t *testing.T, n int) []document.Commit {
	t.Helper()
	out := make([]document.Commit, 0, n)
	var parent *codec.Hash
	for i := 0; i < n; i++ {
		c := buildCommit(t, parent)
		out = append(out, c)
		h := c.Hash
		parent = &h
	}
	return out
}

// countingStore wraps DirStore and counts Puts per key class.
type countingStore struct {
	*DirStore
	segmentPuts  int
	manifestPuts int
	prefixPuts   int
}

func (c *countingStore) Put(ctx context.Context, key string, data []byte) error {
	switch {
	case strings.HasSuffix(key, "manifest.json"):
		c.manifestPuts++
	case strings.HasSuffix(key, ".prefix"):
		c.prefixPuts++
	default:
		c.segmentPuts++
	}
	return c.DirStore.Put(ctx, key, data)
}

// TestBackupManifestNamesConsistentSet is spec §6.5 test 1: immediately after a backup, every
// object the manifest names exists and matches its recorded hash.
func TestBackupManifestNamesConsistentSet(t *testing.T) {
	shim := newTestShim(t)
	commits := chain(t, 5)
	appendSegment(t, shim, 0, commits[0], commits[1], commits[2])
	appendSegment(t, shim, 1, commits[3], commits[4])

	store := &DirStore{Root: t.TempDir()}
	m, err := Create(shim, ns, storage.CompressionNone, store, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Segments) != 2 {
		t.Fatalf("expected 2 segment entries, got %+v", m.Segments)
	}
	if m.CommitCount != 5 {
		t.Fatalf("commit count = %d, want 5", m.CommitCount)
	}
	if m.HeadHashes["main"] != commits[4].Hash.Hex() {
		t.Fatalf("head hash = %v, want tip %s", m.HeadHashes, commits[4].Hash.Hex())
	}
	res, err := Verify(store, ns, m.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Clean() {
		t.Fatalf("fresh backup does not verify: %v", res.Problems)
	}
}

// TestIncrementalOmitsUnchangedSegments is spec §6.5 test 2: a second backup with no new
// writes uploads no new sealed-segment objects - only a new manifest (and the cheap active
// prefix re-upload the spec explicitly allows).
func TestIncrementalOmitsUnchangedSegments(t *testing.T) {
	shim := newTestShim(t)
	commits := chain(t, 4)
	appendSegment(t, shim, 0, commits[0], commits[1])
	appendSegment(t, shim, 1, commits[2], commits[3])

	store := &countingStore{DirStore: &DirStore{Root: t.TempDir()}}
	first, err := Create(shim, ns, storage.CompressionNone, store, "")
	if err != nil {
		t.Fatal(err)
	}
	store.segmentPuts, store.manifestPuts, store.prefixPuts = 0, 0, 0

	second, err := Create(shim, ns, storage.CompressionNone, store, first.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	if store.segmentPuts != 0 {
		t.Fatalf("incremental with no new writes uploaded %d sealed segments, want 0", store.segmentPuts)
	}
	if store.manifestPuts != 1 {
		t.Fatalf("manifest puts = %d, want 1", store.manifestPuts)
	}
	if second.BaseBackupID == nil || *second.BaseBackupID != first.BackupID {
		t.Fatalf("base backup id = %v, want %s", second.BaseBackupID, first.BackupID)
	}
	if res, err := Verify(store, ns, second.BackupID); err != nil || !res.Clean() {
		t.Fatalf("incremental backup does not verify: %v %v", err, res)
	}
}

// TestIncrementalIncludesOnlyNewSealedSegments is spec §6.5 test 3.
func TestIncrementalIncludesOnlyNewSealedSegments(t *testing.T) {
	shim := newTestShim(t)
	commits := chain(t, 6)
	appendSegment(t, shim, 0, commits[0], commits[1])
	appendSegment(t, shim, 1, commits[2], commits[3])

	store := &countingStore{DirStore: &DirStore{Root: t.TempDir()}}
	first, err := Create(shim, ns, storage.CompressionNone, store, "")
	if err != nil {
		t.Fatal(err)
	}

	// Segment 1 seals (it stops being the active candidate) once segment 2 exists.
	appendSegment(t, shim, 2, commits[4], commits[5])
	store.segmentPuts, store.manifestPuts, store.prefixPuts = 0, 0, 0

	second, err := Create(shim, ns, storage.CompressionNone, store, first.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	// Exactly one newly-sealed segment (sequence 1, which the base only had as a prefix) plus
	// the new active prefix (sequence 2).
	if store.segmentPuts != 1 {
		t.Fatalf("incremental uploaded %d sealed segments, want exactly 1", store.segmentPuts)
	}
	if store.prefixPuts != 1 {
		t.Fatalf("incremental uploaded %d active prefixes, want 1", store.prefixPuts)
	}
	if second.CommitCount != 6 {
		t.Fatalf("commit count = %d, want 6", second.CommitCount)
	}
}

// TestBackupExcludesUnflushedTail is spec §6.5 test 4: bytes past the last CRC-verified offset
// of the active segment are never uploaded.
func TestBackupExcludesUnflushedTail(t *testing.T) {
	shim := newTestShim(t)
	commits := chain(t, 3)
	appendSegment(t, shim, 0, commits[0], commits[1], commits[2])

	// Simulate a torn in-progress write: append half a frame to the active segment.
	torn := rawFrame(t, buildCommit(t, nil))[:9]
	name := storio.SegmentNameBuilder.DeltaSequenced(ns, 0)
	if _, err := shim.AppendToSegment(name, torn); err != nil {
		t.Fatal(err)
	}
	if err := shim.FlushSegment(name); err != nil {
		t.Fatal(err)
	}

	store := &DirStore{Root: t.TempDir()}
	m, err := Create(shim, ns, storage.CompressionNone, store, "")
	if err != nil {
		t.Fatal(err)
	}
	if m.CommitCount != 3 {
		t.Fatalf("commit count = %d, want 3", m.CommitCount)
	}
	obj, err := store.Get(context.Background(), m.Segments[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	full, err := shim.ReadFromSegment(name, 0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if len(obj) >= len(full) {
		t.Fatalf("uploaded object (%d bytes) includes the torn tail (segment is %d bytes)", len(obj), len(full))
	}
	if res, _ := Verify(store, ns, m.BackupID); !res.Clean() {
		t.Fatalf("backup with excluded tail does not verify: %v", res.Problems)
	}
}

// TestBackupVerifyDetectsTruncatedUpload is spec §6.5 test 5.
func TestBackupVerifyDetectsTruncatedUpload(t *testing.T) {
	shim := newTestShim(t)
	commits := chain(t, 4)
	appendSegment(t, shim, 0, commits[0], commits[1])
	appendSegment(t, shim, 1, commits[2], commits[3])

	store := &DirStore{Root: t.TempDir()}
	m, err := Create(shim, ns, storage.CompressionNone, store, "")
	if err != nil {
		t.Fatal(err)
	}

	// Truncate the sealed segment object in place.
	key := m.Segments[0].Key
	data, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), key, data[:len(data)/2]); err != nil {
		t.Fatal(err)
	}

	res, err := Verify(store, ns, m.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	if res.Clean() {
		t.Fatal("verify did not detect the truncated object")
	}
}

// TestBackupWipeRestoreRoundTrip is Phase 2.11's exit criteria: back up, destroy the data
// directory, restore from the backup alone, and confirm the restored log holds exactly the
// original verified commits.
func TestBackupWipeRestoreRoundTrip(t *testing.T) {
	shim := newTestShim(t)
	commits := chain(t, 5)
	appendSegment(t, shim, 0, commits[0], commits[1], commits[2])
	appendSegment(t, shim, 1, commits[3], commits[4])

	originalCommits, err := integrity.ScanVerifiedCommits(shim, ns, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}

	store := &DirStore{Root: t.TempDir()}
	m, err := Create(shim, ns, storage.CompressionNone, store, "")
	if err != nil {
		t.Fatal(err)
	}

	// "Wipe": the original shim is simply never consulted again. Fetch the backup into a fresh
	// directory and hybrid-restore from it as the only source.
	fetched := t.TempDir()
	if _, err := FetchToDir(store, ns, m.BackupID, fetched); err != nil {
		t.Fatal(err)
	}
	fetchedStore, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &fetched})
	if err != nil {
		t.Fatal(err)
	}
	fetchedShim := storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &fetched, FsyncOnFlush: true}, fetchedStore)

	outRoot := t.TempDir()
	outStore, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &outRoot})
	if err != nil {
		t.Fatal(err)
	}
	outShim := storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &outRoot, FsyncOnFlush: true}, outStore)

	result, err := recovery.HybridRestore([]recovery.Source{{Label: "backup", Shim: fetchedShim}}, ns, storage.CompressionNone, outShim)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.MissingHashes) > 0 {
		t.Fatalf("restore reported missing commits: %v", result.MissingHashes)
	}

	restored, err := integrity.ScanVerifiedCommits(outShim, ns, storage.CompressionNone)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != len(originalCommits) {
		t.Fatalf("restored %d commits, want %d", len(restored), len(originalCommits))
	}
	for h := range originalCommits {
		if _, ok := restored[h]; !ok {
			t.Fatalf("commit %s lost through backup+restore", h)
		}
	}
	if report, err := integrity.Verify(outShim, ns, integrity.Options{Level: integrity.L2, Compression: storage.CompressionNone}); err != nil || !report.Clean() {
		t.Fatalf("restored directory fails verify: err=%v findings=%+v", err, report)
	}
}
