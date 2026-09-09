package embed_test

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	s3io "github.com/limidus/kdb/go/kdb/storage/io/s3"
)

// Restoring what a compaction reclaimed, which is the operation that makes
// "compaction is eviction, not destruction" mean anything.

// archivedRuntime opens a rotating history=none namespace with an in-memory S3 archive behind
// it, so a compaction has somewhere to evict to.
func archivedRuntime(t *testing.T, root string, blobs *s3io.MemoryBlobStore) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.DeltaMaxSegmentBytes = 4096
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	opts.S3Archive = &s3io.Config{Bucket: "archive", Prefix: "kdb"}
	opts.ArchiveBlobs = blobs
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/docs", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

// The round trip: write, compact, confirm segments are gone locally, restore them, confirm the
// floor came back down.
func TestCompactedSegmentsCanBeRestored(t *testing.T) {
	root := t.TempDir()
	blobs := s3io.NewMemoryBlobStore()
	rt := archivedRuntime(t, root, blobs)
	defer rt.Close()

	if !rt.ArchiveAvailable() {
		t.Fatal("an archive was configured but the namespace does not report one")
	}
	for i := 0; i < 60; i++ {
		writeRotationDoc(t, rt, fmt.Sprintf("doc-%d", i), i)
	}
	before := deltaSegmentCount(t, root)

	compacted, err := rt.CompactHistory()
	if err != nil {
		t.Fatal(err)
	}
	if compacted.Removed == 0 {
		t.Fatal("nothing was compacted, so there is nothing to restore")
	}
	if !compacted.Reversible {
		t.Error("a compaction with an archive configured reported itself as irreversible")
	}
	after := deltaSegmentCount(t, root)
	if after >= before {
		t.Fatalf("the compaction left %d segments, was %d; nothing was evicted", after, before)
	}

	res, err := rt.RestoreFromArchive(0, int64(compacted.FloorSequence-1))
	if err != nil {
		t.Fatalf("restoring: %v", err)
	}
	if res.Restored == 0 {
		t.Fatal("the restore brought nothing back")
	}
	if n := deltaSegmentCount(t, root); n <= after {
		t.Fatalf("after restoring there are %d segments, was %d; nothing landed on disk", n, after)
	}
	if res.FloorSequence > 0 {
		t.Errorf("the floor is still %d after restoring from 0; it was not lowered to admit them",
			res.FloorSequence)
	}
	// The live dataset is intact throughout - a restore adds history back, it does not disturb
	// the present.
	for i := 0; i < 60; i++ {
		if _, ok := readDoc(t, rt, fmt.Sprintf("doc-%d", i)); !ok {
			t.Fatalf("doc-%d is missing after a compact-and-restore round trip", i)
		}
	}
}

// Without an archive the refusal has to name why, because "restore failed" would send an
// operator looking for a bug rather than at their configuration.
func TestRestoreWithoutArchiveNamesTheReason(t *testing.T) {
	root := t.TempDir()
	rt := rotatingRuntime(t, root, 4096)
	defer rt.Close()
	writeRotationDoc(t, rt, "a", 1)

	if rt.ArchiveAvailable() {
		t.Fatal("a namespace with no archive configured reports one")
	}
	_, err := rt.RestoreFromArchive(0, 1)
	if err == nil {
		t.Fatal("restoring with no archive succeeded")
	}
	if got := err.Error(); !contains(got, "archive") {
		t.Fatalf("the refusal does not mention the archive: %v", err)
	}
}

// A nonsensical range is refused rather than quietly doing nothing.
func TestRestoreRejectsAnInvertedRange(t *testing.T) {
	root := t.TempDir()
	blobs := s3io.NewMemoryBlobStore()
	rt := archivedRuntime(t, root, blobs)
	defer rt.Close()
	if _, err := rt.RestoreFromArchive(9, 2); err == nil {
		t.Fatal("an inverted range was accepted")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
