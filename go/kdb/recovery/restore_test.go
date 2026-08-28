package recovery

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func newDirShim(t *testing.T) storage.PlatformIOShim {
	t.Helper()
	root := t.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store)
}

func buildCommit(t *testing.T, ns string, parent *codec.Hash) document.Commit {
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

func appendSegment(t *testing.T, shim storage.PlatformIOShim, ns string, seq int64, frames ...[]byte) {
	t.Helper()
	name := storio.SegmentNameBuilder.DeltaSequenced(ns, seq)
	for _, f := range frames {
		if _, err := shim.AppendToSegment(name, f); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRestoreFromBackupOnlyRebuildsCleanly(t *testing.T) {
	const ns = "ns1"
	backup := newDirShim(t)
	c0 := buildCommit(t, ns, nil)
	c1 := buildCommit(t, ns, &c0.Hash)
	appendSegment(t, backup, ns, 0, rawFrame(t, c0), rawFrame(t, c1))

	out := newDirShim(t)
	result, err := HybridRestore([]Source{{Label: "backup", Shim: backup}}, ns, storage.CompressionNone, out)
	if err != nil {
		t.Fatal(err)
	}
	if result.AppliedCount != 2 || len(result.MissingHashes) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}

	restored, err := integrity.ScanVerifiedCommits(out, ns)
	if err != nil {
		t.Fatal(err)
	}
	if len(restored) != 2 || !hasHash(restored, c0.Hash) || !hasHash(restored, c1.Hash) {
		t.Fatalf("restored log missing expected commits: %+v", restored)
	}
}

func TestHybridRestoreLocalAheadOfBackup(t *testing.T) {
	const ns = "ns1"
	c0 := buildCommit(t, ns, nil)
	c1 := buildCommit(t, ns, &c0.Hash)
	c2 := buildCommit(t, ns, &c1.Hash)
	c3 := buildCommit(t, ns, &c2.Hash) // will be torn in the local copy

	backup := newDirShim(t)
	appendSegment(t, backup, ns, 0, rawFrame(t, c0), rawFrame(t, c1)) // stale: missing c2, c3

	local := newDirShim(t)
	torn := rawFrame(t, c3)[:8]
	appendSegment(t, local, ns, 0, rawFrame(t, c0), rawFrame(t, c1), rawFrame(t, c2), torn)

	out := newDirShim(t)
	result, err := HybridRestore(
		[]Source{{Label: "local", Shim: local}, {Label: "backup", Shim: backup}},
		ns, storage.CompressionNone, out,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.AppliedCount != 3 {
		t.Fatalf("expected 3 applied commits (c0,c1,c2), got %+v", result)
	}
	if len(result.MissingHashes) != 0 {
		t.Fatalf("expected no missing hashes, got %v", result.MissingHashes)
	}

	restored, err := integrity.ScanVerifiedCommits(out, ns)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range []codec.Hash{c0.Hash, c1.Hash, c2.Hash} {
		if !hasHash(restored, h) {
			t.Fatalf("expected restored log to contain %s", h.Hex())
		}
	}
	if hasHash(restored, c3.Hash) {
		t.Fatal("restored log must not contain the torn, unverified commit c3")
	}
}

func TestHybridRestoreFillsGapFromPeer(t *testing.T) {
	const ns = "ns1"
	c0 := buildCommit(t, ns, nil)
	c1 := buildCommit(t, ns, &c0.Hash) // missing from both local and backup
	c2 := buildCommit(t, ns, &c1.Hash)

	local := newDirShim(t)
	appendSegment(t, local, ns, 0, rawFrame(t, c0))
	appendSegment(t, local, ns, 1, rawFrame(t, c2)) // c2's parent c1 is absent

	withoutPeer, err := HybridRestore([]Source{{Label: "local", Shim: local}}, ns, storage.CompressionNone, newDirShim(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(withoutPeer.MissingHashes) == 0 {
		t.Fatalf("expected missing hashes without the peer source, got %+v", withoutPeer)
	}

	peer := newDirShim(t)
	appendSegment(t, peer, ns, 0, rawFrame(t, c1))

	out := newDirShim(t)
	result, err := HybridRestore(
		[]Source{{Label: "local", Shim: local}, {Label: "peer", Shim: peer}},
		ns, storage.CompressionNone, out,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.AppliedCount != 3 || len(result.MissingHashes) != 0 {
		t.Fatalf("expected the peer source to fill the gap, got %+v", result)
	}
}

func TestRestoreNeverTrustsUnverifiedLocalFrames(t *testing.T) {
	const ns = "ns1"
	c0 := buildCommit(t, ns, nil)
	c1 := buildCommit(t, ns, &c0.Hash)

	local := newDirShim(t)
	corrupt := append([]byte(nil), rawFrame(t, c1)...)
	corrupt[19] ^= 0xFF // CRC mismatch, not truncation - a frame that "fits" but is wrong
	appendSegment(t, local, ns, 0, rawFrame(t, c0), corrupt)

	out := newDirShim(t)
	result, err := HybridRestore([]Source{{Label: "local", Shim: local}}, ns, storage.CompressionNone, out)
	if err != nil {
		t.Fatal(err)
	}
	if result.AppliedCount != 1 {
		t.Fatalf("expected only the verified commit c0 to be restored, got %+v", result)
	}
	restored, err := integrity.ScanVerifiedCommits(out, ns)
	if err != nil {
		t.Fatal(err)
	}
	if hasHash(restored, c1.Hash) {
		t.Fatal("restore must never apply a commit whose frame failed CRC verification")
	}
}

func hasHash(m map[string]document.Commit, h codec.Hash) bool {
	_, ok := m[h.Hex()]
	return ok
}
