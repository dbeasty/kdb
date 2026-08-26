package integrity

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/delta"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

func newTestShim(t *testing.T) storage.PlatformIOShim {
	t.Helper()
	root := t.TempDir()
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		t.Fatal(err)
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root, FsyncOnFlush: true}, store)
}

// buildCommit constructs one valid, hash-consistent commit. parent is nil
// for a genesis commit.
func buildCommit(t *testing.T, ns string, parent *codec.Hash) document.Commit {
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

// buildChain builds n commits, each the sole parent of the next.
func buildChain(t *testing.T, n int, ns string) []document.Commit {
	t.Helper()
	commits := make([]document.Commit, 0, n)
	var parent *codec.Hash
	for i := 0; i < n; i++ {
		c := buildCommit(t, ns, parent)
		commits = append(commits, c)
		h := c.Hash
		parent = &h
	}
	return commits
}

// rawFrame encodes one commit as an uncompressed KDBP frame, for tests
// that need exact control over segment bytes.
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

// flippedFrame returns a copy of a raw frame with one body byte flipped,
// producing a CRC mismatch without changing the frame's declared length -
// i.e. a frame that "fits" but fails CorruptFrameError's CRC check,
// distinct from a frame simply cut short.
func flippedFrame(t *testing.T, c document.Commit) []byte {
	t.Helper()
	f := append([]byte(nil), rawFrame(t, c)...)
	if len(f) < 20 {
		t.Fatalf("frame too short to flip a body byte: %d bytes", len(f))
	}
	f[19] ^= 0xFF
	return f
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
