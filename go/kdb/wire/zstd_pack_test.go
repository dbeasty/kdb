package wire_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

func packCommits(t *testing.T, n int) []document.Commit {
	t.Helper()
	var out []document.Commit
	var parents []codec.Hash
	for i := 0; i < n; i++ {
		tx, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		doc, _ := codec.RandomUUID()
		c := document.Commit{ParentHashes: parents, NamespaceID: "app/pack", TransactionID: tx, Timestamp: codec.TimestampNow(),
			AuthorNodeID: author, Operations: []document.Op{document.WriteOp{DocID: doc,
				Patch: fmt.Sprintf(`{"i":%d,"name":"customer %d","pad":%q}`, i, i, strings.Repeat("abc", 60))}},
			DocumentTreeHash: document.EmptyDocumentTree().TreeHash, Message: "pack test"}
		h, err := document.ComputeCommitHash(c)
		if err != nil {
			t.Fatal(err)
		}
		c.Hash = h
		out = append(out, c)
		parents = []codec.Hash{h}
	}
	return out
}

func hashes(cs []document.Commit) string {
	var b strings.Builder
	for _, c := range cs {
		b.WriteString(c.Hash.Hex())
	}
	return b.String()
}

// TestPackPageZstdRoundTrip: a page sent compressed decodes to the same commits, reports that it
// arrived compressed, and is smaller on the wire than the same page uncompressed.
func TestPackPageZstdRoundTrip(t *testing.T) {
	w := wire.NewCodec(wire.EncodingJSON)
	commits := packCommits(t, 200)
	sizes := map[bool]int{}
	for _, z := range []bool{false, true} {
		frame, err := w.Encode(wire.PackPageMessage{H: wire.Header{MessageType: wire.MsgPackPage, ProtocolVersion: wire.ProtocolVersion, CorrelationID: 7},
			Namespace: "app/pack", Commits: commits, Done: true, Zstd: z})
		if err != nil {
			t.Fatal(err)
		}
		sizes[z] = len(frame)
		msg, err := w.Decode(frame)
		if err != nil {
			t.Fatal(err)
		}
		p := msg.(wire.PackPageMessage)
		if hashes(p.Commits) != hashes(commits) || p.Zstd != z || !p.Done {
			t.Fatalf("zstd=%v: page did not round-trip", z)
		}
		for i := range p.Commits {
			if len(p.Commits[i].Operations) != 1 {
				t.Fatalf("zstd=%v: commit %d lost its operations", z, i)
			}
		}
	}
	if sizes[true]*2 > sizes[false] {
		t.Fatalf("compressed page %d bytes, uncompressed %d: expected at least 2x smaller", sizes[true], sizes[false])
	}
	t.Logf("200-commit page: %d bytes uncompressed, %d compressed", sizes[false], sizes[true])
}

func TestRefUpdateZstdRoundTrip(t *testing.T) {
	w := wire.NewCodec(wire.EncodingJSON)
	commits := packCommits(t, 50)
	frame, err := w.Encode(wire.RefUpdateMessage{H: wire.Header{MessageType: wire.MsgRefUpdate, ProtocolVersion: wire.ProtocolVersion, CorrelationID: 3},
		Namespace: "app/pack", Kind: wire.RefBranch, Ref: "main", NewHex: commits[49].Hash.Hex(), Commits: commits, More: true, Zstd: true})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := w.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	u := msg.(wire.RefUpdateMessage)
	if hashes(u.Commits) != hashes(commits) || !u.Zstd || !u.More {
		t.Fatal("ref update did not round-trip")
	}
}
