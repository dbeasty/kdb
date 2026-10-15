package main

// Attachment fixtures for @limidus/kdb-replica (docs/kdb-client-replica-plan.md §9, model of
// docs/kdb-attachments-plan.md): ref, set and scheme-1 content hashes; attachment tables built
// by Go's AttachmentTable from op sequences, with their roots; and commits carrying attachment
// ops and roots. The replica verifies every commit's attachment root as it does the tree hash.

import (
	"fmt"
	"math/rand"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

type attachmentRefFixture struct {
	Scheme      int    `json:"scheme"`
	ContentHash string `json:"contentHash"`
	Size        int64  `json:"size"`
	Mime        string `json:"mime"`
	RefHash     string `json:"refHash"`
}

type attachmentEntryFixture struct {
	ID   string               `json:"id"`
	Name string               `json:"name"`
	Ref  attachmentRefFixture `json:"ref"`
}

type attachmentSetFixture struct {
	Entries []attachmentEntryFixture `json:"entries"`
	SetHash string                   `json:"setHash"`
}

type contentHashFixture struct {
	Name      string `json:"name"`
	Size      int    `json:"size"`
	Seed      int64  `json:"seed"` // byte i is (i*31 + seed*7 + i>>8) & 0xff (contentBytes), reproducible anywhere
	Hash      string `json:"hash"`
	HelloText string `json:"helloText,omitempty"`
}

type attachmentTableStep struct {
	Ops  []replicaOpFixture `json:"ops"`
	Root string             `json:"root"` // "" when the table is empty
}

type attachmentsFixture struct {
	Refs          []attachmentRefFixture `json:"refs"`
	Sets          []attachmentSetFixture `json:"sets"`
	ContentHashes []contentHashFixture   `json:"contentHashes"`
	Table         []attachmentTableStep  `json:"table"`
	Commits       []replicaCommitFixture `json:"commits"`
}

func contentBytes(size int, seed int64) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte((int64(i)*31 + seed*7 + int64(i>>8)) & 0xff)
	}
	return b
}

func refFixture(r document.AttachmentRef) attachmentRefFixture {
	return attachmentRefFixture{Scheme: int(r.Scheme), ContentHash: r.ContentHash.Hex(), Size: r.Size, Mime: r.Mime, RefHash: r.Hash().Hex()}
}

func attachmentOpFixture(op document.Op) replicaOpFixture {
	switch o := op.(type) {
	case document.AttachmentPutOp:
		return replicaOpFixture{Kind: "attachmentPut", DocID: o.DocID.String(), AttachmentID: o.AttachmentID.String(), Name: o.Name,
			Scheme: int(o.Scheme), ContentHash: o.ContentHash.Hex(), Size: o.Size, Mime: o.Mime}
	case document.AttachmentDeleteOp:
		return replicaOpFixture{Kind: "attachmentDelete", DocID: o.DocID.String(), AttachmentID: o.AttachmentID.String()}
	case document.DeleteOp:
		return replicaOpFixture{Kind: "delete", DocID: o.DocID.String()}
	case document.WriteOp:
		return replicaOpFixture{Kind: "write", DocID: o.DocID.String(), JSON: o.Patch}
	}
	panic(fmt.Sprintf("unexpected op %T", op))
}

func writeAttachmentFixtures(dir string) {
	f := attachmentsFixture{}
	rng := rand.New(rand.NewSource(17))
	hashOf := func(b byte) codec.Hash {
		var h codec.Hash
		for i := range h.Bytes {
			h.Bytes[i] = b
		}
		return h
	}
	refs := []document.AttachmentRef{
		{Scheme: 1, ContentHash: hashOf(0xab), Size: 1234567, Mime: "image/png"}, // attachmentRefGolden
		{Scheme: 1, ContentHash: hashOf(0x01), Size: 0, Mime: ""},
		{Scheme: 1, ContentHash: fixedHash("r"), Size: 1 << 40, Mime: "application/vnd.kdb+ünïcode"},
	}
	for _, r := range refs {
		f.Refs = append(f.Refs, refFixture(r))
	}
	sets := []document.AttachmentSet{
		{{ID: codec.UUID{MSB: 1, LSB: 2}, Name: "a.png", AttachmentRef: refs[0]}}, // attachmentSetGolden
		{
			{ID: codec.UUID{MSB: 1, LSB: 1}, Name: "first", AttachmentRef: refs[1]},
			{ID: codec.UUID{MSB: 1, LSB: 9}, Name: "zweite ✨", AttachmentRef: refs[2]},
			{ID: codec.UUID{MSB: -1, LSB: -1}, Name: "last", AttachmentRef: refs[0]},
		},
	}
	for _, s := range sets {
		sf := attachmentSetFixture{SetHash: s.Hash().Hex()}
		for _, a := range s {
			sf.Entries = append(sf.Entries, attachmentEntryFixture{ID: a.ID.String(), Name: a.Name, Ref: refFixture(a.AttachmentRef)})
		}
		f.Sets = append(f.Sets, sf)
	}
	f.ContentHashes = append(f.ContentHashes, contentHashFixture{Name: "hello", Size: 5, HelloText: "hello",
		Hash: document.AttachmentContentHash([]byte("hello")).Hex()})
	for i, size := range []int{0, 1, document.AttachmentChunkBytes - 1, document.AttachmentChunkBytes, document.AttachmentChunkBytes + 1, 3*document.AttachmentChunkBytes + 17, 5 * document.AttachmentChunkBytes} {
		seed := int64(100 + i)
		f.ContentHashes = append(f.ContentHashes, contentHashFixture{Name: fmt.Sprint("random", size), Size: size, Seed: seed,
			Hash: document.AttachmentContentHash(contentBytes(size, seed)).Hex()})
	}

	// A table driven through puts, renames, deletes and document deletes, root after each step.
	docs := make([]codec.UUID, 6)
	atts := make([]codec.UUID, 10)
	for i := range docs {
		docs[i] = codec.DerivedUUID(fmt.Sprint("att-doc-", i))
	}
	for i := range atts {
		atts[i] = codec.DerivedUUID(fmt.Sprint("att-", i))
	}
	table := document.EmptyAttachmentTable()
	for step := 0; step < 60; step++ {
		var ops []document.Op
		for k := 0; k < 1+rng.Intn(4); k++ {
			doc := docs[rng.Intn(len(docs))]
			att := atts[rng.Intn(len(atts))]
			switch rng.Intn(6) {
			case 0:
				ops = append(ops, document.DeleteOp{DocID: doc})
			case 1:
				ops = append(ops, document.AttachmentDeleteOp{DocID: doc, AttachmentID: att})
			default:
				// the name is tied to the attachment id, so a put never collides with another's name
				ops = append(ops, document.AttachmentPutOp{DocID: doc, AttachmentID: att, Name: "file-" + att.String()[:8],
					Scheme: 1, ContentHash: fixedHash(fmt.Sprint("c", step, k)), Size: int64(rng.Intn(1 << 30)), Mime: "image/png"})
			}
		}
		var err error
		if table, err = table.Apply(ops); err != nil {
			fail(err)
		}
		s := attachmentTableStep{}
		for _, op := range ops {
			s.Ops = append(s.Ops, attachmentOpFixture(op))
		}
		if r := table.Root(); r != nil {
			s.Root = r.Hex()
		}
		f.Table = append(f.Table, s)
	}

	// Commits carrying attachment ops and a root: the golden one from document's tests, and others.
	id, _ := codec.UUIDFromString("6f9619ff-8b86-d011-b42d-00cf4fc964ff")
	attID, _ := codec.UUIDFromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
	root := hashOf(0xcd)
	golden, err := document.BuildCommitWithAttachments([]codec.Hash{hashOf(1)}, "app/data", id,
		codec.Timestamp{EpochMillis: 1750000000000, MicroRemainder: 5}, id,
		[]document.Op{
			document.WriteOp{DocID: id, Patch: `{"a":1}`},
			document.AttachmentPutOp{DocID: id, AttachmentID: attID, Name: "photo.png", Scheme: 1, ContentHash: hashOf(0xab), Size: 1234567, Mime: "image/png"},
			document.AttachmentDeleteOp{DocID: id, AttachmentID: attID},
		}, hashOf(2), nil, "m", &root)
	if err != nil {
		fail(err)
	}
	f.Commits = append(f.Commits, commitFixture("attachmentGolden", golden))
	onlyRoot, err := document.BuildCommitWithAttachments(nil, "app/data", id, codec.TimestampFromEpochMicros(9), id,
		[]document.Op{document.WriteOp{DocID: id, Patch: `{}`}}, hashOf(3), &root, "", &root)
	if err != nil {
		fail(err)
	}
	f.Commits = append(f.Commits, commitFixture("rootWithSchemaAndOnlyJSON", onlyRoot))
	writeJSON(filepath.Join(dir, "attachments.json"), f)
}
