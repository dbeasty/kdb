package embed_test

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
)

const attachNS = "app/docs"

func putAttachDoc(t *testing.T, rt *embed.EmbeddedKdbRuntime, id codec.UUID, json string) {
	t.Helper()
	body := `{"id":"` + id.String() + `"}`
	if json != "{}" {
		body = `{"id":"` + id.String() + `",` + json[1:]
	}
	if _, err := embed.PutJSONDocument(rt, attachNS, body); err != nil {
		t.Fatal(err)
	}
}

func readAttachment(t *testing.T, rt *embed.EmbeddedKdbRuntime, id codec.UUID, asOf codec.Hash) ([]byte, embed.AttachmentInfo) {
	t.Helper()
	return readNamed(t, rt, id, embed.AttachmentByName("file"), asOf)
}

func readNamed(t *testing.T, rt *embed.EmbeddedKdbRuntime, id codec.UUID, sel embed.AttachmentSelector, asOf codec.Hash) ([]byte, embed.AttachmentInfo) {
	t.Helper()
	r, info, err := rt.GetAttachment(attachNS, id, sel, asOf)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return b, info
}

func contentHashAt(t *testing.T, rt *embed.EmbeddedKdbRuntime, id codec.UUID) codec.Hash {
	t.Helper()
	head, _ := rt.DAG.Head()
	c, _ := rt.DAG.GetCommitOrThrow(head)
	doc, err := rt.Storage.GetDocument(attachNS, id, c.DocumentTreeHash)
	if err != nil || doc == nil {
		t.Fatalf("document %s: %v", id, err)
	}
	h, err := doc.ContentHash()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func memoryRuntime(t *testing.T) *embed.EmbeddedKdbRuntime {
	t.Helper()
	rt, err := embed.OpenMemoryRuntime("app", attachNS, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10})
	return rt
}

// Replace an attachment three times, then read each commit: every one returns its own bytes, and
// the document's JSON content hash never moves.
func TestAttachmentVersionsReadAsOfEachCommit(t *testing.T) {
	rt := memoryRuntime(t)
	id := codec.UUID{MSB: 7, LSB: 7}
	putAttachDoc(t, rt, id, `{"name":"report"}`)
	before := contentHashAt(t, rt, id)
	r := rand.New(rand.NewSource(1))
	var commits []codec.Hash
	var versions [][]byte
	for i, n := range []int{100, 20_000, 3} {
		data := make([]byte, n)
		r.Read(data)
		c, info, err := rt.PutAttachment(attachNS, id, bytes.NewReader(data), embed.PutAttachmentOptions{Name: "file", Mime: "application/pdf"})
		if err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		if info.Size != int64(n) || info.Mime != "application/pdf" {
			t.Fatalf("info %+v", info)
		}
		commits = append(commits, c)
		versions = append(versions, data)
	}
	if after := contentHashAt(t, rt, id); after != before {
		t.Fatal("an attachment changed the document's content hash")
	}
	for i, c := range commits {
		got, info := readAttachment(t, rt, id, c)
		if !bytes.Equal(got, versions[i]) || info.Size != int64(len(versions[i])) {
			t.Fatalf("as of commit %d: wrong bytes", i)
		}
	}
	got, _ := readAttachment(t, rt, id, codec.Hash{})
	if !bytes.Equal(got, versions[2]) {
		t.Fatal("head read returned an old version")
	}
}

func TestAttachmentNeedsItsDocumentAndDeletesWithIt(t *testing.T) {
	rt := memoryRuntime(t)
	missing := codec.UUID{MSB: 1}
	if _, _, err := rt.PutAttachment(attachNS, missing, bytes.NewReader([]byte("x")), embed.PutAttachmentOptions{Name: "file", Mime: "text/plain"}); !errors.Is(err, embed.ErrNoSuchDocument) {
		t.Fatalf("attachment on a missing document: %v", err)
	}
	id := codec.UUID{MSB: 2}
	putAttachDoc(t, rt, id, `{"a":1}`)
	put, _, err := rt.PutAttachment(attachNS, id, bytes.NewReader([]byte("hello")), embed.PutAttachmentOptions{Name: "file", Mime: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	// Detach: the document stays, the attachment goes.
	if _, err := rt.DeleteAttachment(attachNS, id, embed.AttachmentByName("file")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.GetAttachment(attachNS, id, embed.AttachmentByName("file"), codec.Hash{}); !errors.Is(err, embed.ErrNoAttachment) {
		t.Fatalf("after detach: %v", err)
	}
	if _, err := rt.DeleteAttachment(attachNS, id, embed.AttachmentByName("file")); !errors.Is(err, embed.ErrNoAttachment) {
		t.Fatalf("detaching nothing: %v", err)
	}
	// The old commit still has it.
	if got, _ := readAttachment(t, rt, id, put); string(got) != "hello" {
		t.Fatalf("as of the put: %q", got)
	}
	// Deleting the document takes its attachment.
	if _, _, err := rt.PutAttachment(attachNS, id, bytes.NewReader([]byte("again")), embed.PutAttachmentOptions{Name: "file", Mime: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	deleteDoc(t, rt, id)
	head, _ := rt.DAG.Head()
	table, err := rt.AttachmentTableAt(head)
	if err != nil {
		t.Fatal(err)
	}
	if table.Len() != 0 {
		t.Fatal("deleting the document left its attachment")
	}
}

func deleteDoc(t *testing.T, rt *embed.EmbeddedKdbRuntime, id codec.UUID) {
	t.Helper()
	head, _ := rt.DAG.Head()
	c, _ := rt.DAG.GetCommitOrThrow(head)
	if err := rt.Storage.DeleteDocument(attachNS, id); err != nil {
		t.Fatal(err)
	}
	tree, err := rt.Storage.CommitTree(attachNS, c.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	txID, _ := codec.RandomUUID()
	tx := document.Transaction{ID: txID, BaseVersion: head, Operations: []document.Op{document.DeleteOp{DocID: id}}, Timestamp: codec.TimestampNow(), AuthorNodeID: txID}
	if _, err := rt.DAG.AppendCommit(tx, head, tree, nil, ""); err != nil {
		t.Fatal(err)
	}
}

func openAttachRuntime(t *testing.T, root string, mode storage.HistoryMode) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = mode
	if mode == storage.HistoryModeNone {
		opts.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	}
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", attachNS, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10})
	return rt
}

// Attachments, small and large, survive a restart - replayed from the log, and from a checkpoint.
func TestAttachmentsSurviveReopen(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		root := t.TempDir()
		rt := openAttachRuntime(t, root, storage.HistoryModeFull)
		small, large := codec.UUID{MSB: 1}, codec.UUID{MSB: 2}
		putAttachDoc(t, rt, small, `{"k":"small"}`)
		putAttachDoc(t, rt, large, `{"k":"large"}`)
		bigBytes := bytes.Repeat([]byte("0123456789abcdef"), 4096)
		first, _, err := rt.PutAttachment(attachNS, small, bytes.NewReader([]byte("tiny")), embed.PutAttachmentOptions{Name: "file", Mime: "text/plain"})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := rt.PutAttachment(attachNS, large, bytes.NewReader(bigBytes), embed.PutAttachmentOptions{Name: "file", Mime: "image/png"}); err != nil {
			t.Fatal(err)
		}
		if _, _, err := rt.PutAttachment(attachNS, small, bytes.NewReader([]byte("tiny v2")), embed.PutAttachmentOptions{Name: "file", Mime: "text/plain"}); err != nil {
			t.Fatal(err)
		}
		if checkpoint {
			if _, err := rt.Maintain(); err != nil {
				t.Fatal(err)
			}
		}
		rt.Close()

		rt = openAttachRuntime(t, root, storage.HistoryModeFull)
		if got, info := readAttachment(t, rt, large, codec.Hash{}); !bytes.Equal(got, bigBytes) || info.Mime != "image/png" {
			t.Fatalf("checkpoint=%v: large attachment lost", checkpoint)
		}
		if got, _ := readAttachment(t, rt, small, codec.Hash{}); string(got) != "tiny v2" {
			t.Fatalf("checkpoint=%v: small attachment: %q", checkpoint, got)
		}
		if got, _ := readAttachment(t, rt, small, first); string(got) != "tiny" {
			t.Fatalf("checkpoint=%v: as-of read after reopen: %q", checkpoint, got)
		}
		rt.Close()
	}
}

// Under AttachmentHistoryLatest, and under history=none, replacing an attachment twice leaves only
// the latest blob; an as-of read of an old version then says it is not retained.
func TestOnlyTheLatestAttachmentIsKept(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mode   storage.HistoryMode
		latest bool
	}{
		{"latest-setting", storage.HistoryModeFull, true},
		{"history-none", storage.HistoryModeNone, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			rt := openAttachRuntime(t, root, tc.mode)
			defer rt.Close()
			if tc.latest {
				rt.SetAttachmentHistory(embed.AttachmentHistoryLatest)
			}
			id := codec.UUID{MSB: 3}
			putAttachDoc(t, rt, id, `{}`)
			var firstCommit codec.Hash
			for i := 0; i < 3; i++ {
				c, _, err := rt.PutAttachment(attachNS, id, bytes.NewReader(bytes.Repeat([]byte{byte('a' + i)}, 50_000)), embed.PutAttachmentOptions{Name: "file", Mime: "x/y"})
				if err != nil {
					t.Fatal(err)
				}
				if i == 0 {
					firstCommit = c
				}
			}
			if _, err := rt.Maintain(); err != nil {
				t.Fatal(err)
			}
			blobs, err := rt.AttachmentStore(attachNS).LargeBlobs()
			if err != nil {
				t.Fatal(err)
			}
			if len(blobs) != 1 {
				t.Fatalf("%d large blobs after maintenance, want only the latest", len(blobs))
			}
			got, _ := readAttachment(t, rt, id, codec.Hash{})
			if !bytes.Equal(got, bytes.Repeat([]byte{'c'}, 50_000)) {
				t.Fatal("the latest version was lost")
			}
			if _, _, err := rt.GetAttachment(attachNS, id, embed.AttachmentByName("file"), firstCommit); err == nil {
				t.Fatal("an old version read back although it is not retained")
			}
		})
	}
}

// Under the default (all history), maintenance keeps every committed version - re-storing replaced
// ones as deltas when that is much smaller - and removes uploads no commit references.
func TestAllHistoryKeepsVersionsAsDeltas(t *testing.T) {
	root := t.TempDir()
	rt := openAttachRuntime(t, root, storage.HistoryModeFull)
	defer rt.Close()
	id := codec.UUID{MSB: 4}
	putAttachDoc(t, rt, id, `{}`)
	r := rand.New(rand.NewSource(9))
	base := make([]byte, 60_000)
	r.Read(base)
	var commits []codec.Hash
	var versions [][]byte
	for i := 0; i < 3; i++ {
		v := append([]byte(nil), base...)
		copy(v[i*5000:], []byte("a small edit to this version"))
		c, _, err := rt.PutAttachment(attachNS, id, bytes.NewReader(v), embed.PutAttachmentOptions{Name: "file", Mime: "x/y"})
		if err != nil {
			t.Fatal(err)
		}
		commits = append(commits, c)
		versions = append(versions, v)
	}
	// An upload no commit references.
	if _, err := rt.AttachmentStore(attachNS).Put(bytes.NewReader(bytes.Repeat([]byte("orphan"), 10_000))); err != nil {
		t.Fatal(err)
	}
	res, err := rt.CompactAttachments(attachNS)
	if err != nil {
		t.Fatal(err)
	}
	if res.Deltas != 2 || res.Removed != 1 {
		t.Fatalf("maintenance %+v, want 2 deltas and the orphan removed", res)
	}
	for i, c := range commits {
		if got, _ := readAttachment(t, rt, id, c); !bytes.Equal(got, versions[i]) {
			t.Fatalf("version %d reads back wrong after compaction", i)
		}
	}
}

// A record holds several attachments: each has an ID and a name, is found by either, is listed and
// counted, and is replaced, renamed or deleted alone.
func TestSeveralNamedAttachmentsPerRecord(t *testing.T) {
	rt := memoryRuntime(t)
	id := codec.UUID{MSB: 9}
	putAttachDoc(t, rt, id, `{"title":"album"}`)
	_, cover, err := rt.PutAttachment(attachNS, id, bytes.NewReader([]byte("cover bytes")), embed.PutAttachmentOptions{Name: "cover.jpg", Mime: "image/jpeg"})
	if err != nil {
		t.Fatal(err)
	}
	_, anon, err := rt.PutAttachment(attachNS, id, bytes.NewReader([]byte("no name given")), embed.PutAttachmentOptions{Mime: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if anon.Name != anon.ID.String() {
		t.Fatalf("an unnamed attachment is called %q, want its id %s", anon.Name, anon.ID)
	}
	given, _ := codec.UUIDFromString("0b6f3c1e-2a4d-4e8f-9a1b-3c5d7e9f1a2b")
	_, notes, err := rt.PutAttachment(attachNS, id, bytes.NewReader([]byte("notes")), embed.PutAttachmentOptions{ID: given, Name: "notes.md"})
	if err != nil || notes.ID != given {
		t.Fatalf("a caller-chosen id: %+v %v", notes, err)
	}
	if n, _ := rt.CountAttachments(attachNS, id, codec.Hash{}); n != 3 {
		t.Fatalf("count %d", n)
	}
	list, _ := rt.ListAttachments(attachNS, id, codec.Hash{})
	if len(list) != 3 || list[0].Name > list[1].Name || list[1].Name > list[2].Name {
		t.Fatalf("list not sorted by name: %+v", list)
	}
	// Found by name and by ID alike.
	if b, _ := readNamed(t, rt, id, embed.AttachmentByName("cover.jpg"), codec.Hash{}); string(b) != "cover bytes" {
		t.Fatal("by name")
	}
	if b, _ := readNamed(t, rt, id, embed.AttachmentByID(cover.ID), codec.Hash{}); string(b) != "cover bytes" {
		t.Fatal("by id")
	}
	// Replacing by name keeps the ID and leaves the others.
	beforeReplace, _ := rt.DAG.Head()
	_, again, err := rt.PutAttachment(attachNS, id, bytes.NewReader([]byte("cover v2")), embed.PutAttachmentOptions{Name: "cover.jpg", Mime: "image/jpeg"})
	if err != nil || again.ID != cover.ID {
		t.Fatalf("replace by name: %+v %v", again, err)
	}
	if b, _ := readNamed(t, rt, id, embed.AttachmentByID(cover.ID), codec.Hash{}); string(b) != "cover v2" {
		t.Fatal("replace did not take")
	}
	if b, _ := readNamed(t, rt, id, embed.AttachmentByID(cover.ID), beforeReplace); string(b) != "cover bytes" {
		t.Fatal("the old version as of its commit")
	}
	if b, _ := readNamed(t, rt, id, embed.AttachmentByName("notes.md"), codec.Hash{}); string(b) != "notes" {
		t.Fatal("replacing one attachment changed another")
	}
	// Rename keeps ID and bytes; the old name stops resolving; a taken name is refused.
	if _, renamed, err := rt.RenameAttachment(attachNS, id, embed.AttachmentByID(cover.ID), "front.jpg"); err != nil || renamed.ID != cover.ID {
		t.Fatalf("rename: %v", err)
	}
	if _, _, err := rt.GetAttachment(attachNS, id, embed.AttachmentByName("cover.jpg"), codec.Hash{}); !errors.Is(err, embed.ErrNoAttachment) {
		t.Fatalf("old name after rename: %v", err)
	}
	if _, _, err := rt.RenameAttachment(attachNS, id, embed.AttachmentByID(cover.ID), "notes.md"); err == nil {
		t.Fatal("renamed onto a name another attachment has")
	}
	if _, _, err := rt.PutAttachment(attachNS, id, bytes.NewReader([]byte("x")), embed.PutAttachmentOptions{ID: codec.UUID{MSB: 123}, Name: "notes.md"}); err == nil {
		t.Fatal("a new attachment took a name another has")
	}
	// Deleting one leaves the others; the JSON never moved.
	if _, err := rt.DeleteAttachment(attachNS, id, embed.AttachmentByName("notes.md")); err != nil {
		t.Fatal(err)
	}
	if n, _ := rt.CountAttachments(attachNS, id, codec.Hash{}); n != 2 {
		t.Fatalf("count after delete %d", n)
	}
}

// commitAttachmentPut commits a plain transaction putting data as id's attachment attID - the
// way a client, SQL or peer sync commits one, not PutAttachment.
func commitAttachmentPut(t *testing.T, rt *embed.EmbeddedKdbRuntime, id, attID codec.UUID, data []byte) codec.Hash {
	t.Helper()
	h, n, err := rt.PutAttachmentBlob(attachNS, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	head, _ := rt.DAG.Head()
	c, _ := rt.DAG.GetCommitOrThrow(head)
	tree, err := rt.Storage.CommitTree(attachNS, c.DocumentTreeHash)
	if err != nil {
		t.Fatal(err)
	}
	txID, _ := codec.RandomUUID()
	put := document.AttachmentPutOp{DocID: id, AttachmentID: attID, Name: "file",
		Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h, Size: n, Mime: "x/y"}
	tx := document.Transaction{ID: txID, BaseVersion: head, Operations: []document.Op{put}, Timestamp: codec.TimestampNow(), AuthorNodeID: txID}
	commit, err := rt.DAG.AppendCommit(tx, head, tree, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return commit.Hash
}

// A replacement committed as a plain transaction is re-stored as a delta too: maintenance reads
// replacements from the DAG. Its mark makes a second pass, and one after a restart, redo nothing,
// while a replacement committed after the restart is still found.
func TestTransactionReplacementIsStoredAsDelta(t *testing.T) {
	root := t.TempDir()
	rt := openAttachRuntime(t, root, storage.HistoryModeFull)
	id, attID := codec.UUID{MSB: 7}, codec.UUID{MSB: 7, LSB: 1}
	putAttachDoc(t, rt, id, `{}`)
	r := rand.New(rand.NewSource(11))
	base := make([]byte, 80_000)
	r.Read(base)
	version := func(i int) []byte {
		v := append([]byte(nil), base...)
		copy(v[i*7000:], []byte("an edit committed through a transaction"))
		return v
	}
	var commits []codec.Hash
	for i := 0; i < 2; i++ {
		commits = append(commits, commitAttachmentPut(t, rt, id, attID, version(i)))
	}
	compact := func(want int) {
		t.Helper()
		res, err := rt.CompactAttachments(attachNS)
		if err != nil {
			t.Fatal(err)
		}
		if res.Deltas != want || res.Removed != 0 {
			t.Fatalf("maintenance %+v, want %d deltas and nothing removed", res, want)
		}
	}
	compact(1)
	compact(0) // the mark: nothing left to do

	rt.Close()
	rt = openAttachRuntime(t, root, storage.HistoryModeFull)
	defer rt.Close()
	compact(0) // the mark survived the restart (and Collect)
	commits = append(commits, commitAttachmentPut(t, rt, id, attID, version(2)))
	compact(1)
	for i, c := range commits {
		if got, _ := readNamed(t, rt, id, embed.AttachmentByID(attID), c); !bytes.Equal(got, version(i)) {
			t.Fatalf("version %d reads back wrong after compaction", i)
		}
	}
}
