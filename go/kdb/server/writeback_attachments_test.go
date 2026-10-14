package server

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
)

func readAttachmentOf(t *testing.T, rt *KdbServerRuntime, doc codec.UUID, name string) []byte {
	t.Helper()
	head, err := rt.dag.Head()
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := rt.Runtime.GetAttachment(rt.Runtime.DefaultNamespace, doc, embed.AttachmentByName(name), head)
	if err != nil {
		return nil
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func attachmentNames(t *testing.T, rt *KdbServerRuntime, doc codec.UUID) []string {
	t.Helper()
	head, _ := rt.dag.Head()
	list, err := rt.Runtime.ListAttachments(rt.Runtime.DefaultNamespace, doc, head)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, a := range list {
		out = append(out, a.Name)
	}
	return out
}

// localAttach makes an attachment change on the projection the way a client does: the bytes
// first, then a commit through the server's write path, which records it as a pending write.
func (f *projectionFixture) localAttach(doc codec.UUID, name string, data []byte) {
	f.t.Helper()
	rt := f.replica
	h, n, err := rt.Runtime.PutAttachmentBlob(rt.Runtime.DefaultNamespace, bytes.NewReader(data))
	if err != nil {
		f.t.Fatal(err)
	}
	head, _ := rt.dag.Head()
	id := mustRandomUUID(f.t)
	if a, ok := func() (document.Attachment, bool) {
		table, _ := rt.dag.AttachmentTableAt(head)
		a, ok, err := table.GetByName(doc, name)
		if err != nil {
			f.t.Fatal(err)
		}
		return a, ok
	}(); ok {
		id = a.ID
	}
	tx := document.Transaction{ID: mustRandomUUID(f.t), BaseVersion: head, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.AttachmentPutOp{DocID: doc, AttachmentID: id, Name: name,
			Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h, Size: n}}}
	if _, err := rt.Commit(rt.Runtime.DefaultNamespace, tx, "", auth.Principal{}); err != nil {
		f.t.Fatal(err)
	}
}

// Attachments through a write-back projection, both ways: pulled with their documents (and when
// they alone change), written back with their bytes, kept by a JSON-only write-back, and put back
// from the source when a write-back is refused.
func TestWriteBackProjectionCarriesAttachments(t *testing.T) {
	f := newWriteBackFixture(t, `region = 'EU'`)
	x := mustRandomUUID(t)
	f.put(x, `{"region":"EU","n":1}`)
	big := bytes.Repeat([]byte("source bytes "), 200_000) // 2.6 MB, several chunks
	if _, _, err := f.source.Runtime.PutAttachment("app/data", x, bytes.NewReader(big), embed.PutAttachmentOptions{Name: "a.bin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.syncBack(f.addr); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAttachmentOf(t, f.replica, x, "a.bin"), big) {
		t.Fatal("the projection did not take the document's attachment")
	}

	// An attachment-only change at the source reaches the projection.
	bigger := bytes.Repeat([]byte("changed "), 300_000)
	if _, _, err := f.source.Runtime.PutAttachment("app/data", x, bytes.NewReader(bigger), embed.PutAttachmentOptions{Name: "a.bin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.syncBack(f.addr); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readAttachmentOf(t, f.replica, x, "a.bin"), bigger) {
		t.Fatal("an attachment-only change at the source did not reach the projection")
	}

	// A JSON-only write-back leaves the document's attachments at the source alone.
	f.localPut(x, `{"region":"EU","n":2}`)
	if res, err := f.syncBack(f.addr); err != nil || res.Written != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	if !strings.Contains(f.sourceDoc(x), `"n":2`) || !bytes.Equal(readAttachmentOf(t, f.source, x, "a.bin"), bigger) {
		t.Fatal("a JSON write-back lost the document's attachment at the source")
	}
	if !bytes.Equal(readAttachmentOf(t, f.replica, x, "a.bin"), bigger) {
		t.Fatal("the projection lost the attachment applying a page")
	}

	// An attachment written on the projection reaches the source, bytes and all.
	local := bytes.Repeat([]byte("from the projection "), 100_000)
	f.localAttach(x, "local.bin", local)
	if res, err := f.syncBack(f.addr); err != nil || res.Written != 1 || res.Rejected != 0 {
		t.Fatalf("%+v %v", res, err)
	}
	if !bytes.Equal(readAttachmentOf(t, f.source, x, "local.bin"), local) {
		t.Fatal("an attachment written on the projection did not reach the source")
	}

	// Both sides change the same document's attachments: the projection's write is refused, and
	// the projection is put back to the source's attachments, bytes fetched.
	if _, _, err := f.source.Runtime.PutAttachment("app/data", x, bytes.NewReader([]byte("source wins")), embed.PutAttachmentOptions{Name: "s.txt"}); err != nil {
		t.Fatal(err)
	}
	f.localAttach(x, "mine.txt", []byte("refused"))
	res, err := f.syncBack(f.addr)
	if err != nil || res.Rejected != 1 {
		t.Fatalf("expected the write to be rejected: %+v %v", res, err)
	}
	if got := strings.Join(attachmentNames(t, f.replica, x), ","); got != strings.Join(attachmentNames(t, f.source, x), ",") {
		t.Fatalf("projection has %s, source %s", got, strings.Join(attachmentNames(t, f.source, x), ","))
	}
	if string(readAttachmentOf(t, f.replica, x, "s.txt")) != "source wins" || readAttachmentOf(t, f.replica, x, "mine.txt") != nil {
		t.Fatal("the projection was not put back to the source's attachments")
	}
}
