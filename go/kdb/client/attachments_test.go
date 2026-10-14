package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
)

func attachmentBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7) ^ seed
	}
	return b
}

func TestAttachmentsOverTheWire(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const ns = "app/data"

	id, _ := codec.RandomUUID()
	doc := id.String()
	if _, err := c.PutJSON(ctx, ns, doc, []byte(`{"title":"report"}`)); err != nil {
		t.Fatal(err)
	}

	// Put: 3.5 MiB, several chunks; then a small one by name only.
	big := attachmentBytes(3<<20+512<<10, 1)
	_, photo, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader(big), client.PutAttachmentOptions{Name: "photo.bin", Mime: "application/octet-stream"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader([]byte("hi")), client.PutAttachmentOptions{Name: "note.txt", Mime: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	list, _, err := c.ListAttachments(ctx, ns, doc, "")
	if err != nil || len(list) != 2 || list[0].Name != "note.txt" || list[1].Name != "photo.bin" || list[1].Size != int64(len(big)) {
		t.Fatalf("list %+v %v", list, err)
	}
	got, info, err := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("photo.bin"), "")
	if err != nil || !bytes.Equal(got, big) || info.ID != photo.ID {
		t.Fatalf("read back %d bytes, info %+v, %v", len(got), info, err)
	}

	// A read pinned to its version: replaced half way, it finishes on the old bytes and says so.
	r, err := c.OpenAttachment(ctx, ns, doc, client.ByAttachmentID(photo.ID), "")
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 1<<20)
	if _, err := io.ReadFull(r, head); err != nil {
		t.Fatal(err)
	}
	replacement := attachmentBytes(2<<20, 9)
	_, replaced, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader(replacement), client.PutAttachmentOptions{Name: "photo.bin"})
	if err != nil || replaced.ID != photo.ID || replaced.Mime != "application/octet-stream" {
		t.Fatalf("replace by name: %+v %v", replaced, err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(head, rest...), big) {
		t.Fatal("the pinned read did not finish on the version it started on")
	}
	if !r.Superseded() {
		t.Fatal("the read did not report the newer version")
	}
	if got, _, _ := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("photo.bin"), ""); !bytes.Equal(got, replacement) {
		t.Fatal("a new read does not see the replacement")
	}

	// Rename, delete, a closed-early read, and NOT_FOUND.
	if _, err := c.RenameAttachment(ctx, ns, doc, client.ByAttachmentName("note.txt"), "readme.txt"); err != nil {
		t.Fatal(err)
	}
	r2, err := c.OpenAttachment(ctx, ns, doc, client.ByAttachmentName("photo.bin"), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteAttachment(ctx, ns, doc, client.ByAttachmentName("photo.bin")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("photo.bin"), ""); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("a deleted attachment reads as %v", err)
	}
	list, _, _ = c.ListAttachments(ctx, ns, doc, "")
	if len(list) != 1 || list[0].Name != "readme.txt" {
		t.Fatalf("after rename+delete: %+v", list)
	}

	// An upload attached by SQL (STREAM ?), and bytes read back as a typed cell.
	blob, err := c.UploadAttachment(ctx, ns, bytes.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, ns, "UPDATE data SET ATTACHMENT 'via-sql' = STREAM ?, ATTACHMENT 'inline' = ? WHERE title = 'report'",
		[]any{blob, client.Bytes{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	_, rows, err := c.QueryValues(ctx, ns, "SELECT ATTACHMENT('inline'), ATTACHMENT_SIZE('via-sql') FROM data", nil)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v %v", rows, err)
	}
	if b, ok := rows[0][0].([]byte); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Fatalf("bytes cell %#v", rows[0][0])
	}
	if rows[0][1] != int64(len(big)) {
		t.Fatalf("size cell %#v", rows[0][1])
	}

	// A transaction carrying a document write and an attachment together.
	other, _ := codec.RandomUUID()
	small, err := c.UploadAttachment(ctx, ns, bytes.NewReader([]byte("together")))
	if err != nil {
		t.Fatal(err)
	}
	headHex, err := c.PutJSON(ctx, ns, doc, []byte(`{"title":"report","v":2}`))
	if err != nil {
		t.Fatal(err)
	}
	attID, _ := codec.RandomUUID()
	if _, err := c.Commit(ctx, client.Transaction{Namespace: ns, BaseVersion: headHex,
		Writes:      []client.DocWrite{{DocID: other.String(), JSON: []byte(`{"title":"other"}`)}},
		Attachments: []client.AttachmentChange{{DocID: other.String(), ID: attID.String(), Name: "t.txt", Hash: small.Hash, Size: small.Size}},
	}); err != nil {
		t.Fatal(err)
	}
	if got, _, err := c.ReadAttachment(ctx, ns, other.String(), client.ByAttachmentID(attID.String()), ""); err != nil || string(got) != "together" {
		t.Fatalf("%q %v", got, err)
	}
	// An attachment whose blob was never uploaded is refused at commit.
	if _, err := c.Commit(ctx, client.Transaction{Namespace: ns, BaseVersion: headHex,
		Attachments: []client.AttachmentChange{{DocID: other.String(), ID: attID.String(), Name: "x", Hash: codec.Hash{}.Hex(), Size: 1}},
	}); err == nil {
		t.Fatal("an attachment of bytes never uploaded was committed")
	}
}
