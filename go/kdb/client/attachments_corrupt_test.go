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
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
)

// Stored bytes damaged on the server's disk reach the client as ErrAttachmentCorrupt - the server
// refuses them with INTEGRITY - on every read path: a whole read, a page after an intact first one,
// and SQL ATTACHMENT(). Before, only the client's own verification produced it; a server refusal
// arrived as an untyped error a caller could not tell from any other failure.
func TestCorruptStoredAttachmentIsTyped(t *testing.T) {
	addr, rt := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const ns = "app/data"

	id, _ := codec.RandomUUID()
	doc := id.String()
	if _, err := c.PutJSON(ctx, ns, doc, []byte(`{"title":"report"}`)); err != nil {
		t.Fatal(err)
	}
	data := attachmentBytes(3<<20, 5) // three chunks, three default pages
	_, info, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader(data), client.PutAttachmentOptions{Name: "scan.bin"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := codec.HashFromHex(info.Hash)
	if err != nil {
		t.Fatal(err)
	}
	// Damage the third chunk only: the first page still verifies and is served.
	if err := rt.Runtime.AttachmentStore(ns).CorruptForTest(h, 2<<20+17); err != nil {
		t.Fatal(err)
	}
	before := attachstore.CorruptReads()

	if _, _, err := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("scan.bin"), ""); !errors.Is(err, client.ErrAttachmentCorrupt) {
		t.Fatalf("a whole read of damaged bytes: %v", err)
	}

	r, err := c.OpenAttachment(ctx, ns, doc, client.ByAttachmentName("scan.bin"), "")
	if err != nil {
		t.Fatalf("the first page is intact: %v", err)
	}
	got, err := io.ReadAll(r)
	if !errors.Is(err, client.ErrAttachmentCorrupt) {
		t.Fatalf("a later page of damaged bytes: %v", err)
	}
	if !bytes.Equal(got, data[:len(got)]) || len(got) >= 2<<20+17 {
		t.Fatalf("served %d bytes up to the damage, or wrong ones", len(got))
	}
	_ = r.Close()

	if _, _, err := c.QueryValues(ctx, ns, "SELECT ATTACHMENT('scan.bin') FROM data", nil); !errors.Is(err, client.ErrAttachmentCorrupt) {
		t.Fatalf("SQL ATTACHMENT() of damaged bytes: %v", err)
	}

	if n := attachstore.CorruptReads() - before; n < 3 {
		t.Fatalf("corrupt reads counted %d times, want one per refusal", n)
	}
	// Not every failure is corruption.
	if _, _, err := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("missing"), ""); errors.Is(err, client.ErrAttachmentCorrupt) || !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("a missing attachment: %v", err)
	}
}
