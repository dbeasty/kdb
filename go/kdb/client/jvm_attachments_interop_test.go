//go:build interop

// jvm_attachments_interop_test.go: the Go client's attachment API against a real JVM kdb-service
// (kdb-server's AttachmentsWire.kt) - uploads, listings, whole and verified ranged reads, a read
// pinned across a replace, renames and deletes, all over the real frames. Gated like
// jvm_ws_interop_test.go: the "interop" tag and a running server at KDB_JVM_SQL_WS_URI (see that
// file for how to start one; CI's go-interop job does).
package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/document"
)

func TestAttachmentsAgainstRealJvmServer(t *testing.T) {
	uri := os.Getenv("KDB_JVM_SQL_WS_URI")
	if uri == "" {
		t.Skip("KDB_JVM_SQL_WS_URI not set - see jvm_ws_interop_test.go for how to start a JVM server to test against")
	}
	ns := os.Getenv("KDB_JVM_SQL_NAMESPACE")
	if ns == "" {
		ns = "demo/interop"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := client.Connect(ctx, uri, "")
	if err != nil {
		t.Fatalf("connect to %s: %v", uri, err)
	}
	defer c.Close()

	doc, err := randomUUID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutJSON(ctx, ns, doc, []byte(`{"title":"go-attachments-interop"}`)); err != nil {
		t.Fatalf("PutJSON: %v", err)
	}

	// document's range-proof golden attachment: 4 MiB + 5 bytes, byte i = i mod 251.
	data := make([]byte, 4<<20+5)
	for i := range data {
		data[i] = byte(i % 251)
	}
	const goldenHash = "1a57b9052a9bae5f2bbaa720791a30f99ea84caecaa0898551c84d1746d3ae27"
	commit1, info, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader(data), client.PutAttachmentOptions{Name: "r.bin", Mime: "application/octet-stream"})
	if err != nil {
		t.Fatalf("PutAttachment: %v", err)
	}
	if info.Hash != goldenHash || info.Size != int64(len(data)) {
		t.Fatalf("put %s/%d, want %s/%d", info.Hash, info.Size, goldenHash, len(data))
	}

	list, _, err := c.ListAttachments(ctx, ns, doc, "")
	if err != nil || len(list) != 1 || list[0] != info {
		t.Fatalf("list: %v %+v, want [%+v]", err, list, info)
	}
	got, gotInfo, err := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("r.bin"), "")
	if err != nil || !bytes.Equal(got, data) || gotInfo.ID != info.ID {
		t.Fatalf("read whole: %v (%d bytes)", err, len(got))
	}

	// Verified ranges: the Kotlin server's pages are chunk-aligned and every chunk proved.
	ref := client.ByAttachmentID(info.ID)
	size := int64(len(data))
	for _, r := range [][2]int64{{0, 10}, {1<<20 + 100, 50}, {1<<20 - 10, 20}, {100, 4<<20 + 7 - 100}, {0, -1}, {size - 1, 1}, {size, -1}} {
		rd, err := c.OpenAttachmentRange(ctx, ns, doc, ref, "", r[0], r[1])
		if err != nil {
			t.Fatalf("range %v: %v", r, err)
		}
		b, err := io.ReadAll(rd)
		rd.Close()
		end := size
		if r[1] >= 0 {
			end = min(size, r[0]+r[1])
		}
		if err != nil || !bytes.Equal(b, data[r[0]:end]) {
			t.Fatalf("range %v: %v, %d bytes", r, err, len(b))
		}
	}
	var rangeErr *client.AttachmentRangeError
	if _, err := c.OpenAttachmentRange(ctx, ns, doc, ref, "", size+1, 1); !errors.As(err, &rangeErr) || rangeErr.Size != size {
		t.Fatalf("past the end: %v", err)
	}

	// A read pins its version across a replace, and says so at the end.
	rd, err := c.OpenAttachment(ctx, ns, doc, ref, "")
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 10)
	if _, err := io.ReadFull(rd, head); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader([]byte("replaced")), client.PutAttachmentOptions{ID: info.ID}); err != nil {
		t.Fatalf("replace: %v", err)
	}
	rest, err := io.ReadAll(rd)
	rd.Close()
	if err != nil || !bytes.Equal(append(head, rest...), data) || !rd.Superseded() {
		t.Fatalf("pinned read: %v, %d bytes, superseded=%v", err, len(head)+len(rest), rd.Superseded())
	}
	if b, _, err := c.ReadAttachment(ctx, ns, doc, ref, ""); err != nil || string(b) != "replaced" {
		t.Fatalf("after replace: %v %q", err, b)
	}
	// The first version, as of its commit.
	if b, _, err := c.ReadAttachment(ctx, ns, doc, ref, commit1); err != nil || !bytes.Equal(b, data) {
		t.Fatalf("as of %s: %v", commit1, err)
	}

	// The empty attachment: never stored, always readable.
	_, empty, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader(nil), client.PutAttachmentOptions{Name: "empty"})
	if err != nil || empty.Hash != document.AttachmentContentHash(nil).Hex() {
		t.Fatalf("empty: %v %+v", err, empty)
	}
	if b, _, err := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("empty"), ""); err != nil || len(b) != 0 {
		t.Fatalf("read empty: %v %d", err, len(b))
	}

	if _, err := c.RenameAttachment(ctx, ns, doc, ref, "renamed.bin"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if _, err := c.DeleteAttachment(ctx, ns, doc, client.ByAttachmentName("renamed.bin")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if list, _, err := c.ListAttachments(ctx, ns, doc, ""); err != nil || len(list) != 1 || list[0].Name != "empty" {
		t.Fatalf("after delete: %v %+v", err, list)
	}
	if _, _, err := c.ReadAttachment(ctx, ns, doc, client.ByAttachmentName("renamed.bin"), ""); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("deleted attachment read: %v, want ErrNotFound", err)
	}
}
