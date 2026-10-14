package embed

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestMigrateFileAttachments(t *testing.T) {
	dir := t.TempDir()
	rt, err := OpenFileRuntime(filepath.Join(dir, "data"), "app", "app/files", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	h, err := rt.Storage.WriteBlob([]byte("old bytes"))
	if err != nil {
		t.Fatal(err)
	}
	put, err := PutJSONDocument(rt, "app/files", fmt.Sprintf(`{"name":"report.txt","mime":"text/plain","blobHash":"%s"}`, h.Hex()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PutJSONDocument(rt, "app/files", `{"name":"lost","blobHash":"`+strings.Repeat("ab", 32)+`"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := PutJSONDocument(rt, "app/files", `{"plain":true}`); err != nil {
		t.Fatal(err)
	}

	dry, err := rt.MigrateFileAttachments("app/files", true, true)
	if err != nil || dry.Migrated != 1 || len(dry.Missing) != 1 || dry.Scanned != 3 {
		t.Fatalf("dry run %+v %v", dry, err)
	}
	if n, _ := rt.CountAttachments("app/files", put.DocID, mustHead(t, rt)); n != 0 {
		t.Fatal("a dry run wrote")
	}
	res, err := rt.MigrateFileAttachments("app/files", true, false)
	if err != nil || res.Migrated != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	again, err := rt.MigrateFileAttachments("app/files", true, false)
	if err != nil || again.Migrated != 0 {
		t.Fatalf("a second migration did more: %+v %v", again, err)
	}
	rt.Close()

	// Durable: reopen and read it back; the stripped record no longer names the blob.
	rt, err = OpenFileRuntime(filepath.Join(dir, "data"), "app", "app/files", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	r, info, err := rt.GetAttachment("app/files", put.DocID, AttachmentByName("report.txt"), mustHead(t, rt))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) != "old bytes" || info.Mime != "text/plain" {
		t.Fatalf("%q %+v", b, info)
	}
	head, _ := rt.DAG.GetCommitOrThrow(mustHead(t, rt))
	doc, err := rt.Storage.GetDocument("app/files", put.DocID, head.DocumentTreeHash)
	if err != nil || doc == nil || strings.Contains(doc.JSON, "blobHash") || !strings.Contains(doc.JSON, "report.txt") {
		t.Fatalf("record after strip: %+v %v", doc, err)
	}
}

func mustHead(t *testing.T, rt *EmbeddedKdbRuntime) codec.Hash {
	t.Helper()
	h, err := rt.DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}
