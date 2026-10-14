package embed_test

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/recovery"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// A backup holds a namespace's attachments, small (inline in their pointers) and large (segments
// in parts), and a restore from it reads them back; an incremental backup of unchanged attachments
// uploads none of their bytes again.
func TestBackupAndRestoreCarryAttachments(t *testing.T) {
	root := t.TempDir()
	const ns = "files/docs"
	host, err := embed.OpenFileHost(root, hostOpts())
	if err != nil {
		t.Fatal(err)
	}
	rt, err := host.Namespace("files", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	doc := put(t, rt, ns, `{"t":"doc"}`)
	big := bytes.Repeat([]byte("large attachment "), 200_000) // 3.4 MB
	if _, _, err := rt.PutAttachment(ns, doc, bytes.NewReader(big), embed.PutAttachmentOptions{Name: "big"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.PutAttachment(ns, doc, bytes.NewReader([]byte("tiny")), embed.PutAttachmentOptions{Name: "small"}); err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}

	store := &backup.DirStore{Root: filepath.Join(t.TempDir(), "backups")}
	m, err := backup.Create(dirShim(t, root), ns, store, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Attachments) == 0 {
		t.Fatal("the backup holds no attachment segments")
	}
	if res, err := backup.Verify(store, ns, m.BackupID); err != nil || !res.Clean() {
		t.Fatalf("verify: %+v %v", res, err)
	}
	keys, _ := store.List(t.Context(), "attachments/")
	again, err := backup.Create(dirShim(t, root), ns, store, m.BackupID)
	if err != nil {
		t.Fatal(err)
	}
	if keys2, _ := store.List(t.Context(), "attachments/"); len(keys2) != len(keys) || len(again.Attachments) != len(m.Attachments) {
		t.Fatalf("the incremental backup stored attachment parts again: %d then %d", len(keys), len(keys2))
	}

	fetchDir, out := t.TempDir(), t.TempDir()
	if _, err := backup.FetchToDir(store, ns, again.BackupID, fetchDir); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.HybridRestore([]recovery.Source{{Label: "backup", Shim: dirShim(t, fetchDir)}}, ns, storage.CompressionZSTD, dirShim(t, out)); err != nil {
		t.Fatal(err)
	}
	restored, err := embed.OpenFileHost(out, hostOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	rt, err = restored.Namespace("files", ns, schema.None())
	if err != nil {
		t.Fatal(err)
	}
	head, _ := rt.DAG.Head()
	for name, want := range map[string][]byte{"big": big, "small": []byte("tiny")} {
		r, _, err := rt.GetAttachment(ns, doc, embed.AttachmentByName(name), head)
		if err != nil {
			t.Fatalf("%s after restore: %v", name, err)
		}
		got, _ := io.ReadAll(r)
		r.Close()
		if !bytes.Equal(got, want) {
			t.Fatalf("%s restored as %d bytes", name, len(got))
		}
	}
}
