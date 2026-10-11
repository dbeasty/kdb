package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/backup"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
)

// kdb-inspect backup over a segment damaged in place must fail - main turns the error into exit
// status 1 and prints it - with the typed refusal, for one namespace, for an incremental, and for
// the whole database, and record nothing.
func TestBackupCmdRefusesADamagedSegment(t *testing.T) {
	root := t.TempDir()
	opts := embed.FileRuntimeOptions{}
	none := storage.CompressionNone
	opts.Storage.Compression = &none
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", "app/cli", schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := embed.PutJSONDocument(rt, "app/cli", fmt.Sprintf(`{"id":"d%d"}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	rt.Close()

	to := t.TempDir()
	if err := run([]string{"backup", "--data-dir", root, "--namespace", "app/cli", "--to", to}); err != nil {
		t.Fatalf("the clean backup should succeed: %v", err)
	}
	ids, err := backup.ListBackups(&backup.DirStore{Root: to}, "app/cli")
	if err != nil || len(ids) != 1 {
		t.Fatalf("want one backup, got %v (%v)", ids, err)
	}

	segs, _ := filepath.Glob(filepath.Join(root, "ns", "app", "cli", "delta", "*.seg"))
	path := segs[len(segs)-1]
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	second := 20 + (int(raw[8])<<24 | int(raw[9])<<16 | int(raw[10])<<8 | int(raw[11]))
	raw[second] ^= 0xFF // the second frame's magic; two intact frames follow it
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	for _, args := range [][]string{
		{"backup", "--data-dir", root, "--namespace", "app/cli", "--to", to},
		{"backup", "--data-dir", root, "--namespace", "app/cli", "--to", to, "--base-backup-id", ids[0]},
		{"backup", "--data-dir", root, "--to", to},
	} {
		err := run(args)
		var refused *backup.DamagedSegmentError
		if !errors.As(err, &refused) {
			t.Fatalf("%v: want *backup.DamagedSegmentError, got %v", args, err)
		}
		if refused.Offset != int64(second) || !strings.Contains(err.Error(), "kdb-inspect verify") {
			t.Fatalf("%v: the refusal should name byte %d and the next step: %v", args, second, err)
		}
	}
	if after, _ := backup.ListBackups(&backup.DirStore{Root: to}, "app/cli"); len(after) != 1 {
		t.Fatalf("a refused backup recorded a manifest: %v", after)
	}
	if dbs, _ := backup.ListDatabaseBackups(&backup.DirStore{Root: to}); len(dbs) != 0 {
		t.Fatalf("a refused database backup recorded %v", dbs)
	}
}
