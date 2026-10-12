package fsx_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/fsx"
)

// The storage engine deletes a segment while a reader may still hold it open. That has always
// worked on unix; these pin that it works through fsx everywhere, which on Windows is the whole
// point of the package.

func TestRemoveWhileOpen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "seg")
	if err := os.WriteFile(p, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := fsx.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := fsx.Remove(p); err != nil {
		t.Fatalf("remove of an open file: %v", err)
	}
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, 0); err != nil || string(buf) != "data" {
		t.Fatalf("read through the handle after remove = %q, %v", buf, err)
	}
}

func TestRenameWhileOpen(t *testing.T) {
	dir := t.TempDir()
	from, to := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	f, err := fsx.OpenFile(from, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := fsx.Rename(from, to); err != nil {
		t.Fatalf("rename of an open file: %v", err)
	}
	if b, err := os.ReadFile(to); err != nil || string(b) != "x" {
		t.Fatalf("renamed file = %q, %v", b, err)
	}
}

func TestRenameReplaces(t *testing.T) {
	dir := t.TempDir()
	from, to := filepath.Join(dir, "tmp"), filepath.Join(dir, "final")
	for p, body := range map[string]string{from: "new", to: "old"} {
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := fsx.Rename(from, to); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(to); string(b) != "new" {
		t.Fatalf("target = %q, want new", b)
	}
	if err := fsx.SyncDir(dir); err != nil {
		t.Fatalf("SyncDir: %v", err)
	}
}

func TestOpenFileFlags(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	f, err := fsx.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("hello")
	f.Close()
	if _, err := fsx.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !os.IsExist(err) {
		t.Fatalf("O_EXCL on an existing file: err = %v, want exist", err)
	}
	if _, err := fsx.Open(filepath.Join(filepath.Dir(p), "missing")); !os.IsNotExist(err) {
		t.Fatalf("open of a missing file: err = %v, want not-exist", err)
	}
	f, err = fsx.OpenFile(p, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if info, _ := os.Stat(p); info.Size() != 0 {
		t.Fatalf("O_TRUNC left %d bytes", info.Size())
	}
	f, err = fsx.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("ab")
	_, _ = f.WriteString("cd")
	f.Close()
	if b, _ := os.ReadFile(p); string(b) != "abcd" {
		t.Fatalf("O_APPEND wrote %q", b)
	}
	d, err := fsx.Open(filepath.Dir(p))
	if err != nil {
		t.Fatalf("opening a directory: %v", err)
	}
	d.Close()
}
