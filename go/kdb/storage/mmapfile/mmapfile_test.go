package mmapfile

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func writeFile(t *testing.T, content []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func open(t *testing.T, p string) *Mapping {
	t.Helper()
	m, err := Open(p)
	if errors.Is(err, ErrUnsupported) {
		t.Skip("mmap unsupported on this platform")
	}
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestReadAndCopyAt(t *testing.T) {
	want := bytes.Repeat([]byte("kdb-"), 5000)
	m := open(t, writeFile(t, want))
	r, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Bytes(), want) {
		t.Fatal("mapped bytes differ from file")
	}
	got, err := r.CopyAt(8, 100)
	if err != nil || !bytes.Equal(got, want[8:108]) {
		t.Fatalf("CopyAt: %v", err)
	}
	if _, err := r.CopyAt(len(want)-1, 2); err == nil {
		t.Fatal("out-of-range CopyAt must fail")
	}
	r.Release()
	r.Release() // idempotent
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyFile(t *testing.T) {
	m := open(t, writeFile(t, nil))
	if m.Size() != 0 {
		t.Fatal("empty file must map to an empty mapping")
	}
	r, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	r.Release()
	m.Close()
}

// The owner closing while a reader holds a Region must not unmap: the reader's bytes stay valid
// until its Release, and only then does the unmap happen. This is the rebuild-while-reading rule.
func TestCloseDefersUnmapUntilLastRelease(t *testing.T) {
	want := bytes.Repeat([]byte{0xAB}, 1<<16)
	m := open(t, writeFile(t, want))
	r, _ := m.Acquire()
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if m.done {
		t.Fatal("unmapped while a reader still held a Region")
	}
	if got, err := r.CopyAt(0, len(want)); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("reader lost its bytes after owner Close: %v", err)
	}
	if _, err := m.Acquire(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire after Close = %v, want ErrClosed", err)
	}
	r.Release()
	if !m.done {
		t.Fatal("last Release did not unmap")
	}
}

// Truncating the file under a mapped reader raises SIGBUS on touch; CopyAt must turn that into an
// error rather than killing the process.
func TestCopyAtSurvivesTruncatedFile(t *testing.T) {
	p := writeFile(t, bytes.Repeat([]byte{1}, 1<<16))
	m := open(t, p)
	defer m.Close()
	r, _ := m.Acquire()
	defer r.Release()
	if err := os.Truncate(p, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CopyAt(0, 1<<16); err == nil {
		t.Skip("platform kept the pages resident after truncate; no fault to observe")
	}
}

func TestConcurrentReadersVersusClose(t *testing.T) {
	want := bytes.Repeat([]byte("x"), 1<<16)
	p := writeFile(t, want)
	for round := 0; round < 50; round++ {
		m := open(t, p)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					r, err := m.Acquire()
					if err != nil {
						return // owner closed: reader reopens in real code
					}
					if got, err := r.CopyAt(0, 4096); err != nil || got[0] != 'x' {
						t.Errorf("bad read: %v", err)
					}
					r.Release()
				}
			}()
		}
		m.Close()
		wg.Wait()
		if !m.done {
			t.Fatal("mapping leaked after all readers released")
		}
	}
}
