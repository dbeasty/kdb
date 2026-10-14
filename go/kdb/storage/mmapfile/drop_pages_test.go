package mmapfile

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// writtenFile makes a file of size bytes of real data (byte i is i%256), so its pages are
// file-backed rather than holes. Written in chunks, so the test's own heap does not grow by size.
func writtenFile(t *testing.T, size int) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "data")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i)
	}
	for written := 0; written < size; written += len(chunk) {
		if _, err := f.Write(chunk[:min(len(chunk), size-written)]); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

// processRSS is this process's resident set as ps reports it: what DropPages is for, and what
// mincore cannot show - for a file mapping it reports page-cache residency, which a page keeps
// after leaving this process's resident set.
func processRSS(t *testing.T) int64 {
	t.Helper()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Skipf("ps: %v", err)
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Skipf("ps output %q: %v", out, err)
	}
	return kb << 10
}

// DropPages takes a read mapping's pages out of the process's resident set, and the bytes stay
// readable afterwards.
func TestDropPagesLowersRSS(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("DropPages is a no-op here")
	}
	const size = 128 << 20
	m := open(t, writtenFile(t, size))
	defer m.Close()
	r, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	data := r.Bytes()
	touch(data, len(data))
	touched := processRSS(t)
	if err := r.DropPages(0, len(data)); err != nil {
		t.Fatal(err)
	}
	dropped := processRSS(t)
	t.Logf("RSS %d MB after reading the mapping, %d MB after DropPages", touched>>20, dropped>>20)
	if touched-dropped < size/2 {
		t.Fatalf("RSS fell by %d MB after dropping a %d MB mapping", (touched-dropped)>>20, size>>20)
	}
	for _, off := range []int{0, 4097, size/2 + 3, size - 1} {
		if data[off] != byte(off) {
			t.Fatalf("byte %d reads %d after DropPages, want %d", off, data[off], byte(off))
		}
	}
	if err := r.DropPages(0, len(data)+1); err == nil {
		t.Fatal("a range past the mapping was accepted")
	}
	if err := r.DropPages(-1, 1); err == nil {
		t.Fatal("a negative offset was accepted")
	}
}

// Only the whole pages inside a range are dropped, so a reader can drop what lies behind its
// position without losing the page it is reading.
func TestWholePages(t *testing.T) {
	const p = 4096
	for _, tc := range []struct{ off, n, start, end int }{
		{0, 3 * p, 0, 3 * p},
		{1, 3 * p, p, 3 * p},
		{p + 1, 39 * p, 2 * p, 40 * p},
		{p / 2, p / 4, p, 0},
		{0, p - 1, 0, 0},
	} {
		start, end := wholePages(tc.off, tc.n, p)
		if end <= start {
			start, end = 0, 0
		}
		if tc.end <= tc.start {
			tc.start, tc.end = 0, 0
		}
		if start != tc.start || end != tc.end {
			t.Errorf("wholePages(%d, %d) = [%d, %d), want [%d, %d)", tc.off, tc.n, start, end, tc.start, tc.end)
		}
	}
}
