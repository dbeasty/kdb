package mmapfile

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// sparseFile makes a file of size bytes that is all hole: none of its pages are in the page cache
// until something reads them, so a fresh mapping of it starts with nothing resident.
func sparseFile(t *testing.T, size int64) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "sparse")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

// sink keeps page-touching loops from being optimised away.
var sink byte

// touch reads one byte of every page in data[:n], faulting each page in.
func touch(data []byte, n int) {
	var sum byte
	for i := 0; i < n; i += os.Getpagesize() {
		sum += data[i]
	}
	sink += sum
}

func residentNow() int64 {
	resident.mu.Lock()
	defer resident.mu.Unlock()
	return residentBytesNow()
}

func TestResidentBytesRisesAfterTouchAndFallsOnUnmap(t *testing.T) {
	const size = 32 << 20
	m := open(t, sparseFile(t, size))
	before := residentNow()
	if mapped := MappedBytes(); before > mapped {
		t.Fatalf("resident %d exceeds mapped %d", before, mapped)
	}
	if before >= size/2 {
		// Every other live mapping in the test binary is tiny; a fresh sparse mapping must not
		// show up as resident.
		t.Fatalf("untouched %d-byte mapping already counts %d resident bytes", size, before)
	}

	r, err := m.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	data := r.Bytes()
	half := len(data) / 2
	touch(data, half)
	touched := residentNow()
	if touched-before < int64(half)*9/10 {
		t.Fatalf("touched %d bytes but resident rose only %d -> %d", half, before, touched)
	}
	if touched > MappedBytes() {
		t.Fatalf("resident %d exceeds mapped %d", touched, MappedBytes())
	}
	r.Release()
	m.Close()
	if after := residentNow(); after > touched-int64(half)*9/10 {
		t.Fatalf("unmapping did not drop resident bytes: %d -> %d", touched, after)
	}
}

func TestResidentBytesNeverExceedsMapped(t *testing.T) {
	m := open(t, writeFile(t, make([]byte, 3*os.Getpagesize()+17))) // partial last page
	defer m.Close()
	r, _ := m.Acquire()
	_, _ = r.CopyAt(0, m.Size())
	r.Release()
	if got, mapped := residentNow(), MappedBytes(); got > mapped {
		t.Fatalf("resident %d exceeds mapped %d", got, mapped)
	}
	if got, mapped := ResidentBytes(), MappedBytes(); got > mapped {
		t.Fatalf("cached resident %d exceeds mapped %d", got, mapped)
	}
}

// Counting residency pins each mapping; owners closing and readers releasing concurrently must
// neither unmap under the count nor leak a mapping the count was holding.
func TestResidentBytesVersusConcurrentClose(t *testing.T) {
	p := writeFile(t, make([]byte, 1<<20))
	stop := make(chan struct{})
	var counter sync.WaitGroup
	counter.Add(1)
	go func() {
		defer counter.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = residentNow()
			}
		}
	}()
	deadline := time.Now().Add(300 * time.Millisecond)
	var ms []*Mapping
	for time.Now().Before(deadline) {
		m := open(t, p)
		r, err := m.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		_, _ = r.CopyAt(0, 4096)
		m.Close()
		r.Release()
		ms = append(ms, m)
	}
	close(stop)
	counter.Wait()
	for _, m := range ms {
		m.mu.Lock()
		done := m.done
		m.mu.Unlock()
		if !done {
			t.Fatal("a mapping pinned by the residency count was never unmapped")
		}
	}
	liveMu.Lock()
	for m := range live {
		for _, x := range ms {
			if m == x {
				liveMu.Unlock()
				t.Fatal("an unmapped mapping is still registered as live")
			}
		}
	}
	liveMu.Unlock()
}

// BenchmarkResidentBytes measures one uncached count over a 4 GiB sparse mapping, half touched.
func BenchmarkResidentBytes(b *testing.B) {
	const size = 4 << 30
	p := filepath.Join(b.TempDir(), "big")
	f, err := os.Create(p)
	if err != nil {
		b.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		b.Fatal(err)
	}
	f.Close()
	m, err := Open(p)
	if err != nil {
		b.Skip(err)
	}
	defer m.Close()
	r, _ := m.Acquire()
	data := r.Bytes()
	touch(data, len(data)/2)
	r.Release()
	b.ResetTimer()
	for b.Loop() {
		_ = residentNow()
	}
	b.ReportMetric(float64(residentNow())/(1<<20), "residentMiB")
}
