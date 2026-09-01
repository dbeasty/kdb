package dag

import (
	"errors"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// mid appends two commits and returns the middle one - a commit that is neither genesis nor a
// branch head, i.e. exactly the kind Squash is allowed to reclaim and a reader may still be
// holding.
func mid(t *testing.T, d *InMemoryCommitDag) (middle codec.Hash, head codec.Hash) {
	t.Helper()
	genesis, _ := d.Head()
	c1, err := d.AppendCommit(newTx(genesis), genesis, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	c2, err := d.AppendCommit(newTx(c1.Hash), c1.Hash, document.EmptyDocumentTree(), nil, "")
	if err != nil {
		t.Fatal(err)
	}
	return c1.Hash, c2.Hash
}

func TestSquashRefusesPinnedCommit(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	middle, _ := mid(t, d)

	// A pinned commit is exactly the case that used to be reclaimed out from under a SNAPSHOT
	// reader: it is not a branch head, so the only check Squash had did not cover it.
	release := d.Pin(middle)
	_, err = d.Squash([]codec.Hash{middle}, middle, document.EmptyDocumentTree(), nil, "squash")
	var safety *CompactionSafetyError
	if !errors.As(err, &safety) {
		t.Fatalf("expected CompactionSafetyError for a pinned commit, got %v", err)
	}
	if _, ok := d.GetCommit(middle); !ok {
		t.Fatal("pinned commit was removed despite the refusal")
	}

	// Releasing the pin makes it reclaimable again - the pin is a hold, not a permanent veto.
	release()
	if d.PinnedCount() != 0 {
		t.Fatalf("expected no pins after release, got %d", d.PinnedCount())
	}
	if _, err := d.Squash([]codec.Hash{middle}, middle, document.EmptyDocumentTree(), nil, "squash"); err != nil {
		t.Fatalf("squash after release: %v", err)
	}
}

func TestStubCommitRefusesPinnedCommit(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	middle, _ := mid(t, d)
	release := d.Pin(middle)
	defer release()
	_, err = d.StubCommit(middle, "ice://bucket/obj")
	var safety *CompactionSafetyError
	if !errors.As(err, &safety) {
		t.Fatalf("expected CompactionSafetyError archiving a pinned commit, got %v", err)
	}
	if _, ok := d.GetCommit(middle); !ok {
		t.Fatal("pinned commit was archived despite the refusal")
	}
}

// Two readers pinning the same commit is the normal case, not the exotic one: every SNAPSHOT
// session that opened in the same transaction window pins the same head. One of them finishing
// must not expose the commit to reclamation while the other is still reading.
func TestPinsAreCounted(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	middle, _ := mid(t, d)
	first := d.Pin(middle)
	second := d.Pin(middle)

	first()
	if !d.IsPinned(middle) {
		t.Fatal("commit unpinned while a second reader still holds it")
	}
	if _, err := d.Squash([]codec.Hash{middle}, middle, document.EmptyDocumentTree(), nil, "squash"); err == nil {
		t.Fatal("squash succeeded while a second reader still held the commit")
	}

	second()
	if d.IsPinned(middle) {
		t.Fatal("commit still pinned after every reader released")
	}
}

// Release is idempotent so callers can both defer it and call it explicitly on an early path -
// double-releasing must not decrement another reader's count.
func TestPinReleaseIsIdempotent(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	middle, _ := mid(t, d)
	mine := d.Pin(middle)
	other := d.Pin(middle)
	defer other()

	mine()
	mine()
	mine()
	if !d.IsPinned(middle) {
		t.Fatal("repeated release dropped another reader's pin")
	}
}

func TestPinConcurrentAcquireRelease(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	middle, _ := mid(t, d)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				d.Pin(middle)()
			}
		}()
	}
	wg.Wait()
	if d.PinnedCount() != 0 {
		t.Fatalf("expected pins to drain to 0, got %d", d.PinnedCount())
	}
}
