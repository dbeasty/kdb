package server

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// chain pushes premerges a, b, c... of one document, each built on the one before.
func chain(f *inFlightPremerges, id codec.UUID, n int) []*inFlightPremerge {
	var out []*inFlightPremerge
	var prev *inFlightPremerge
	for i := 0; i < n; i++ {
		p := f.push(id, &premergedUpsert{}, prev)
		out = append(out, p)
		prev = p
	}
	return out
}

func TestInFlightPremergesBuildOnTheNewest(t *testing.T) {
	var f inFlightPremerges
	id := codec.UUID{MSB: 1, LSB: 2}
	if got := f.newest(id); got != nil {
		t.Fatalf("empty: newest = %p, want none", got)
	}
	w := chain(&f, id, 3)
	if got := f.newest(id); got != w[2] {
		t.Fatalf("newest = %p, want the last pushed", got)
	}
	// The ones ahead commit as built: c is still the one to build on.
	f.done(id, w[0], true)
	f.done(id, w[1], true)
	if got := f.newest(id); got != w[2] {
		t.Errorf("after a and b commit: newest = %p, want c", got)
	}
	// Once the newest commits there is nothing in flight; head is the version.
	f.done(id, w[2], true)
	if got := f.newest(id); got != nil {
		t.Errorf("all committed: newest = %p, want none", got)
	}
	if n := len(f.shard(id).m); n != 0 {
		t.Errorf("%d entries left", n)
	}
}

// A write built on one that went stale or failed will not be current either: a new write builds
// on what is left - here, head.
func TestInFlightPremergesSkipWritesBuiltOnAStaleOne(t *testing.T) {
	var f inFlightPremerges
	id := codec.UUID{MSB: 1, LSB: 2}
	w := chain(&f, id, 3) // a <- b <- c
	f.done(id, w[0], false)
	if got := f.newest(id); got != nil {
		t.Fatalf("a failed: newest = %p, want none (b and c are built on a)", got)
	}
	// b's retry builds on head and is pushed; c, failing in turn, must not hide it.
	b2 := f.push(id, &premergedUpsert{}, f.newest(id))
	f.done(id, w[1], false)
	f.done(id, w[2], false)
	if got := f.newest(id); got != b2 {
		t.Errorf("newest = %p, want b's retry", got)
	}
}

// A write that overtook the one it was built on goes stale; the next write builds on the
// overtaken one again - it is still queued and still current-to-be.
func TestInFlightPremergesFallBackToTheWriteOvertaken(t *testing.T) {
	var f inFlightPremerges
	id := codec.UUID{MSB: 1, LSB: 2}
	w := chain(&f, id, 2) // a <- b, and b reaches the gate first
	f.done(id, w[1], false)
	if got := f.newest(id); got != w[0] {
		t.Errorf("newest = %p, want a", got)
	}
	// Behind a newer write that was built on b, the same.
	f2 := inFlightPremerges{}
	w = chain(&f2, id, 3) // a <- b <- c, b stale
	f2.done(id, w[1], false)
	if got := f2.newest(id); got != w[0] {
		t.Errorf("behind c: newest = %p, want a", got)
	}
}

// A stale write built on one that has since committed: the next write builds on head, not on the
// committed one's body - other writes may have committed after it, which is why this one is stale.
func TestInFlightPremergesNeverBuildOnACommittedWrite(t *testing.T) {
	var f inFlightPremerges
	id := codec.UUID{MSB: 1, LSB: 2}
	w := chain(&f, id, 2) // a <- b
	f.done(id, w[0], true)
	f.done(id, w[1], false)
	if got := f.newest(id); got != nil {
		t.Errorf("newest = %p, want none (head)", got)
	}
	// The same behind a newer write built on the stale one.
	var f2 inFlightPremerges
	w = chain(&f2, id, 3) // a <- b <- c
	f2.done(id, w[0], true)
	f2.done(id, w[1], false)
	if got := f2.newest(id); got != nil {
		t.Errorf("behind c: newest = %p, want none (head)", got)
	}
}
