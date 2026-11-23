package engine

import (
	"fmt"
	"testing"
)

// A superseded version is the next one evicted, ahead of live versions written or read before
// it, and a read it got before being superseded earns it no second chance.
func TestDemotedVersionIsEvictedFirst(t *testing.T) {
	s := newShardedDocByHashStore(0)
	sh := s.shards[0]
	const size = 100
	per := int64(size) + docHashEntryOverhead
	sh.budget = 4 * per
	for i := 0; i < 4; i++ {
		s.Put(shardHash(i), bodyFor(i, size))
	}
	s.Get(shardHash(3)) // referenced: would otherwise get a second chance
	s.Demote(shardHash(3))
	if got := lruOrder(sh); got[len(got)-1] != shardHash(3) {
		t.Fatalf("demoted version is not at the tail: %v", got)
	}
	if i, _ := sh.index.find(sh.slots, ptrHash(shardHash(3))); sh.slots[i].off&slotReferenced != 0 {
		t.Fatal("demoting kept the version's second chance")
	}
	s.Put(shardHash(4), bodyFor(4, size)) // over budget by one
	if _, ok := s.Get(shardHash(3)); ok {
		t.Fatal("the superseded version survived; a live one was evicted instead")
	}
	for _, k := range []int{0, 1, 2, 4} {
		if _, ok := s.Get(shardHash(k)); !ok {
			t.Fatalf("live version %d evicted ahead of the superseded one", k)
		}
	}
	// Demoting the tail, the head, the only entry, or something not resident is harmless.
	s.Demote(shardHash(99))
	s.Demote(lruOrder(sh)[0])
	s.Demote(lruOrder(sh)[len(lruOrder(sh))-1])
	checkShard(t, sh)

	one := newShardedDocByHashStore(0)
	one.Put(shardHash(1), bodyFor(1, size))
	one.Demote(shardHash(1))
	checkShard(t, one.shards[0])
}

// A version superseded while still pinned (its commit not yet durable) joins the ring at the tail
// when the last pin goes - unless it became live again in between.
func TestDemotedWhilePinnedGoesToTheTailOnUnpin(t *testing.T) {
	s := newShardedDocByHashStore(0)
	sh := s.shards[0]
	const size = 100
	sh.budget = 10 * (int64(size) + docHashEntryOverhead)
	for i := 0; i < 3; i++ {
		s.Put(shardHash(i), bodyFor(i, size))
	}
	s.Pin(shardHash(5))
	s.Put(shardHash(5), bodyFor(5, size)) // pinned: off the ring
	s.Demote(shardHash(5))
	if got := lruOrder(sh); len(got) != 3 {
		t.Fatalf("pinned version on the ring: %v", got)
	}
	s.Unpin(shardHash(5))
	if got := lruOrder(sh); got[len(got)-1] != shardHash(5) {
		t.Fatalf("version superseded while pinned did not join at the tail: %v", got)
	}
	if len(sh.demoted) != 0 {
		t.Fatalf("demoted set kept %d entries", len(sh.demoted))
	}

	// Superseded, then written again (pinned again as live) before its pin was released: it
	// re-enters at the front like any live version.
	s.Pin(shardHash(6))
	s.Put(shardHash(6), bodyFor(6, size))
	s.Demote(shardHash(6))
	s.Pin(shardHash(6)) // the document changed back to this content
	s.Unpin(shardHash(6))
	s.Unpin(shardHash(6))
	if got := lruOrder(sh); got[0] != shardHash(6) {
		t.Fatalf("a version live again did not re-enter at the front: %v", fmt.Sprint(got))
	}
	checkShard(t, sh)
}
