package peersync

import "testing"

func TestConflictQueueSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	q, err := NewConflictQueue(dir)
	if err != nil {
		t.Fatal(err)
	}
	id := ConflictID(ConflictDivergence, "a/b", "branch:main", "peer-1")
	if _, err := q.Record(ConflictEntry{ID: id, Kind: ConflictDivergence, Namespace: "a/b", Ref: "branch:main"}); err != nil {
		t.Fatal(err)
	}
	again, err := NewConflictQueue(dir)
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := again.Get(id); !ok || e.Seen != 1 || e.FirstSeen.IsZero() {
		t.Fatalf("reloaded entry: %+v %v", e, ok)
	}
	if err := again.Remove(id); err != nil {
		t.Fatal(err)
	}
	third, _ := NewConflictQueue(dir)
	if third.Len() != 0 {
		t.Fatal("a removed entry came back")
	}
}
