package dag

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// appendN appends n commits in a line and returns their hashes, oldest
// first. Timestamps step forward by one millisecond each so that Walk's
// timestamp ordering and CommitAtOrBefore have something to order by -
// TimestampNow inside one test is otherwise the same instant for all of
// them.
func appendN(t *testing.T, d *InMemoryCommitDag, n int) []codec.Hash {
	t.Helper()
	head, err := d.Head()
	if err != nil {
		t.Fatal(err)
	}
	out := make([]codec.Hash, 0, n)
	base := codec.TimestampNow()
	for i := 0; i < n; i++ {
		txID, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		tx := document.Transaction{
			ID: txID, BaseVersion: head,
			Timestamp:    codec.Timestamp{EpochMillis: base.EpochMillis + int64(i) + 1},
			AuthorNodeID: author,
		}
		c, err := d.AppendCommit(tx, head, document.EmptyDocumentTree(), nil, commitMessage(i))
		if err != nil {
			t.Fatal(err)
		}
		head = c.Hash
		out = append(out, c.Hash)
	}
	return out
}

func commitMessage(i int) string {
	return string(rune('a'+i%26)) + "-commit"
}

func TestNthAncestorWalksFirstParents(t *testing.T) {
	d, err := NewInMemoryCommitDag("ns")
	if err != nil {
		t.Fatal(err)
	}
	hashes := appendN(t, d, 5)
	head := hashes[len(hashes)-1]

	if got, err := d.NthAncestor(head, 0); err != nil || got != head {
		t.Fatalf("~0 should be the commit itself: %v %v", got, err)
	}
	for back := 1; back <= 5; back++ {
		got, err := d.NthAncestor(head, back)
		if err != nil {
			t.Fatalf("~%d: %v", back, err)
		}
		if back == 5 {
			// Five back from the fifth commit is genesis, which is not in
			// hashes and so has nothing here to compare against.
			continue
		}
		if want := hashes[len(hashes)-1-back]; got != want {
			t.Fatalf("~%d resolved to the wrong commit", back)
		}
	}
}

func TestNthAncestorPastTheRootIsAnError(t *testing.T) {
	d, _ := NewInMemoryCommitDag("ns")
	hashes := appendN(t, d, 2)
	// Two commits plus genesis: three deep, so four steps back is past
	// the root and must say so rather than returning genesis.
	_, err := d.NthAncestor(hashes[1], 4)
	var notFound *RevisionNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("want RevisionNotFoundError, got %v", err)
	}
}

func TestParseRevision(t *testing.T) {
	cases := []struct {
		spec string
		ref  CommitRef
		back int
	}{
		{"head", RefByBranch{Name: "main"}, 0},
		{"HEAD", RefByBranch{Name: "main"}, 0},
		{"head~10", RefByBranch{Name: "main"}, 10},
		{"head^", RefByBranch{Name: "main"}, 1},
		{"head^^", RefByBranch{Name: "main"}, 2},
		{"head~", RefByBranch{Name: "main"}, 1},
		{"tag:v1", RefByTag{Name: "v1"}, 0},
		{"tag:v1~2", RefByTag{Name: "v1"}, 2},
		{"branch:dev~3", RefByBranch{Name: "dev"}, 3},
		{"abc123", RefByHash{Hex: "abc123"}, 0},
		{"abc123~4", RefByHash{Hex: "abc123"}, 4},
	}
	for _, c := range cases {
		ref, back, err := ParseRevision(c.spec)
		if err != nil {
			t.Fatalf("%s: %v", c.spec, err)
		}
		if ref != c.ref || back != c.back {
			t.Fatalf("%s resolved to %#v ~%d, want %#v ~%d", c.spec, ref, back, c.ref, c.back)
		}
	}
	for _, bad := range []string{"", "   ", "head~x", "head~-1", "~2"} {
		if _, _, err := ParseRevision(bad); err == nil {
			t.Fatalf("%q should not parse", bad)
		}
	}
}

func TestResolveRevisionRelative(t *testing.T) {
	d, _ := NewInMemoryCommitDag("ns")
	hashes := appendN(t, d, 12)
	got, err := d.ResolveRevision("head~10")
	if err != nil {
		t.Fatal(err)
	}
	if got != hashes[len(hashes)-1-10] {
		t.Fatal("head~10 resolved to the wrong commit")
	}
	byHash, err := d.ResolveRevision(hashes[5].Hex() + "~2")
	if err != nil {
		t.Fatal(err)
	}
	if byHash != hashes[3] {
		t.Fatal("<hash>~2 resolved to the wrong commit")
	}
}

// The bug this replaces: AtTag and AtTime used to resolve to head, so a
// query against a name that did not exist silently returned current data.
func TestResolveRefNeverFallsBackToHead(t *testing.T) {
	d, _ := NewInMemoryCommitDag("ns")
	appendN(t, d, 3)
	head, _ := d.Head()

	if h, err := d.ResolveRef(RefByTag{Name: "nope"}); err == nil {
		t.Fatalf("unknown tag resolved to %s instead of failing", h.Hex())
	} else if h == head {
		t.Fatal("unknown tag resolved to head")
	}
	if h, err := d.ResolveRef(RefByBranch{Name: "nope"}); err == nil {
		t.Fatalf("unknown branch resolved to %s instead of failing", h.Hex())
	}
	unknown := codec.Hash{}
	if _, err := d.ResolveRef(RefByHash{Hex: unknown.Hex()}); err == nil {
		t.Fatal("a commit that is not retained resolved instead of failing")
	}
	if _, err := d.ResolveRef(RefByHash{Hex: "not-a-hash"}); err == nil {
		t.Fatal("a malformed hash resolved instead of failing")
	}
}

func TestResolveRefByTagAndTime(t *testing.T) {
	d, _ := NewInMemoryCommitDag("ns")
	hashes := appendN(t, d, 5)
	if _, err := d.CreateTag("v1", hashes[1], "first release"); err != nil {
		t.Fatal(err)
	}
	got, err := d.ResolveRef(RefByTag{Name: "v1"})
	if err != nil || got != hashes[1] {
		t.Fatalf("tag resolved to %v (%v)", got, err)
	}

	third, _ := d.GetCommit(hashes[2])
	at, err := d.ResolveRef(RefByTime{Timestamp: third.Timestamp})
	if err != nil {
		t.Fatal(err)
	}
	if at != hashes[2] {
		t.Fatal("AT TIME should resolve to the newest commit at or before the timestamp")
	}
}

func TestCreateTagRefusesAnUnknownCommit(t *testing.T) {
	d, _ := NewInMemoryCommitDag("ns")
	if _, err := d.CreateTag("v1", codec.Hash{}, ""); err == nil {
		t.Fatal("a tag on a commit that is not retained should be refused")
	}
	if _, ok := d.GetTag("v1"); ok {
		t.Fatal("the refused tag was recorded anyway")
	}
}

func TestListCommitsDescribesWithoutLoadingOperations(t *testing.T) {
	d, _ := NewInMemoryCommitDag("ns")
	hashes := appendN(t, d, 6)
	head := hashes[len(hashes)-1]

	all, err := d.ListCommits(head, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("want 3 entries, got %d", len(all))
	}
	if all[0].Hash != head {
		t.Fatal("listing should start at the commit asked for, newest first")
	}
	if all[0].Message == "" {
		t.Fatal("listing should carry the commit message")
	}
	if len(all[0].ParentHashes) == 0 {
		t.Fatal("listing should carry parents")
	}

	page2, err := d.ListCommits(head, 3, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 3 {
		t.Fatalf("second page should have 3 entries, got %d", len(page2))
	}
	if page2[0].Hash == all[0].Hash {
		t.Fatal("skip did not advance the page")
	}
	if got, _ := d.ListCommits(head, 0, 0); got != nil {
		t.Fatal("a zero limit should return nothing")
	}
}
