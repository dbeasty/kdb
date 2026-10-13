package replication

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
)

func pendingCommit(t *testing.T, d *dag.InMemoryCommitDag, i int, parents ...codec.Hash) codec.Hash {
	t.Helper()
	txID, _ := codec.RandomUUID()
	author, _ := codec.RandomUUID()
	doc, _ := codec.RandomUUID()
	c, err := document.BuildCommit(parents, d.NamespaceID, txID, codec.TimestampNow(), author,
		[]document.Op{document.WriteOp{DocID: doc, Patch: fmt.Sprintf(`{"i":%d}`, i)}},
		document.EmptyDocumentTree().TreeHash, nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	if err := d.PutCommit(c, true); err != nil {
		t.Fatal(err)
	}
	return c.Hash
}

func hexes(hs ...codec.Hash) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.Hex()
	}
	return out
}

// TestPendingTipsAreAFrontier: tips of failed syncs accumulate as a frontier - a tip another tip
// builds on is dropped, as is one main already reaches and one this node does not hold - and the
// list is capped however many failed syncs feed it.
func TestPendingTipsAreAFrontier(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("app/pending")
	if err != nil {
		t.Fatal(err)
	}
	genesis, _ := d.Head()
	// main: genesis - m1 - m2 (local main at m2)
	m1 := pendingCommit(t, d, 1, genesis)
	m2 := pendingCommit(t, d, 2, m1)
	if err := d.SetHead("main", m2); err != nil {
		t.Fatal(err)
	}
	// Received from a peer, not adopted: a line p1..p30 off m1, and a side branch s1 off p5.
	var line []codec.Hash
	parent := m1
	for i := 0; i < 30; i++ {
		parent = pendingCommit(t, d, 100+i, parent)
		line = append(line, parent)
	}
	s1 := pendingCommit(t, d, 200, line[4])
	unheld := codec.Hash{Bytes: [32]byte{0xab}}

	// First failed sync: pages ended at p10 and s1; the previous attempt left p3 and m1 pending.
	got := pendingFrontier(d, hexes(line[2], m1), []codec.Hash{line[9], s1, unheld})
	if want := hexes(line[9], s1); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("frontier %v, want p10 and s1 (p3 below p10, m1 below main, the unheld tip dropped)", got)
	}
	// Second failed sync got further: p30 retires p10.
	got = pendingFrontier(d, got, []codec.Hash{line[29]})
	if want := hexes(s1, line[29]); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("frontier %v, want s1 and p30", got)
	}

	// Many unrelated tips: capped, newest kept.
	var many []codec.Hash
	for i := 0; i < 3*peersync.MaxPendingTips; i++ {
		many = append(many, pendingCommit(t, d, 1000+i, genesis))
	}
	got = pendingFrontier(d, got, many)
	if len(got) != peersync.MaxPendingTips || got[len(got)-1] != many[len(many)-1].Hex() {
		t.Fatalf("%d pending tips, want the newest %d", len(got), peersync.MaxPendingTips)
	}
	// Without the namespace open, only duplicates and the cap apply.
	if got := pendingFrontier(nil, hexes(m1, m1), []codec.Hash{m1, m2}); len(got) != 2 {
		t.Fatalf("no DAG: %v", got)
	}
}
