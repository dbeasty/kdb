package peersync

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// randomHistory builds n commits on d's root, each on one or two random earlier commits, with
// writes of random size so byte-capped pages cut at varying places. Returns them in creation
// order (parents first).
func randomHistory(t *testing.T, d *dag.InMemoryCommitDag, rng *rand.Rand, n int) []codec.Hash {
	t.Helper()
	root, _ := d.Head()
	all := []codec.Hash{root}
	for i := 0; i < n; i++ {
		parents := []codec.Hash{all[len(all)-1-rng.Intn(min(len(all), 6))]}
		if rng.Intn(4) == 0 && len(all) > 2 {
			if other := all[rng.Intn(len(all))]; other != parents[0] {
				parents = append(parents, other)
			}
		}
		txID, _ := codec.RandomUUID()
		author, _ := codec.RandomUUID()
		docID, _ := codec.RandomUUID()
		ops := []document.Op{document.WriteOp{DocID: docID, Patch: fmt.Sprintf(`{"i":%d,"pad":%q}`, i, strings.Repeat("x", rng.Intn(400)))}}
		c, err := document.BuildCommit(parents, d.NamespaceID, txID, codec.TimestampNow(), author,
			ops, document.EmptyDocumentTree().TreeHash, nil, "random")
		if err != nil {
			t.Fatal(err)
		}
		if err := d.PutCommit(c, true); err != nil {
			t.Fatal(err)
		}
		all = append(all, c.Hash)
	}
	return all
}

func hashesOf(cs []document.Commit) []codec.Hash {
	out := make([]codec.Hash, len(cs))
	for i, c := range cs {
		out[i] = c.Hash
	}
	return out
}

// TestMissingCursorServesTheSamePagesAsRecomputing: paging with one cursor serves exactly the pages
// that working the missing set out afresh for every page - the haves grown by each page's tips, as
// a fetching peer sends them - served before. Random histories with merges, random page sizes.
func TestMissingCursorServesTheSamePagesAsRecomputing(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		d, err := dag.NewInMemoryCommitDag("app/cursor")
		if err != nil {
			t.Fatal(err)
		}
		all := randomHistory(t, d, rng, 80+rng.Intn(120))
		wants := []codec.Hash{all[len(all)-1]}
		if rng.Intn(2) == 0 {
			wants = append(wants, all[len(all)/2+rng.Intn(len(all)/2)])
		}
		var haves []codec.Hash
		for i := 0; i < rng.Intn(4); i++ {
			haves = append(haves, all[rng.Intn(len(all)/2)])
		}
		maxBytes := 300 + rng.Intn(6000)

		cur := newMissingCursor(d, wants, haves)
		grown := append([]codec.Hash(nil), haves...)
		for page := 0; ; page++ {
			want, wantStubs, wantDone, err := newMissingCursor(d, wants, grown).page(d, maxBytes)
			if err != nil {
				t.Fatal(err)
			}
			got, gotStubs, gotDone, err := cur.page(d, maxBytes)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(hashesOf(got)) != fmt.Sprint(hashesOf(want)) || gotDone != wantDone || len(gotStubs) != len(wantStubs) {
				t.Fatalf("seed %d page %d: cursor served %d commits (done=%v), recomputing serves %d (done=%v)",
					seed, page, len(got), gotDone, len(want), wantDone)
			}
			for i := range got {
				if len(got[i].Operations) != len(want[i].Operations) {
					t.Fatalf("seed %d page %d: commit %d served without its operations", seed, page, i)
				}
			}
			if gotDone {
				break
			}
			// Asked again before advancing - a retry - the cursor serves the same page.
			again, _, _, err := cur.page(d, maxBytes)
			if err != nil || fmt.Sprint(hashesOf(again)) != fmt.Sprint(hashesOf(got)) {
				t.Fatalf("seed %d page %d: a repeated request got a different page", seed, page)
			}
			tips := pageTips(got)
			grown = append(grown, tips...)
			if !cur.continues(wants, grown) {
				t.Fatalf("seed %d page %d: the next request was not recognised as continuing", seed, page)
			}
			cur.advance()
		}
	}
}

// TestMissingCursorContinuesOnlyThePeersTransfer: a request continues the cursor only when it
// names the same wants and holds the last page's tips; anything else is worked out afresh.
func TestMissingCursorContinuesOnlyThePeersTransfer(t *testing.T) {
	d, err := dag.NewInMemoryCommitDag("app/cursor-continues")
	if err != nil {
		t.Fatal(err)
	}
	all := randomHistory(t, d, rand.New(rand.NewSource(7)), 60)
	want := []codec.Hash{all[len(all)-1]}
	cur := newMissingCursor(d, want, nil)
	page, _, done, err := cur.page(d, 500)
	if err != nil || done {
		t.Fatalf("expected a first page of several, got done=%v err=%v", done, err)
	}
	tips := pageTips(page)
	if cur.continues(want, nil) {
		t.Error("a request not holding the last page continued the transfer")
	}
	if cur.continues([]codec.Hash{all[len(all)-2]}, tips) {
		t.Error("a request for other wants continued the transfer")
	}
	if !cur.continues(want, tips) {
		t.Error("the next request did not continue the transfer")
	}
}

// TestV2HostFetchPagesFromOneWalk: a peer fetching a long history page by page is served from the
// cursor its first request worked out - the pages are the same, and a request repeated after a
// lost reply gets the same page again.
func TestV2HostFetchPagesFromOneWalk(t *testing.T) {
	ns := "app/v2-host-cursor"
	remote := newTestNamespaces(t, ns)
	r := remote.side(ns)
	g, _ := r.dag.Head()
	tip := chain(t, r, ns, g, 200, "r")
	host := newHost(remote)
	if ack, ok := hostCall(t, host, wire.SyncHelloMessage{
		H: header(wire.MsgSyncHello, 1), NodeID: "peer", Protocol: wire.SyncProtocolVersion, Namespaces: []string{ns},
	}).(wire.SyncHelloAckMessage); !ok || !ack.Accepted {
		t.Fatalf("hello: %+v", ack)
	}
	fetch := func(haves []codec.Hash) wire.PackPageMessage {
		t.Helper()
		msg := hostCall(t, host, wire.FetchRequestMessage{H: header(wire.MsgFetchRequest, 99), Namespace: ns,
			Wants: []codec.Hash{tip.Hash}, Haves: haves, MaxBytes: 3_000})
		p, ok := msg.(wire.PackPageMessage)
		if !ok {
			t.Fatalf("expected PACK_PAGE, got %T", msg)
		}
		return p
	}
	haves := []codec.Hash{g}
	got := 0
	for pages := 0; ; pages++ {
		p := fetch(haves)
		if pages == 1 {
			// The reply was "lost": the same request again gets the same page.
			if again := fetch(haves); fmt.Sprint(hashesOf(again.Commits)) != fmt.Sprint(hashesOf(p.Commits)) {
				t.Fatal("a repeated request got a different page")
			}
		}
		if pages > 0 && host.fetch == nil && !p.Done {
			t.Fatal("the host dropped the transfer's cursor mid-way")
		}
		got += len(p.Commits)
		if p.Done {
			break
		}
		haves = append(haves, pageTips(p.Commits)...)
	}
	if got != 200 {
		t.Fatalf("served %d commits for a 200-commit history", got)
	}
	if host.fetch != nil {
		t.Fatal("the host kept the cursor of a finished transfer")
	}
}
