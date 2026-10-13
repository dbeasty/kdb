package peersync

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

func fakeCommit(b byte, parents ...codec.Hash) document.Commit {
	return document.Commit{Hash: codec.Hash{Bytes: [32]byte{b}}, ParentHashes: parents}
}

// TestFoldFrontierKeepsOnlyTips: earlier pages' tips retire as later pages build on them; a side
// branch nothing builds on stays; duplicates collapse; the cap drops the oldest.
func TestFoldFrontierKeepsOnlyTips(t *testing.T) {
	a := fakeCommit(1)
	b := fakeCommit(2, a.Hash)
	side := fakeCommit(3, a.Hash)
	c := fakeCommit(4, b.Hash)
	d := fakeCommit(5, c.Hash)

	f := foldFrontier(nil, []document.Commit{a, b, side}, 10)
	if len(f) != 2 || f[0] != b.Hash || f[1] != side.Hash {
		t.Fatalf("page 1 frontier %v, want b and side", f)
	}
	f = foldFrontier(f, []document.Commit{c, d}, 10)
	if len(f) != 2 || f[0] != side.Hash || f[1] != d.Hash {
		t.Fatalf("page 2 frontier %v, want side and d (b retired by c)", f)
	}
	// The same page again changes nothing.
	f = foldFrontier(f, []document.Commit{d}, 10)
	if len(f) != 2 {
		t.Fatalf("a repeated page grew the frontier: %v", f)
	}
	// Cap: many unrelated tips keep the newest.
	var page []document.Commit
	for i := 0; i < 20; i++ {
		page = append(page, fakeCommit(byte(100+i)))
	}
	f = foldFrontier(f, page, 8)
	if len(f) != 8 || f[7] != page[19].Hash || f[0] != page[12].Hash {
		t.Fatalf("capped frontier %v", f)
	}
}

// TestPullHavesStayBoundedAndTheCursorContinues: a pull of a long history in many pages sends the
// same few haves on every request - the ones it started with and the last page's tips - rather
// than one more page's tips each time, and the host serves every page after the first from its
// cursor (missingCursor.continues) rather than working the missing set out again. ReceivedTips is
// the frontier of what arrived, not every page's tips.
func TestPullHavesStayBoundedAndTheCursorContinues(t *testing.T) {
	ns := "app/haves-bounded"
	remote := newTestNamespaces(t, ns)
	local := newTestNamespaces(t, ns)
	r := remote.side(ns)
	genesis, _ := r.dag.Head()
	chain(t, r, ns, genesis, 200, "long")
	host := newHost(remote)
	w := wire.NewCodec(wire.EncodingJSON)
	requests, continued, maxHaves := 0, 0, 0
	v2HubRaw(t, "hub-haves-bounded", func(frame []byte) []byte {
		msg, _ := w.Decode(frame)
		req, isFetch := msg.(wire.FetchRequestMessage)
		var before *missingCursor
		if isFetch {
			requests++
			maxHaves = max(maxHaves, len(req.Haves))
			host.fetchMu.Lock()
			if host.fetch != nil {
				before = host.fetch.cursor
			}
			host.fetchMu.Unlock()
		}
		out, _ := host.HandleFrame(frame)
		if isFetch && before != nil {
			host.fetchMu.Lock()
			if host.fetch == nil || host.fetch.cursor == before {
				continued++ // served from the same cursor (nil: it was the last page)
			}
			host.fetchMu.Unlock()
		}
		return out
	})
	cfg := V2ClientConfig{PeerURI: "memory://hub-haves-bounded", Local: local, NodeID: "c", Namespaces: []string{ns}, Mode: SyncPull, PageBytes: 2_000, AdoptEveryBytes: -1}
	res, err := SyncV2(w, stream.NewInMemoryTransport(), cfg)
	if err != nil || res.Namespaces[0].Err != nil {
		t.Fatal(err, res.Namespaces[0].Err)
	}
	if mustHead(t, local.side(ns)) != mustHead(t, r) {
		t.Fatal("the pull did not reach the remote head")
	}
	if requests < 20 {
		t.Fatalf("only %d FETCH requests: pages too large for the test", requests)
	}
	// Spread haves of an empty node (genesis) plus one tip per page of a linear history.
	if maxHaves > 4 {
		t.Fatalf("a request carried %d haves: they grow with the transfer", maxHaves)
	}
	if continued != requests-1 {
		t.Fatalf("%d of %d requests continued the host's cursor, want all but the first", continued, requests)
	}
	if tips := res.Namespaces[0].ReceivedTips; len(tips) != 1 || tips[0] != mustHead(t, r) {
		t.Fatalf("ReceivedTips %d tips, want the frontier: the remote head alone", len(tips))
	}
}
