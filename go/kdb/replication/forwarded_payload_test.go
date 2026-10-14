package replication

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// A host serves a logged commit as the payload its log holds (C4): for every commit the log
// holds, LoggedPayload returns bytes that hash to the commit and are exactly its canonical
// encoding - after a reopen too, when the operations come from nowhere but the log - and a peer
// that pulls the history through them ends with the same head.
func TestLoggedPayloadsAreTheCommitsEncoding(t *testing.T) {
	ns := "app/data"
	dir := t.TempDir()
	a, closeA := fileNodeAt(t, dir, ns)
	id, _ := codec.RandomUUID()
	var hashes []codec.Hash
	for i := 0; i < 200; i++ {
		c, err := a.Upsert(ns, id, fmt.Sprintf(`{"i":%d}`, i), auth.Principal{})
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, c.Hash)
	}
	closeA()
	a, closeA = fileNodeAt(t, dir, ns)
	defer closeA()
	d := a.PeerIngestEnv().DAG
	served := 0
	for _, h := range hashes {
		p, ok, err := d.LoggedPayload(h)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			continue
		}
		served++
		if sha256.Sum256(p) != h.Bytes {
			t.Fatalf("payload for %s does not hash to it", h.Hex()[:12])
		}
		c, err := d.GetCommitOrThrow(h)
		if err != nil {
			t.Fatal(err)
		}
		want, err := c.ToPayloadBytes()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(p, want) {
			t.Fatalf("payload for %s differs from the commit's encoding", h.Hex()[:12])
		}
	}
	if served < len(hashes)-1 {
		t.Fatalf("only %d of %d logged commits came back as payloads", served, len(hashes))
	}
	b := fileNode(t, ns)
	if _, err := syncWith(t, b, listen(t, a, ns), ns, peersync.SyncPull, newCounting()); err != nil {
		t.Fatal(err)
	}
	if head(t, a) != head(t, b) {
		t.Fatal("a pull served from logged payloads did not converge")
	}
}
