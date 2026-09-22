package server

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func (f *projectionFixture) readThrough(capacity int) *ReadThrough {
	f.t.Helper()
	opens := 0
	rt := &ReadThrough{Capacity: capacity, Open: func() (*peersync.RepairSession, string, error) {
		opens++
		s, err := peersync.OpenDocSession(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: "edge", PeerURI: f.addr,
		})
		return s, "app/data", err
	}}
	f.replica.ReadThrough = rt
	f.t.Cleanup(rt.Close)
	return rt
}

func (f *projectionFixture) read(id codec.UUID) (string, bool, error) {
	body, _, found, err := f.replica.ReadDocument(f.replica.Runtime.DefaultNamespace, id)
	return body, found, err
}

// A projection with read-through answers reads of documents outside its filter from the source,
// verified, without making them part of the projection.
func TestReadThroughServesDocumentsOutsideTheFilter(t *testing.T) {
	f := newProjectionFixture(t, "region = 'EU'")
	eu, us := mustRandomUUID(t), mustRandomUUID(t)
	f.put(eu, `{"region":"EU","n":1}`)
	f.put(us, `{"region":"US","n":2}`)
	f.sync()
	rt := f.readThrough(0)

	if body, found, err := f.read(eu); err != nil || !found || body != `{"region":"EU","n":1}` {
		t.Fatalf("a held document reads locally: %s %v %v", body, found, err)
	}
	if rt.Stats().Fetches != 0 {
		t.Fatal("a held document must not go to the source")
	}
	body, found, err := f.read(us)
	if err != nil || !found || body != `{"region":"US","n":2}` {
		t.Fatalf("read-through of a document outside the filter: %s %v %v", body, found, err)
	}
	if _, _, found, _ := f.replica.GetDocument(f.replica.Runtime.DefaultNamespace, us); found {
		t.Fatal("a read-through document must not become part of the projection")
	}
	if held := f.held(); len(held) != 1 {
		t.Fatalf("the projection still holds only the filter's documents: %v", held)
	}
	if _, _, err := f.read(us); err != nil {
		t.Fatal(err)
	}
	if s := rt.Stats(); s.Fetches != 1 || s.Hits != 1 || s.Held != 1 {
		t.Fatalf("the second read should come from the hoard: %+v", s)
	}

	// A document the source does not have is proved absent.
	if _, found, err := f.read(mustRandomUUID(t)); err != nil || found {
		t.Fatalf("an unknown document should be proved absent: %v %v", found, err)
	}
}

// Read-through answers as of the source commit the projection is at, so it never shows a document
// newer than the rest of what the edge holds; once the projection moves on, so do its reads.
func TestReadThroughIsConsistentWithTheProjectionsPosition(t *testing.T) {
	f := newProjectionFixture(t, "region = 'EU'")
	eu, us := mustRandomUUID(t), mustRandomUUID(t)
	f.put(eu, `{"region":"EU","v":1}`)
	f.put(us, `{"region":"US","v":1}`)
	f.sync()
	f.readThrough(0)
	if body, _, _ := f.read(us); body != `{"region":"US","v":1}` {
		t.Fatalf("first read: %s", body)
	}

	// The source moves on; the projection has not synced.
	f.put(us, `{"region":"US","v":2}`)
	f.put(eu, `{"region":"EU","v":2}`)
	if body, _, _ := f.read(us); body != `{"region":"US","v":1}` {
		t.Fatalf("before the projection syncs, reads stay at its position: %s", body)
	}
	if body, _, _ := f.read(eu); body != `{"region":"EU","v":1}` {
		t.Fatalf("the held document is at the same position: %s", body)
	}

	f.sync()
	if body, _, _ := f.read(us); body != `{"region":"US","v":2}` {
		t.Fatalf("after the projection syncs, read-through follows it: %s", body)
	}
}

// Read-through asks the source with the edge's principal: a document it may not read is refused.
func TestReadThroughHonoursTheSourcesReadRights(t *testing.T) {
	us := mustRandomUUID(t)
	f := newProjectionFixture(t, "region = 'EU'", hideDocs{hidden: map[string]bool{us.String(): true}})
	f.put(us, `{"region":"US"}`)
	f.put(mustRandomUUID(t), `{"region":"EU"}`)
	f.sync()
	f.readThrough(0)
	if _, _, err := f.read(us); !errors.Is(err, ErrReadThroughForbidden) {
		t.Fatalf("expected the source's refusal, got %v", err)
	}
}

func TestReadThroughHoardIsBounded(t *testing.T) {
	f := newProjectionFixture(t, "region = 'EU'")
	var ids []codec.UUID
	for i := 0; i < 5; i++ {
		id := mustRandomUUID(t)
		ids = append(ids, id)
		f.put(id, `{"region":"US"}`)
	}
	f.sync()
	rt := f.readThrough(2)
	for _, id := range ids {
		if _, found, err := f.read(id); err != nil || !found {
			t.Fatal(err)
		}
	}
	if s := rt.Stats(); s.Held != 2 || s.Fetches != 5 {
		t.Fatalf("the hoard keeps at most its capacity: %+v", s)
	}
	if _, _, err := f.read(ids[0]); err != nil { // evicted, fetched again
		t.Fatal(err)
	}
	if s := rt.Stats(); s.Fetches != 6 {
		t.Fatalf("an evicted document is fetched again: %+v", s)
	}
}

// Before its first sync a projection has no source position to read consistently at.
func TestReadThroughWaitsForTheFirstSync(t *testing.T) {
	f := newProjectionFixture(t, "region = 'EU'")
	us := mustRandomUUID(t)
	f.put(us, `{"region":"US"}`)
	rt := f.readThrough(0)
	if _, found, err := f.read(us); err != nil || found {
		t.Fatalf("no read-through before the first sync: %v %v", found, err)
	}
	if rt.Stats().Fetches != 0 {
		t.Fatal("the source must not be asked")
	}
}
