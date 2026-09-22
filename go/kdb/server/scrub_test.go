package server

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func openFileNode(t *testing.T, dir string) (*embed.EmbeddedKdbRuntime, *KdbServerRuntime) {
	t.Helper()
	rt, err := embed.OpenFileRuntime(dir, "app", "app/data", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	srv := NewKdbServerRuntime(rt)
	srv.NodeID = codec.DerivedUUID("scrub-node:" + dir)
	return rt, srv
}

// syncTo pushes and pulls app/data between from and a listener on to.
func syncTo(t *testing.T, from, to *KdbServerRuntime) {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", to, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: from.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"}, Local: from.PeerNamespaces(),
	})
	if err != nil || res.Namespaces[0].Err != nil {
		t.Fatalf("sync: %v %+v", err, res)
	}
}

// fetcherFrom is a BodyFetcher that asks one peer over OBJECT_FETCH.
func fetcherFrom(t *testing.T, peer *KdbServerRuntime) (BodyFetcher, func()) {
	t.Helper()
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", peer, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	fetch := func(ns string, wanted map[codec.UUID]codec.Hash, treeHex string) (map[codec.UUID]string, error) {
		s, err := peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: "scrubber", PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
		})
		if err != nil {
			return nil, err
		}
		defer s.Close()
		return s.Fetch(ns, wanted, treeHex)
	}
	return fetch, func() { ln.Close() }
}

// corruptOnDisk flips one byte inside needle in every delta segment that holds it, and reports
// how many it changed.
func corruptOnDisk(t *testing.T, dir, needle string) int {
	t.Helper()
	changed := 0
	err := filepath.Walk(filepath.Join(dir, "ns"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !bytes.Contains([]byte(filepath.ToSlash(path)), []byte("/delta/")) {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		i := bytes.Index(raw, []byte(needle))
		if i < 0 {
			return nil
		}
		raw[i] ^= 0x01
		changed++
		return os.WriteFile(path, raw, info.Mode())
	})
	if err != nil {
		t.Fatal(err)
	}
	return changed
}

func readable(srv *KdbServerRuntime, id codec.UUID, body string) bool {
	got, _, found, err := srv.GetDocument("app/data", id)
	return err == nil && found && got == body
}

// TestScrubRepairsABodyCorruptedOnDiskFromAPeer: a document body damaged in the delta log is
// unreadable after a restart. A scrub finds it, fetches the body from a peer by content hash, and
// rewrites it - durably, so it reads again after another restart - leaving the tree unchanged.
func TestScrubRepairsABodyCorruptedOnDiskFromAPeer(t *testing.T) {
	dir := t.TempDir()
	rt, a := openFileNode(t, dir)
	peer := newMetaNode(t).data
	ids := make([]codec.UUID, 6)
	bodies := make([]string, 6)
	for i := range ids {
		ids[i] = mustRandomUUID(t)
		bodies[i] = fmt.Sprintf(`{"payload":"needle-%d-%s"}`, i, ids[i])
		if _, err := a.Upsert("app/data", ids[i], bodies[i], auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	syncTo(t, a, peer)
	rt.Close()

	if n := corruptOnDisk(t, dir, "needle-2-"); n == 0 {
		t.Fatal("the body was not found on disk to corrupt")
	}
	rt, a = openFileNode(t, dir)
	if readable(a, ids[2], bodies[2]) {
		t.Fatal("the corrupted body still reads - the test is not exercising damage")
	}
	head := mustHeadOf(t, a)

	// Without a source, the scrub reports and records the damage.
	rep, err := a.Scrub(nil)
	if !IsScrubUnrepaired(err) || rep.Checked != len(ids) || !hasString(rep.Damaged, ids[2].String()) {
		t.Fatalf("expected the damage reported: %+v %v", rep, err)
	}
	var recorded bool
	for _, e := range a.Conflicts.List() {
		recorded = recorded || (e.Kind == ConflictDamaged && e.ID == damagedID("app/data", ids[2].String()))
	}
	if !recorded || mustHeadOf(t, a) != head {
		t.Fatal("an unrepaired scrub should record a damaged entry and change nothing")
	}

	// With the peer as a source, it repairs everything it found.
	fetch, stop := fetcherFrom(t, peer)
	defer stop()
	rep, err = a.Scrub(fetch)
	if err != nil || len(rep.Repaired) != len(rep.Damaged) || rep.RepairCommit == "" {
		t.Fatalf("repair: %+v %v", rep, err)
	}
	_, repair, _, _ := a.dag.HeadCommit()
	before, _ := a.dag.GetCommit(head)
	if repair.Message != RepairMessage || repair.DocumentTreeHash != before.DocumentTreeHash {
		t.Fatalf("the repair should be a %s commit that leaves the tree unchanged: %q", RepairMessage, repair.Message)
	}
	for i := range ids {
		if !readable(a, ids[i], bodies[i]) {
			t.Fatalf("document %d is not readable after the repair", i)
		}
	}
	for _, e := range a.Conflicts.List() {
		if e.Kind == ConflictDamaged {
			t.Fatalf("a repaired document keeps its damaged entry: %+v", e)
		}
	}
	rt.Close()

	// Durable: a restart reads the repaired copy, and a fresh scrub finds nothing.
	rt, a = openFileNode(t, dir)
	defer rt.Close()
	for i := range ids {
		if !readable(a, ids[i], bodies[i]) {
			t.Fatalf("document %d is unreadable again after a restart", i)
		}
	}
	if rep, err := a.Scrub(nil); err != nil || len(rep.Damaged) != 0 {
		t.Fatalf("a scrub after the repair should be clean: %+v %v", rep, err)
	}
}

// A body that does not hash to what the tree names is never written, whoever supplied it.
func TestScrubRefusesABodyThatDoesNotMatch(t *testing.T) {
	dir := t.TempDir()
	rt, a := openFileNode(t, dir)
	id := mustRandomUUID(t)
	if _, err := a.Upsert("app/data", id, `{"payload":"needle-x"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	rt.Close()
	corruptOnDisk(t, dir, "needle-x")
	rt, a = openFileNode(t, dir)
	defer rt.Close()
	head := mustHeadOf(t, a)
	lying := func(ns string, wanted map[codec.UUID]codec.Hash, _ string) (map[codec.UUID]string, error) {
		out := map[codec.UUID]string{}
		for id := range wanted {
			out[id] = `{"payload":"forged"}`
		}
		return out, nil
	}
	rep, err := a.Scrub(lying)
	if !IsScrubUnrepaired(err) || len(rep.Repaired) != 0 || mustHeadOf(t, a) != head {
		t.Fatalf("a forged body must be refused: %+v %v", rep, err)
	}
}

// A scrub of a healthy namespace reads everything and changes nothing.
func TestScrubOfAHealthyNamespaceIsClean(t *testing.T) {
	a := newMetaNode(t).data
	for i := 0; i < 20; i++ {
		if _, err := a.Upsert("app/data", mustRandomUUID(t), fmt.Sprintf(`{"i":%d}`, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	head := mustHeadOf(t, a)
	rep, err := a.Scrub(func(string, map[codec.UUID]codec.Hash, string) (map[codec.UUID]string, error) {
		return nil, errors.New("must not be asked")
	})
	if err != nil || rep.Checked != 20 || len(rep.Damaged) != 0 || mustHeadOf(t, a) != head {
		t.Fatalf("healthy scrub: %+v %v", rep, err)
	}
}

// TREE_NODES: a node finds exactly the documents it holds differently from a peer by comparing
// subtree hashes.
func TestRepairSessionDiffFindsTheDocumentsThatDiffer(t *testing.T) {
	a, b := newMetaNode(t).data, newMetaNode(t).data
	for i := 0; i < 50; i++ {
		if _, err := a.Upsert("app/data", mustRandomUUID(t), fmt.Sprintf(`{"i":%d}`, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	syncTo(t, a, b)
	onlyA, onlyB := mustRandomUUID(t), mustRandomUUID(t)
	if _, err := a.Upsert("app/data", onlyA, `{"only":"a"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Upsert("app/data", onlyB, `{"only":"b"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, "app/data")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	s, err := peersync.OpenRepairSession(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: a.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{"app/data"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, headA, _, _ := a.dag.HeadCommit()
	tree, ok, err := a.Runtime.Storage.(storage.TreeResolver).TreeAt(headA.DocumentTreeHash)
	if err != nil || !ok {
		t.Fatalf("A's head tree: %v %v", ok, err)
	}
	_, diff, err := s.Diff("app/data", tree, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(diff) != 2 {
		t.Fatalf("expected exactly the two one-sided documents, got %+v", diff)
	}
	for _, d := range diff {
		switch d.DocID {
		case onlyA:
			if d.Local == (codec.Hash{}) || d.Remote != (codec.Hash{}) {
				t.Fatalf("onlyA should be local-only: %+v", d)
			}
		case onlyB:
			if d.Remote == (codec.Hash{}) || d.Local != (codec.Hash{}) {
				t.Fatalf("onlyB should be remote-only: %+v", d)
			}
		default:
			t.Fatalf("unexpected difference %+v", d)
		}
	}
	// The peer supplies onlyB's body by content hash; asking with a wrong hash gets nothing.
	var bh codec.Hash
	for _, d := range diff {
		if d.DocID == onlyB {
			bh = d.Remote
		}
	}
	got, err := s.Fetch("app/data", map[codec.UUID]codec.Hash{onlyB: bh}, "")
	if err != nil || got[onlyB] != `{"only":"b"}` {
		t.Fatalf("fetch by content hash: %v %v", got, err)
	}
	if got, _ := s.Fetch("app/data", map[codec.UUID]codec.Hash{onlyB: {}}, ""); len(got) != 0 {
		t.Fatalf("a body not matching the asked-for hash must not be returned: %v", got)
	}
}

func hasString(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}
