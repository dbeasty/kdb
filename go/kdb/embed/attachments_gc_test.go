package embed_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

func putVersion(t *testing.T, rt *embed.EmbeddedKdbRuntime, id codec.UUID, content []byte) codec.Hash {
	t.Helper()
	c, _, err := rt.PutAttachment(attachNS, id, bytes.NewReader(content), embed.PutAttachmentOptions{Name: "file", Mime: "x/y"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func compact(t *testing.T, rt *embed.EmbeddedKdbRuntime) embed.AttachmentMaintenance {
	t.Helper()
	res, err := rt.CompactAttachments(attachNS)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// refIndexFiles are the reference index's segment files under root.
func refIndexFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.Contains(p, "attachment") && strings.Contains(filepath.Base(p), "r-0") {
			out = append(out, p)
		}
		return nil
	})
	return out
}

// All-history collection reads all of history once, then only what was committed since - across a
// restart too - and rebuilds its index when it is lost.
func TestAttachmentCollectionReadsOnlyNewCommits(t *testing.T) {
	root := t.TempDir()
	rt := openAttachRuntime(t, root, storage.HistoryModeFull)
	id := codec.UUID{MSB: 21}
	putAttachDoc(t, rt, id, `{}`)
	var commits []codec.Hash
	var versions [][]byte
	put := func(i int) {
		v := bytes.Repeat([]byte{byte('a' + i)}, 5_000+i)
		commits = append(commits, putVersion(t, rt, id, v))
		versions = append(versions, v)
	}
	for i := 0; i < 3; i++ {
		put(i)
	}
	if res := compact(t, rt); !res.Rebuilt || res.Scanned < 4 {
		t.Fatalf("first pass %+v, want all of history read", res)
	}
	put(3)
	put(4)
	if res := compact(t, rt); res.Rebuilt || res.Scanned != 2 {
		t.Fatalf("second pass %+v, want only the 2 new commits read", res)
	}
	if res := compact(t, rt); res.Rebuilt || res.Scanned != 0 {
		t.Fatalf("pass with nothing new %+v", res)
	}
	rt.Close()

	rt = openAttachRuntime(t, root, storage.HistoryModeFull)
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10, UploadGrace: time.Nanosecond})
	if res := compact(t, rt); res.Rebuilt || res.Scanned != 0 || res.Removed != 0 {
		t.Fatalf("pass after a restart %+v, want the index reused and nothing removed", res)
	}
	put(5)
	if res := compact(t, rt); res.Rebuilt || res.Scanned != 1 || res.Removed != 0 {
		t.Fatalf("pass after a restart and a commit %+v", res)
	}
	rt.Close()

	// The index lost: rebuilt from all of history, and the same blobs kept.
	files := refIndexFiles(t, root)
	if len(files) == 0 {
		t.Fatal("no reference index on disk")
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
	rt = openAttachRuntime(t, root, storage.HistoryModeFull)
	defer rt.Close()
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10, UploadGrace: time.Nanosecond})
	if res := compact(t, rt); !res.Rebuilt || res.Removed != 0 {
		t.Fatalf("pass without an index %+v, want a rebuild that removes nothing", res)
	}
	for i, c := range commits {
		if got, _ := readAttachment(t, rt, id, c); !bytes.Equal(got, versions[i]) {
			t.Fatalf("version %d reads back wrong", i)
		}
	}
}

// Replaced versions re-stored as deltas stay readable through incremental passes: a delta's base
// is kept with it.
func TestDeltaBasesSurviveIncrementalCollection(t *testing.T) {
	rt := openAttachRuntime(t, t.TempDir(), storage.HistoryModeFull)
	defer rt.Close()
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10, UploadGrace: time.Nanosecond})
	id := codec.UUID{MSB: 22}
	putAttachDoc(t, rt, id, `{}`)
	base := bytes.Repeat([]byte("0123456789abcdef"), 4_000)
	var commits []codec.Hash
	var versions [][]byte
	for i := 0; i < 5; i++ {
		v := append([]byte(nil), base...)
		copy(v[i*1000:], []byte("an edit to this version"))
		commits = append(commits, putVersion(t, rt, id, v))
		versions = append(versions, v)
		res := compact(t, rt)
		if i > 0 && (res.Deltas != 1 || res.Removed != 0) {
			t.Fatalf("pass %d %+v, want the replaced version stored as a delta and nothing removed", i, res)
		}
	}
	for i, c := range commits {
		if got, _ := readAttachment(t, rt, id, c); !bytes.Equal(got, versions[i]) {
			t.Fatalf("version %d reads back wrong", i)
		}
	}
}

// Under history=none, once history is truncated, a version only the truncated history referenced
// is collected; one a retained record still references - the same bytes another document has - is
// kept.
func TestCollectionAfterHistoryNoneTruncation(t *testing.T) {
	root := t.TempDir()
	rt := openAttachRuntime(t, root, storage.HistoryModeNone)
	a, b := codec.UUID{MSB: 23}, codec.UUID{MSB: 24}
	putAttachDoc(t, rt, a, `{}`)
	putAttachDoc(t, rt, b, `{}`)
	only, shared, last := bytes.Repeat([]byte("w"), 50_000), bytes.Repeat([]byte("x"), 50_000), bytes.Repeat([]byte("y"), 50_000)
	putVersion(t, rt, a, only)
	putVersion(t, rt, a, shared)
	putVersion(t, rt, b, shared)
	putVersion(t, rt, a, last)
	if _, err := rt.Maintain(); err != nil {
		t.Fatal(err)
	}
	rt.Close()

	rt = openAttachRuntime(t, root, storage.HistoryModeNone)
	defer rt.Close()
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10, UploadGrace: time.Nanosecond})
	res := compact(t, rt)
	if !res.Rebuilt {
		t.Fatalf("after truncation %+v, want the index rebuilt", res)
	}
	for name, c := range map[string]struct {
		content []byte
		kept    bool
	}{"only-in-truncated-history": {only, false}, "shared": {shared, true}, "latest": {last, true}} {
		has, err := rt.HasAttachmentBlob(attachNS, document.AttachmentContentHash(c.content))
		if err != nil {
			t.Fatal(err)
		}
		if has != c.kept {
			t.Fatalf("%s: stored %v, want %v (%+v)", name, has, c.kept, res)
		}
	}
	if got, _ := readAttachment(t, rt, a, codec.Hash{}); !bytes.Equal(got, last) {
		t.Fatal("a's latest version reads back wrong")
	}
	if got, _ := readAttachment(t, rt, b, codec.Hash{}); !bytes.Equal(got, shared) {
		t.Fatal("b's version reads back wrong")
	}
}

func openSpillingRuntime(t *testing.T, root string, mode storage.HistoryMode) *embed.EmbeddedKdbRuntime {
	t.Helper()
	opts := embed.FileRuntimeOptions{}
	opts.Storage.MemoryBudgetBytes = 4 << 20
	opts.Storage.HistoryMode = mode
	opts.Storage.AttachmentSetBudgetBytes = 1 // every set but the newest goes to the blob store
	if mode == storage.HistoryModeNone {
		opts.Storage.Retain = storage.RetentionWindow{Duration: storage.RetainNothing}
	}
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", attachNS, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10})
	return rt
}

// With attachment sets spilled to the blob store, every commit's attachments read back - at once,
// after a restart, and through the blob store's merges. Under history=none a merge drops the sets
// only replaced versions used and keeps the ones the heads use.
func TestAttachmentSetsSpillToTheBlobStore(t *testing.T) {
	for _, mode := range []storage.HistoryMode{storage.HistoryModeFull, storage.HistoryModeNone} {
		t.Run(mode.String(), func(t *testing.T) {
			root := t.TempDir()
			rt := openSpillingRuntime(t, root, mode)
			eng := rt.Storage.(*engine.ServerEngine)
			docs := []codec.UUID{{MSB: 31}, {MSB: 32}, {MSB: 33}}
			for _, d := range docs {
				putAttachDoc(t, rt, d, `{}`)
			}
			type point struct {
				commit codec.Hash
				doc    codec.UUID
				set    codec.Hash
				want   []byte
			}
			var points []point
			latest := map[codec.UUID][]byte{}
			dropped := 0
			for i := 0; i < 30; i++ {
				d := docs[i%len(docs)]
				v := []byte(strings.Repeat(string(rune('a'+i)), 10+i))
				c := putVersion(t, rt, d, v)
				table, err := rt.AttachmentTableAt(c)
				if err != nil {
					t.Fatal(err)
				}
				set, _ := table.SetHash(d)
				points = append(points, point{c, d, set, v})
				latest[d] = v
				if i%5 == 4 { // a run per flush: enough of them make a merge
					res, err := eng.CompactBlobStore()
					if err != nil {
						t.Fatal(err)
					}
					dropped += res.Dropped
				}
			}
			if st := rt.AttachmentSetPoolStats(); st.Spills == 0 || st.Sets > 2 {
				t.Fatalf("pool %+v, want sets spilled and at most a couple resident", st)
			}
			checkHeads := func() {
				t.Helper()
				for d, v := range latest {
					if got, _ := readAttachment(t, rt, d, codec.Hash{}); !bytes.Equal(got, v) {
						t.Fatalf("doc %s at the head reads %q, want %q", d, got, v)
					}
				}
			}
			checkHistory := func() {
				t.Helper()
				for _, p := range points {
					if got, _ := readAttachment(t, rt, p.doc, p.commit); !bytes.Equal(got, p.want) {
						t.Fatalf("doc %s at %s reads %q, want %q", p.doc, p.commit.Hex()[:8], got, p.want)
					}
				}
			}
			checkHeads()
			if mode == storage.HistoryModeFull {
				checkHistory()
			} else {
				// More merges with no set written: the written-set grace runs out, and only the
				// heads' use keeps their sets.
				for k := 0; k < 12; k++ {
					// Writes to other documents make the runs a merge needs.
					filler := codec.UUID{MSB: 900 + int64(k)}
					putAttachDoc(t, rt, filler, `{}`)
					fv := []byte(strings.Repeat("f", 5+k))
					putVersion(t, rt, filler, fv)
					latest[filler] = fv
					res, err := eng.CompactBlobStore()
					if err != nil {
						t.Fatal(err)
					}
					dropped += res.Dropped
				}
				checkHeads()
				if dropped == 0 {
					t.Fatal("no blob store merge dropped anything")
				}
				// The first versions' sets are gone from the blob store; the heads' are there.
				if b, _ := eng.ReadBlob(points[0].set); b != nil {
					t.Fatal("a set only a replaced version used survived the merges")
				}
				for _, p := range points[len(points)-len(docs):] {
					if b, _ := eng.ReadBlob(p.set); b == nil && !rt.AttachmentSetPoolContains(p.set) {
						t.Fatalf("the head's set of %s is neither stored nor resident", p.doc)
					}
				}
			}
			rt.Close()
			rt = openSpillingRuntime(t, root, mode)
			defer rt.Close()
			checkHeads()
			if mode == storage.HistoryModeFull {
				checkHistory()
			}
		})
	}
}

// A version only a deleted branch referenced is collected: the reference index's heads are no
// longer all below the current ones, so the next pass rebuilds it from what is still reachable.
func TestCollectionAfterABranchIsDeleted(t *testing.T) {
	rt := openAttachRuntime(t, t.TempDir(), storage.HistoryModeFull)
	defer rt.Close()
	rt.SetAttachmentConfig(attachstore.Config{SmallMaxBytes: 1 << 10, UploadGrace: time.Nanosecond})
	persisting, ok := rt.DAG.(*embed.PersistingCommitDAG)
	if !ok {
		t.Fatalf("DAG is %T", rt.DAG)
	}
	d := persisting.Delegate()
	id := codec.UUID{MSB: 25}
	putAttachDoc(t, rt, id, `{}`)
	onMain := bytes.Repeat([]byte("m"), 50_000)
	mainCommit := putVersion(t, rt, id, onMain)

	// A side branch whose one commit attaches bytes nothing on main has.
	onSide := bytes.Repeat([]byte("s"), 50_000)
	h, size, err := rt.PutAttachmentBlob(attachNS, bytes.NewReader(onSide))
	if err != nil {
		t.Fatal(err)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	attID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	side, err := d.AppendCommitDetached(document.Transaction{ID: txID, BaseVersion: mainCommit, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.AttachmentPutOp{DocID: id, AttachmentID: attID, Name: "side",
			Scheme: document.AttachmentHashSchemeChunked1, ContentHash: h, Size: size, Mime: "x/y"}}},
		mainCommit, document.EmptyDocumentTree(), nil, "side")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.CreateBranch("side", side.Hash); err != nil {
		t.Fatal(err)
	}
	// Appending re-points main; put it back on its own line of history, beside the branch.
	txID2, _ := codec.RandomUUID()
	if _, err := d.AppendCommitDetached(document.Transaction{ID: txID2, BaseVersion: mainCommit, Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.WriteOp{DocID: codec.UUID{MSB: 26}, Patch: `{}`}}},
		mainCommit, document.EmptyDocumentTree(), nil, "main again"); err != nil {
		t.Fatal(err)
	}
	if head, _ := d.Head(); head == side.Hash || d.IsAncestor(side.Hash, head) {
		t.Fatal("main still has the side commit")
	}
	// Past the upload grace even on a coarse wall clock: only references keep blobs from here.
	for start := time.Now().Round(0); !time.Now().Round(0).After(start); {
		time.Sleep(time.Millisecond)
	}
	if res := compact(t, rt); res.Removed != 0 {
		t.Fatalf("with the branch: %+v, want nothing removed", res)
	}
	if has, _ := rt.HasAttachmentBlob(attachNS, h); !has {
		t.Fatal("the side branch's version was collected while the branch exists")
	}

	if err := d.DeleteBranch("side"); err != nil {
		t.Fatal(err)
	}
	res := compact(t, rt)
	if !res.Rebuilt || res.Removed != 1 {
		t.Fatalf("after deleting the branch: %+v, want the index rebuilt and one blob removed", res)
	}
	if has, _ := rt.HasAttachmentBlob(attachNS, h); has {
		t.Fatal("the deleted branch's version is still stored")
	}
	if got, _ := readAttachment(t, rt, id, codec.Hash{}); !bytes.Equal(got, onMain) {
		t.Fatal("main's version reads back wrong")
	}
	// Nothing changed since: the rebuilt index is reused.
	if res := compact(t, rt); res.Rebuilt || res.Scanned != 0 || res.Removed != 0 {
		t.Fatalf("pass after the rebuild: %+v", res)
	}
}
