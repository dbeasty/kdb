package integration

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// The Kotlin CLI opens a namespace from Go's checkpoint (kdb-jdbc CheckpointOpen.kt) instead of
// replaying its whole delta log. These tests have Go write namespaces that produce each checkpoint
// format and feature, then have the Kotlin CLI read every document back. A read must be exactly
// what Go holds - from the checkpoint where Kotlin can use it (proved by the line it logs, and by
// truncated logs a replay could not rebuild), and from a full replay where it cannot.

const cpNS = "app/data"

// cpDoc is the state a namespace should read as: live bodies, and documents deleted.
type cpDoc struct {
	live map[codec.UUID]string
	gone map[codec.UUID]bool
}

func newCPDoc() *cpDoc { return &cpDoc{live: map[codec.UUID]string{}, gone: map[codec.UUID]bool{}} }

func cpOptions(mut func(*embed.FileRuntimeOptions)) embed.FileRuntimeOptions {
	opts := embed.FileRuntimeOptions{}
	opts.Storage.SyncMode = storio.SyncModeFast
	if mut != nil {
		mut(&opts)
	}
	return opts
}

func openCP(t *testing.T, root string, opts embed.FileRuntimeOptions) (*embed.EmbeddedKdbRuntime, *server.KdbServerRuntime) {
	t.Helper()
	rt, err := embed.OpenFileRuntimeWithOptions(root, "app", cpNS, schema.None(), opts)
	if err != nil {
		t.Fatal(err)
	}
	s := server.NewKdbServerRuntime(rt)
	s.NodeID = randomID(t)
	return rt, s
}

// writeBatch commits ops for the given documents in one transaction; a "" body deletes.
func writeBatch(t *testing.T, s *server.KdbServerRuntime, want *cpDoc, batch map[codec.UUID]string) {
	t.Helper()
	head, err := s.PeerIngestEnv().DAG.Head()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]codec.UUID, 0, len(batch))
	for id := range batch {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	ops := make([]document.Op, 0, len(batch))
	for _, id := range ids {
		if body := batch[id]; body == "" {
			ops = append(ops, document.DeleteOp{DocID: id})
			delete(want.live, id)
			want.gone[id] = true
		} else {
			ops = append(ops, document.WriteOp{DocID: id, Patch: body, Replace: true})
			want.live[id] = body
			delete(want.gone, id)
		}
	}
	if _, err := s.Commit(cpNS, document.Transaction{ID: randomID(t), BaseVersion: head, Operations: ops,
		Timestamp: codec.TimestampNow()}, "", auth.Principal{}); err != nil {
		t.Fatal(err)
	}
}

// seedDocs writes n documents in batches, rewrites a few and deletes one: every kind of change.
func seedDocs(t *testing.T, s *server.KdbServerRuntime, want *cpDoc, n, batchSize int) []codec.UUID {
	t.Helper()
	ids := make([]codec.UUID, n)
	for start := 0; start < n; start += batchSize {
		batch := map[codec.UUID]string{}
		for i := start; i < n && i < start+batchSize; i++ {
			ids[i] = randomID(t)
			batch[ids[i]] = fmt.Sprintf(`{"i":%d,"pad":"%s"}`, i, strings.Repeat("p", i%7))
		}
		writeBatch(t, s, want, batch)
	}
	for r := 0; r < 3; r++ {
		writeBatch(t, s, want, map[codec.UUID]string{ids[r]: fmt.Sprintf(`{"i":%d,"rewrite":%d}`, r, r)})
	}
	writeBatch(t, s, want, map[codec.UUID]string{ids[n-1]: ""})
	return ids
}

var openedFromCheckpoint = regexp.MustCompile(`opened from its checkpoint \(format v(\d+), through delta segment (-?\d+); ([^)]*)\); replayed (\d+) later segment`)

// kotlinOpen is what one Kotlin CLI run said about how it opened the namespace.
type kotlinOpen struct {
	fromCheckpoint bool
	version        int
	tail           int
	checkedBy      string
	stderr         string
}

// checkKotlinReads reads every document of want through one Kotlin CLI shell session and fails
// unless each live one reads exactly as Go holds it and each deleted one is not found.
func checkKotlinReads(t *testing.T, repo, root string, want *cpDoc) kotlinOpen {
	t.Helper()
	live := make([]codec.UUID, 0, len(want.live))
	for id := range want.live {
		live = append(live, id)
	}
	sort.Slice(live, func(i, j int) bool { return live[i].String() < live[j].String() })
	var in strings.Builder
	for _, id := range live {
		fmt.Fprintf(&in, "get %s\n", id)
	}
	for id := range want.gone {
		fmt.Fprintf(&in, "get %s\n", id)
	}
	cmd := kotlinCommand(repo, "--data-dir", root, "--quiet", "shell", cpNS)
	cmd.Stdin = strings.NewReader(in.String())
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("kotlin shell: %v\n%s", err, errb.String())
	}
	stderr := errb.String()
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(live) > 0 && len(lines) < len(live) {
		t.Fatalf("Kotlin printed %d line(s) for %d live documents; stderr:\n%s", len(lines), len(live), tail(stderr))
	}
	lines = lines[len(lines)-len(live):] // anything before is gradle's, not the CLI's
	bad := 0
	for i, id := range live {
		if !sameJSON(lines[i], want.live[id]) {
			if bad < 5 {
				t.Errorf("Kotlin read %q for %s, Go holds %s", lines[i], id, want.live[id])
			}
			bad++
		}
	}
	if bad > 0 {
		t.Fatalf("%d of %d documents read wrong; stderr:\n%s", bad, len(live), tail(stderr))
	}
	if n := strings.Count(stderr, "document not found"); n != len(want.gone) {
		t.Fatalf("Kotlin found %d of %d deleted documents missing; stderr:\n%s", n, len(want.gone), tail(stderr))
	}
	res := kotlinOpen{stderr: stderr}
	if m := openedFromCheckpoint.FindStringSubmatch(stderr); m != nil {
		res.fromCheckpoint = true
		res.version, _ = strconv.Atoi(m[1])
		res.checkedBy = m[3]
		res.tail, _ = strconv.Atoi(m[4])
	}
	return res
}

func openLine(stderr string) string {
	for _, l := range strings.Split(stderr, "\n") {
		if strings.Contains(l, "opened from its checkpoint") {
			return strings.TrimSpace(l[strings.Index(l, "replayed"):])
		}
	}
	return ""
}

func tail(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > 20 {
		lines = lines[len(lines)-20:]
	}
	return strings.Join(lines, "\n")
}

func requireCheckpointOpen(t *testing.T, got kotlinOpen, versions ...int) {
	t.Helper()
	if !got.fromCheckpoint {
		t.Fatalf("Kotlin did not open from the checkpoint; stderr:\n%s", tail(got.stderr))
	}
	for _, v := range versions {
		if got.version == v {
			t.Logf("Kotlin opened from a v%d checkpoint (%s), replaying %d later segment(s): %s", got.version, got.checkedBy, got.tail, openLine(got.stderr))
			return
		}
	}
	t.Fatalf("Kotlin opened from a v%d checkpoint, want one of %v", got.version, versions)
}

func deltaSequences(t *testing.T, root string) []int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "ns", "app", "data", "delta"))
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, e := range entries {
		if n, err := strconv.Atoi(strings.TrimSuffix(e.Name(), ".seg")); err == nil {
			out = append(out, n)
		}
	}
	sort.Ints(out)
	return out
}

func checkpointFile(root string) string {
	return filepath.Join(root, "snap", "kdb_checkpoint_app%2Fdata")
}

// sectionFiles are version 6 checkpoint sections on disk.
func sectionFiles(t *testing.T, root string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(root, "snap", "kdb_checkpoint-section_*"))
	return m
}

// largeEntries passes the bound past which Go writes a packed checkpoint (legacyCheckpointMaxEntries).
const largeEntries = 21_000

// TestKotlinOpensFromGoCheckpoint: every checkpoint format and feature Go writes, read by Kotlin.
func TestKotlinOpensFromGoCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)

	t.Run("v2-small", func(t *testing.T) {
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(nil))
		seedDocs(t, s, want, 50, 10)
		rt.Close()
		requireCheckpointOpen(t, checkKotlinReads(t, repo, root, want), 2)
	})

	t.Run("v2-with-a-tail", func(t *testing.T) {
		// The checkpoint covers the first session; the second logs on top of it without writing
		// another, so Kotlin restores the first and replays the second's segment.
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(nil))
		ids := seedDocs(t, s, want, 30, 10)
		rt.Close()
		rt, s = openCP(t, root, cpOptions(func(o *embed.FileRuntimeOptions) { o.Storage.DisableCheckpoints = true }))
		writeBatch(t, s, want, map[codec.UUID]string{ids[5]: `{"after":"checkpoint"}`, ids[6]: "", randomID(t): `{"new":true}`})
		rt.Close()
		got := checkKotlinReads(t, repo, root, want)
		requireCheckpointOpen(t, got, 2)
		if got.tail == 0 {
			t.Fatal("Kotlin replayed no segment after the checkpoint, yet the second session's writes are only there")
		}
	})

	t.Run("packed-large", func(t *testing.T) {
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(nil))
		seedDocs(t, s, want, largeEntries, 1000)
		rt.Close()
		requireCheckpointOpen(t, checkKotlinReads(t, repo, root, want), 4, 6)
	})

	t.Run("packed-large-tree-file", func(t *testing.T) {
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(func(o *embed.FileRuntimeOptions) { o.Storage.TreeFile = true }))
		seedDocs(t, s, want, largeEntries, 1000)
		rt.Close()
		if m, _ := filepath.Glob(filepath.Join(root, "ns", "app", "data", "tree", "*")); len(m) == 0 {
			t.Fatal("tree.file is on but Go wrote no tree segment")
		}
		requireCheckpointOpen(t, checkKotlinReads(t, repo, root, want), 4, 6)
	})

	t.Run("replay-strategy-scan", func(t *testing.T) {
		// The replay history strategy files no location records, so the bodies of documents last
		// written before the checkpoint are found by scanning the segments it covers.
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(func(o *embed.FileRuntimeOptions) { o.Storage.HistoryStrategy = storage.HistoryStrategyReplay }))
		seedDocs(t, s, want, 40, 10)
		rt.Close()
		got := checkKotlinReads(t, repo, root, want)
		requireCheckpointOpen(t, got, 2)
		if !strings.Contains(got.stderr, "segment scan 39") {
			t.Fatalf("expected every body from the segment scan: %s", openLine(got.stderr))
		}
	})

	t.Run("v2-tree-file", func(t *testing.T) {
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(func(o *embed.FileRuntimeOptions) { o.Storage.TreeFile = true }))
		seedDocs(t, s, want, 40, 10)
		rt.Close()
		requireCheckpointOpen(t, checkKotlinReads(t, repo, root, want), 2)
	})

	t.Run("attachments", func(t *testing.T) {
		// A namespace with attachments is never version 2: its packed commits carry attachment
		// roots, and the heads' attachment tables ride along (v5, or v6 with field 21).
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(nil))
		ids := seedDocs(t, s, want, 20, 5)
		if _, _, err := rt.PutAttachment(cpNS, ids[3], strings.NewReader("attachment bytes"),
			embed.PutAttachmentOptions{ID: randomID(t), Name: "a.txt", Mime: "text/plain"}); err != nil {
			t.Fatal(err)
		}
		writeBatch(t, s, want, map[codec.UUID]string{ids[4]: `{"after":"attachment"}`})
		rt.Close()
		requireCheckpointOpen(t, checkKotlinReads(t, repo, root, want), 5, 6)
	})

	t.Run("graph-file-without-segments", func(t *testing.T) {
		// graph.file on, but too few commits for Go to have written a graph segment: the
		// checkpoint carries every commit and Kotlin uses it.
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(func(o *embed.FileRuntimeOptions) { o.Storage.Graph.FileEnabled = true }))
		seedDocs(t, s, want, 30, 10)
		rt.Close()
		requireCheckpointOpen(t, checkKotlinReads(t, repo, root, want), 2)
	})

	t.Run("graph-file-with-segments", func(t *testing.T) {
		// history=full with graph segments on disk: the checkpoint references them for the commits
		// it does not carry (fields 11/12), which this build does not read - so Kotlin replays the
		// log, and every read is still right.
		root, want := t.TempDir(), newCPDoc()
		opts := cpOptions(func(o *embed.FileRuntimeOptions) {
			o.Storage.Graph.FileEnabled = true
			o.Storage.Graph.RebuildCommits = 16
		})
		rt, s := openCP(t, root, opts)
		seedDocs(t, s, want, 120, 2)
		rt.Close()
		rt, s = openCP(t, root, opts) // a second session: the first left graph segments behind
		writeBatch(t, s, want, map[codec.UUID]string{randomID(t): `{"session":2}`})
		rt.Close()
		if m, _ := filepath.Glob(filepath.Join(root, "ns", "app", "data", "graph", "*")); len(m) == 0 {
			t.Fatal("Go wrote no graph segment")
		}
		got := checkKotlinReads(t, repo, root, want)
		if got.fromCheckpoint || !strings.Contains(got.stderr, "graph segment") {
			t.Fatalf("expected Kotlin to replay because the checkpoint references graph segments; stderr:\n%s", tail(got.stderr))
		}
	})

	t.Run("history-none-truncated", func(t *testing.T) {
		// The log below the checkpoint's floor is gone, so a replay could not rebuild this
		// namespace: reading it at all proves the checkpoint was used, and every body comes from
		// the blob store Go materialized before truncating.
		root, want := t.TempDir(), newCPDoc()
		none := func(window storage.RetentionWindow) embed.FileRuntimeOptions {
			return cpOptions(func(o *embed.FileRuntimeOptions) {
				o.Storage.HistoryMode = storage.HistoryModeNone
				o.Storage.Retain = window
			})
		}
		var ids []codec.UUID
		for session := 0; session < 4; session++ {
			rt, s := openCP(t, root, none(storage.RetentionWindow{}))
			if ids == nil {
				ids = seedDocs(t, s, want, 40, 10)
			} else {
				writeBatch(t, s, want, map[codec.UUID]string{ids[session]: fmt.Sprintf(`{"session":%d}`, session)})
			}
			rt.Close()
		}
		rt, _ := openCP(t, root, none(storage.RetentionWindow{Duration: storage.RetainNothing}))
		res, err := rt.Maintain()
		if err != nil {
			t.Fatal(err)
		}
		rt.Close()
		seqs := deltaSequences(t, root)
		if res.Removed == 0 || len(seqs) == 0 || seqs[0] == 0 {
			t.Fatalf("expected Go to truncate the log (removed %d, segments %v)", res.Removed, seqs)
		}
		requireCheckpointOpen(t, checkKotlinReads(t, repo, root, want), 2)
	})
}

// TestKotlinFallsBackFromAnUnusableCheckpoint: a checkpoint that is damaged, missing, or whose
// section is gone or damaged costs Kotlin a full replay, never a wrong read.
func TestKotlinFallsBackFromAnUnusableCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	build := func(t *testing.T, n, batch int) (string, *cpDoc) {
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(nil))
		seedDocs(t, s, want, n, batch)
		rt.Close()
		return root, want
	}
	requireReplay := func(t *testing.T, got kotlinOpen, why string) {
		t.Helper()
		if got.fromCheckpoint {
			t.Fatalf("Kotlin opened from a checkpoint that should be unusable (%s)", why)
		}
		if why != "" && !strings.Contains(got.stderr, "replaying the log in full") {
			t.Fatalf("Kotlin replayed without saying why; stderr:\n%s", tail(got.stderr))
		}
		t.Logf("Kotlin replayed the log in full (%s)", why)
	}

	t.Run("damaged-record", func(t *testing.T) {
		root, want := build(t, 30, 10)
		raw, err := os.ReadFile(checkpointFile(root))
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)/2] ^= 0xFF
		if err := os.WriteFile(checkpointFile(root), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		requireReplay(t, checkKotlinReads(t, repo, root, want), "damaged record")
	})

	t.Run("missing", func(t *testing.T) {
		root, want := build(t, 30, 10)
		if err := os.Remove(checkpointFile(root)); err != nil {
			t.Fatal(err)
		}
		requireReplay(t, checkKotlinReads(t, repo, root, want), "")
	})

	t.Run("segment-changed", func(t *testing.T) {
		// A covered segment that is no longer what the checkpoint fingerprinted (here, one more
		// commit appended to it by hand) makes the checkpoint untrustworthy.
		root, want := t.TempDir(), newCPDoc()
		rt, s := openCP(t, root, cpOptions(nil))
		seedDocs(t, s, want, 20, 5)
		rt.Close()
		seqs := deltaSequences(t, root)
		first := filepath.Join(root, "ns", "app", "data", "delta", fmt.Sprintf("%020d.seg", seqs[0]))
		rt, s = openCP(t, root, cpOptions(func(o *embed.FileRuntimeOptions) { o.Storage.DisableCheckpoints = true }))
		writeBatch(t, s, want, map[codec.UUID]string{randomID(t): `{"appended":true}`})
		rt.Close()
		seqs = deltaSequences(t, root)
		second := filepath.Join(root, "ns", "app", "data", "delta", fmt.Sprintf("%020d.seg", seqs[len(seqs)-1]))
		// Move the second session's frames onto the end of the first segment.
		a, _ := os.ReadFile(first)
		b, _ := os.ReadFile(second)
		if err := os.WriteFile(first, append(a, b...), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(second); err != nil {
			t.Fatal(err)
		}
		requireReplay(t, checkKotlinReads(t, repo, root, want), "segment changed")
	})

	for _, damage := range []string{"section-missing", "section-damaged"} {
		t.Run(damage, func(t *testing.T) {
			root, want := build(t, largeEntries, 1000)
			sections := sectionFiles(t, root)
			if len(sections) == 0 {
				t.Skip("Go wrote no version 6 checkpoint section (feat/checkpoint-v6-tree-section not merged)")
			}
			for _, p := range sections {
				if damage == "section-missing" {
					os.Remove(p)
					continue
				}
				raw, _ := os.ReadFile(p)
				raw[len(raw)/3] ^= 0x01
				os.WriteFile(p, raw, 0o644)
			}
			requireReplay(t, checkKotlinReads(t, repo, root, want), damage)
		})
	}
}

// TestKotlinOpensASnapshotBootstrappedGoNamespace: B, a Go file-backed namespace, bootstraps from
// A's snapshot - main is A's head as a shallow root, its documents in B's blob store and its
// checkpoint, and nowhere in B's log. Kotlin used to refuse it (#209); it now opens it from the
// checkpoint and reads every document, with and without commits logged on top.
func TestKotlinOpensASnapshotBootstrappedGoNamespace(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the Kotlin CLI through gradle")
	}
	repo := findRepoRoot(t)
	for _, writeOnTop := range []bool{false, true} {
		t.Run(fmt.Sprintf("write-on-top=%v", writeOnTop), func(t *testing.T) {
			root := t.TempDir()
			mrt, err := embed.OpenMemoryRuntime("app", cpNS, schema.None())
			if err != nil {
				t.Fatal(err)
			}
			a := server.NewKdbServerRuntime(mrt)
			a.NodeID = randomID(t)
			want := newCPDoc()
			ids := seedDocs(t, a, want, 40, 8)
			ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", a, cpNS)
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()

			rt, b := openCP(t, root, cpOptions(nil))
			res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
				NodeID: b.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{cpNS},
				Local: b.PeerNamespaces(), Mode: peersync.SyncPull, PreferSnapshot: true,
			})
			if err != nil || len(res.Namespaces) != 1 || res.Namespaces[0].Err != nil || res.Namespaces[0].Snapshot == "" {
				t.Fatalf("snapshot bootstrap: %v %+v", err, res)
			}
			rt.Close()
			if writeOnTop {
				// Logged after the snapshot's checkpoint and not checkpointed again - the state a
				// crash leaves, taken by copying the directory while Go has it open (Go will not
				// open a shallow namespace with checkpoints off) - so Kotlin must restore the
				// snapshot and replay these on top of it.
				rt, b = openCP(t, root, cpOptions(nil))
				writeBatch(t, b, want, map[codec.UUID]string{ids[1]: `{"local":"on top of the snapshot"}`, ids[2]: "", randomID(t): `{"local":"new"}`})
				crashed := t.TempDir()
				copyTree(t, root, crashed)
				rt.Close()
				root = crashed
			}
			meta, _ := os.ReadFile(filepath.Join(root, "ns", "app", "data", "meta.json"))
			if !strings.Contains(string(meta), "shallowRoots") {
				t.Fatalf("expected the marker to name a shallow root: %s", meta)
			}
			got := checkKotlinReads(t, repo, root, want)
			requireCheckpointOpen(t, got, 2, 4, 5, 6)
			if writeOnTop && got.tail == 0 {
				t.Fatal("Kotlin replayed nothing after the snapshot's checkpoint")
			}
		})
	}
}

// copyTree copies a data directory as it is on disk, minus the lock a live process holds on it.
func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.Walk(from, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, p)
		dst := filepath.Join(to, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		if info.Name() == ".kdb.lock" {
			return nil
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, raw, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}
