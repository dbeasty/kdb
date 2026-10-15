// Command kdb-cpinterop is the Go side of the checkpoint interop tests between the Go and Kotlin
// runtimes (kdb-integration CheckpointInteropTest): one checkpoint format, written and read by both.
//
//	kdb-cpinterop dump -dir D -ns N
//	    Opens the namespace read-only and prints its state as one JSON object: whether open replayed
//	    the log in full (it must not, when a checkpoint is usable), main's head and tree, branches,
//	    tags, shallow roots, the attachment root at every ref, and every live document at main.
//	kdb-cpinterop squash -dir D -ns N [-docs K]
//	    Writes a Go namespace that caught up with a peer by squash (a shallow root only its
//	    checkpoint records), with a write, a branch, a tag and an attachment on top, and closes it.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/server"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kdb-cpinterop dump|squash -dir D -ns N")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	dir := fs.String("dir", "", "data directory")
	ns := fs.String("ns", "app/data", "namespace")
	docs := fs.Int("docs", 6, "documents the squash scenario's peer writes")
	_ = fs.Parse(os.Args[2:])
	var err error
	switch os.Args[1] {
	case "dump":
		err = dump(*dir, *ns)
	case "squash":
		err = squash(*dir, *ns, *docs)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kdb-cpinterop:", err)
		os.Exit(1)
	}
}

// State is a namespace as both runtimes must read it. Hashes are hex; attachment roots are "" for
// a commit without attachments.
type State struct {
	ReplayedInFull  bool              `json:"replayedInFull"`
	Head            string            `json:"head"`
	Tree            string            `json:"tree"`
	Branches        map[string]string `json:"branches"`
	Tags            map[string]string `json:"tags"`
	ShallowRoots    []string          `json:"shallowRoots"`
	AttachmentRoots map[string]string `json:"attachmentRoots"`
	Documents       map[string]string `json:"documents"`
}

func dump(dir, ns string) error {
	rt, err := embed.OpenReadOnlyFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		return err
	}
	defer rt.Close()
	d := server.NewKdbServerRuntime(rt).PeerIngestEnv().DAG
	if d == nil {
		return errors.New("the namespace has no in-memory DAG")
	}
	head, err := d.Head()
	if err != nil {
		return err
	}
	headCommit, err := d.GetCommitOrThrow(head)
	if err != nil {
		return err
	}
	st := State{
		ReplayedInFull:  rt.Recovery().ReplayedInFull,
		Head:            head.Hex(),
		Tree:            headCommit.DocumentTreeHash.Hex(),
		Branches:        map[string]string{},
		Tags:            map[string]string{},
		ShallowRoots:    []string{},
		AttachmentRoots: map[string]string{},
		Documents:       map[string]string{},
	}
	refs := []codec.Hash{}
	for _, b := range d.ListBranches() {
		st.Branches[b.Name] = b.HeadHash.Hex()
		refs = append(refs, b.HeadHash)
	}
	for _, t := range d.ListTags() {
		st.Tags[t.Name] = t.CommitHash.Hex()
		refs = append(refs, t.CommitHash)
	}
	for _, h := range d.ShallowRoots() {
		st.ShallowRoots = append(st.ShallowRoots, h.Hex())
	}
	sort.Strings(st.ShallowRoots)
	for _, h := range refs {
		table, err := rt.AttachmentTableAt(h)
		if err != nil {
			return fmt.Errorf("attachment table at %s: %w", h.Hex(), err)
		}
		root := ""
		if r := table.Root(); r != nil {
			root = r.Hex()
		}
		st.AttachmentRoots[h.Hex()] = root
	}
	// The engine scans by tree hash.
	err = rt.Storage.ScanDocuments(ns, headCommit.DocumentTreeHash, 1000, func(batch []document.Document) error {
		for _, doc := range batch {
			st.Documents[doc.ID.String()] = doc.JSON
		}
		return nil
	})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	return enc.Encode(st)
}

// squash is go/kdb/integration/squash_interop_test.go's scenario as a command: B (file-backed, at
// dir) pulls from A (in memory) by replay, A moves on by rewrites, a delete and a new document, B
// catches up by squash, and then B writes on top, adds a branch and a tag at the squash root, and
// attaches a file - all recorded only by B's checkpoint and the log after it.
func squash(dir, ns string, docs int) error {
	mrt, err := embed.OpenMemoryRuntime("app", ns, schema.None())
	if err != nil {
		return err
	}
	a := server.NewKdbServerRuntime(mrt)
	a.NodeID = newID()
	fileRT, err := embed.OpenFileRuntime(dir, "app", ns, schema.None())
	if err != nil {
		return err
	}
	defer fileRT.Close()
	b := server.NewKdbServerRuntime(fileRT)
	b.NodeID = newID()

	upsert := func(rt *server.KdbServerRuntime, id codec.UUID, body string) error {
		_, err := rt.Upsert(ns, id, body, auth.Principal{})
		return err
	}
	ids := make([]codec.UUID, docs)
	for i := range ids {
		ids[i] = newID()
		if err := upsert(a, ids[i], fmt.Sprintf(`{"v":%d}`, i)); err != nil {
			return err
		}
	}
	ln, err := server.ListenPeerSync("tcp://127.0.0.1:0?bind=true", a, ns)
	if err != nil {
		return err
	}
	defer ln.Close()
	pull := func(route peersync.CatchUpRoute) (peersync.NamespaceSyncResult, error) {
		res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: b.NodeID.String(), PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns},
			Local: b.PeerNamespaces(), Mode: peersync.SyncPull, CatchUp: route,
		})
		if err != nil {
			return peersync.NamespaceSyncResult{}, err
		}
		if len(res.Namespaces) != 1 || res.Namespaces[0].Err != nil {
			return peersync.NamespaceSyncResult{}, fmt.Errorf("pull: %+v", res)
		}
		return res.Namespaces[0], nil
	}
	if _, err := pull(peersync.CatchUpReplay); err != nil {
		return err
	}
	for i := 0; i < 60; i++ {
		if err := upsert(a, ids[i%len(ids)], fmt.Sprintf(`{"v":%d,"round":%d}`, i, i/len(ids))); err != nil {
			return err
		}
	}
	head, err := a.PeerIngestEnv().DAG.Head()
	if err != nil {
		return err
	}
	if _, err := a.Commit(ns, document.Transaction{ID: newID(), BaseVersion: head,
		Operations: []document.Op{document.DeleteOp{DocID: ids[0]}}, Timestamp: codec.TimestampNow()}, "", auth.Principal{}); err != nil {
		return err
	}
	if err := upsert(a, newID(), `{"fresh":true}`); err != nil {
		return err
	}
	aHead, _ := a.PeerIngestEnv().DAG.Head()
	res, err := pull(peersync.CatchUpSquash)
	if err != nil {
		return err
	}
	if res.Squashed != aHead.Hex() {
		return fmt.Errorf("expected a squash to %s, got %+v", aHead.Hex(), res)
	}

	d := b.PeerIngestEnv().DAG
	if _, err := d.CreateBranch("at-squash", aHead); err != nil {
		return err
	}
	if _, err := d.CreateTag("squashed", aHead, "the squash root"); err != nil {
		return err
	}
	if err := upsert(b, ids[1], `{"local":"on top of the squash"}`); err != nil {
		return err
	}
	if _, _, err := fileRT.PutAttachment(ns, ids[1], bytes.NewReader(bytes.Repeat([]byte("go attachment "), 64)),
		embed.PutAttachmentOptions{Name: "go.txt", Mime: "text/plain"}); err != nil {
		return err
	}
	return nil
}

func newID() codec.UUID {
	id, err := codec.RandomUUID()
	if err != nil {
		panic(err)
	}
	return id
}
