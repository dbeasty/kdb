package embed

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Attachments: one binary attachment per document (docs/kdb-attachments-plan.md).
//
// The bytes live in the namespace's attachment store (storage/attachstore) - small ones packed in
// the ordinary blob store, larger ones as standalone segments - and a commit records which
// document has which bytes (document.AttachmentPutOp), atomically with whatever else it writes and
// versioned like everything else: an as-of read returns the attachment as of that commit.

// AttachmentHistory is how much of an attachment's history the engine keeps.
type AttachmentHistory int

const (
	// AttachmentHistoryAll keeps every version a retained commit references, so an as-of read
	// returns the bytes as of that commit. The default. A replaced version is re-stored as a delta
	// against its successor when that is much smaller (see CompactAttachments).
	AttachmentHistoryAll AttachmentHistory = iota
	// AttachmentHistoryLatest keeps only what branch and tag heads reference; a replaced or
	// deleted attachment's bytes go at the next maintenance pass, and an as-of read of them fails
	// with ErrAttachmentNotRetained.
	AttachmentHistoryLatest
)

// ErrNoAttachment reports a document with no attachment (at the commit asked about).
var ErrNoAttachment = errors.New("kdb: document has no attachment")

// ErrAttachmentNotRetained reports an attachment version a commit references whose bytes the
// engine no longer keeps (AttachmentHistoryLatest, or a history truncated past it).
var ErrAttachmentNotRetained = errors.New("kdb: attachment version is no longer retained")

// ErrNoSuchDocument reports an attachment written to a document that does not exist.
var ErrNoSuchDocument = errors.New("kdb: no such document")

// AttachmentInfo describes a document's attachment.
type AttachmentInfo struct {
	DocID       codec.UUID
	ContentHash codec.Hash
	Size        int64
	Mime        string
}

// attachmentState is a runtime's attachment stores, one per namespace, and its settings.
type attachmentState struct {
	mu      sync.Mutex
	stores  map[string]*attachstore.Store
	memShim *storio.InMemoryPlatformIO // for a runtime with no platform I/O of its own
	history AttachmentHistory
	config  attachstore.Config
	// replaced are (old, new) content hashes from replacing commits, for CompactAttachments to
	// try as deltas. Best effort: lost on restart, which only means those stay whole.
	replaced [][2]codec.Hash
}

// SetAttachmentHistory chooses how much attachment history this runtime keeps.
func (rt *EmbeddedKdbRuntime) SetAttachmentHistory(h AttachmentHistory) {
	rt.attach.mu.Lock()
	rt.attach.history = h
	rt.attach.mu.Unlock()
}

// SetAttachmentConfig tunes the attachment store (size limit, chunking, deltas). Takes effect for
// stores opened afterwards.
func (rt *EmbeddedKdbRuntime) SetAttachmentConfig(cfg attachstore.Config) {
	rt.attach.mu.Lock()
	rt.attach.config = cfg
	rt.attach.mu.Unlock()
}

// AttachmentStore returns namespaceID's attachment store, opening it on first use.
func (rt *EmbeddedKdbRuntime) AttachmentStore(namespaceID string) *attachstore.Store {
	rt.attach.mu.Lock()
	defer rt.attach.mu.Unlock()
	if s, ok := rt.attach.stores[namespaceID]; ok {
		return s
	}
	if rt.attach.stores == nil {
		rt.attach.stores = map[string]*attachstore.Store{}
	}
	shim := rt.shim
	if shim == nil {
		if rt.attach.memShim == nil {
			rt.attach.memShim = storio.NewInMemoryPlatformIO()
		}
		shim = rt.attach.memShim
	}
	s := attachstore.New(shim, namespaceID, rt.Storage, rt.attach.config)
	rt.attach.stores[namespaceID] = s
	if eng, ok := rt.Storage.(*engine.ServerEngine); ok {
		// Under history=none the blob store keeps only what the live tree names; attachment
		// blobs packed into it must count too.
		eng.SetExtraReachable(func() func(codec.Hash) bool {
			keep, err := rt.headAttachmentHashes(namespaceID)
			if err != nil {
				return func(codec.Hash) bool { return true } // unsure: keep everything
			}
			return func(h codec.Hash) bool { return keep[h] }
		})
	}
	return s
}

// attachmentTables is the DAG capability attachments need.
type attachmentTables interface {
	AttachmentTableAt(commit codec.Hash) (document.AttachmentTable, error)
}

func (rt *EmbeddedKdbRuntime) inMemoryDAG() (*dag.InMemoryCommitDag, error) {
	switch d := rt.DAG.(type) {
	case *dag.InMemoryCommitDag:
		return d, nil
	case *PersistingCommitDAG:
		return d.Delegate(), nil
	default:
		return nil, fmt.Errorf("kdb: attachments need the in-memory commit DAG, not %T", rt.DAG)
	}
}

// AttachmentTableAt returns the attachment table at commit.
func (rt *EmbeddedKdbRuntime) AttachmentTableAt(commit codec.Hash) (document.AttachmentTable, error) {
	d, err := rt.inMemoryDAG()
	if err != nil {
		return document.AttachmentTable{}, err
	}
	return d.AttachmentTableAt(commit)
}

// PutAttachment stores r's bytes as docID's attachment, of type mime, and commits it: the bytes
// are durable before the commit that references them is. Replacing an attachment leaves the
// document's JSON and content hash as they were. It returns the commit.
func (rt *EmbeddedKdbRuntime) PutAttachment(namespaceID string, docID codec.UUID, r io.Reader, mime string) (codec.Hash, AttachmentInfo, error) {
	if err := rt.AssertWritable(); err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	info, err := rt.AttachmentStore(namespaceID).Put(r)
	if err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	op := document.AttachmentPutOp{DocID: docID, ContentHash: info.Hash, Size: info.Size, Mime: mime}
	c, prev, err := rt.commitAttachmentOps(namespaceID, docID, []document.Op{op})
	if err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	if prev != nil && prev.ContentHash != info.Hash {
		rt.attach.mu.Lock()
		rt.attach.replaced = append(rt.attach.replaced, [2]codec.Hash{prev.ContentHash, info.Hash})
		rt.attach.mu.Unlock()
	}
	return c, AttachmentInfo{DocID: docID, ContentHash: info.Hash, Size: info.Size, Mime: mime}, nil
}

// DeleteAttachment removes docID's attachment, leaving the document. Removing one that is not
// there is ErrNoAttachment.
func (rt *EmbeddedKdbRuntime) DeleteAttachment(namespaceID string, docID codec.UUID) (codec.Hash, error) {
	if err := rt.AssertWritable(); err != nil {
		return codec.Hash{}, err
	}
	c, prev, err := rt.commitAttachmentOps(namespaceID, docID, []document.Op{document.AttachmentDeleteOp{DocID: docID}})
	if err != nil {
		return codec.Hash{}, err
	}
	if prev == nil {
		return c, ErrNoAttachment
	}
	return c, nil
}

// commitAttachmentOps appends a commit of ops on main's head, after checking docID exists there.
// It returns the commit and docID's attachment before it.
func (rt *EmbeddedKdbRuntime) commitAttachmentOps(namespaceID string, docID codec.UUID, ops []document.Op) (codec.Hash, *document.AttachmentRef, error) {
	head, err := rt.DAG.Head()
	if err != nil {
		return codec.Hash{}, nil, err
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return codec.Hash{}, nil, err
	}
	doc, err := rt.Storage.GetDocument(namespaceID, docID, commit.DocumentTreeHash)
	if err != nil {
		return codec.Hash{}, nil, err
	}
	if doc == nil {
		return codec.Hash{}, nil, fmt.Errorf("%w: %s", ErrNoSuchDocument, docID)
	}
	table, err := rt.AttachmentTableAt(head)
	if err != nil {
		return codec.Hash{}, nil, err
	}
	var prev *document.AttachmentRef
	if ref, ok := table.Get(docID); ok {
		prev = &ref
	}
	if _, isDelete := ops[0].(document.AttachmentDeleteOp); isDelete && prev == nil {
		return head, nil, nil // nothing to remove; no commit
	}
	tree, err := rt.Storage.CommitTree(namespaceID, commit.DocumentTreeHash)
	if err != nil {
		return codec.Hash{}, nil, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return codec.Hash{}, nil, err
	}
	author, err := codec.RandomUUID()
	if err != nil {
		return codec.Hash{}, nil, err
	}
	tx := document.Transaction{ID: txID, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	c, err := rt.DAG.AppendCommit(tx, head, tree, commit.SchemaHash, "")
	if err != nil {
		return codec.Hash{}, nil, err
	}
	return c.Hash, prev, nil
}

// GetAttachment opens docID's attachment as of commit asOf (zero: main's head). The caller closes
// the reader.
func (rt *EmbeddedKdbRuntime) GetAttachment(namespaceID string, docID codec.UUID, asOf codec.Hash) (io.ReadCloser, AttachmentInfo, error) {
	if asOf == (codec.Hash{}) {
		head, err := rt.DAG.Head()
		if err != nil {
			return nil, AttachmentInfo{}, err
		}
		asOf = head
	}
	info, err := rt.AttachmentInfoAt(namespaceID, docID, asOf)
	if err != nil {
		return nil, AttachmentInfo{}, err
	}
	r, _, err := rt.AttachmentStore(namespaceID).Open(info.ContentHash)
	if errors.Is(err, attachstore.ErrNotFound) {
		return nil, AttachmentInfo{}, fmt.Errorf("%w: %s of %s at %s", ErrAttachmentNotRetained, info.ContentHash.Hex(), docID, asOf.Hex())
	}
	if err != nil {
		return nil, AttachmentInfo{}, err
	}
	return r, info, nil
}

// AttachmentInfoAt describes docID's attachment at commit, or ErrNoAttachment.
func (rt *EmbeddedKdbRuntime) AttachmentInfoAt(namespaceID string, docID codec.UUID, commit codec.Hash) (AttachmentInfo, error) {
	table, err := rt.AttachmentTableAt(commit)
	if err != nil {
		return AttachmentInfo{}, err
	}
	ref, ok := table.Get(docID)
	if !ok {
		return AttachmentInfo{}, ErrNoAttachment
	}
	return AttachmentInfo{DocID: docID, ContentHash: ref.ContentHash, Size: ref.Size, Mime: ref.Mime}, nil
}

// headAttachmentHashes is every content hash a branch or tag head's attachment table references.
func (rt *EmbeddedKdbRuntime) headAttachmentHashes(namespaceID string) (map[codec.Hash]bool, error) {
	d, err := rt.inMemoryDAG()
	if err != nil {
		return nil, err
	}
	keep := map[codec.Hash]bool{}
	var heads []codec.Hash
	for _, b := range d.ListBranches() {
		heads = append(heads, b.HeadHash)
	}
	for _, t := range d.ListTags() {
		heads = append(heads, t.CommitHash)
	}
	for _, h := range heads {
		table, err := d.AttachmentTableAt(h)
		if err != nil {
			return nil, err
		}
		table.Walk(func(_ codec.UUID, r document.AttachmentRef) bool {
			keep[r.ContentHash] = true
			return true
		})
	}
	return keep, nil
}

// referencedAttachmentHashes is every content hash any commit reachable from a branch or tag
// references - what AttachmentHistoryAll keeps. It reads the operations only of commits whose
// attachment root differs from their first parent's.
func (rt *EmbeddedKdbRuntime) referencedAttachmentHashes(namespaceID string) (map[codec.Hash]bool, error) {
	keep, err := rt.headAttachmentHashes(namespaceID)
	if err != nil {
		return nil, err
	}
	d, err := rt.inMemoryDAG()
	if err != nil {
		return nil, err
	}
	var heads []codec.Hash
	for _, b := range d.ListBranches() {
		heads = append(heads, b.HeadHash)
	}
	for _, t := range d.ListTags() {
		heads = append(heads, t.CommitHash)
	}
	seen := map[codec.Hash]bool{}
	for _, head := range heads {
		for _, e := range d.Walk(head, nil, 1<<30) {
			full, ok := e.(dag.FullEntry)
			if !ok || seen[full.Commit.Hash] {
				continue
			}
			c := full.Commit
			seen[c.Hash] = true
			if c.AttachmentTreeHash == nil {
				continue
			}
			if len(c.ParentHashes) > 0 {
				if p, ok := d.GetCommit(c.ParentHashes[0]); ok && p.AttachmentTreeHash != nil && *p.AttachmentTreeHash == *c.AttachmentTreeHash {
					continue
				}
			}
			ops, err := d.CommitOperations(c.Hash)
			if err != nil {
				return nil, err
			}
			for _, op := range ops {
				if put, ok := op.(document.AttachmentPutOp); ok {
					keep[put.ContentHash] = true
				}
			}
		}
	}
	return keep, nil
}

// AttachmentMaintenance is what CompactAttachments did.
type AttachmentMaintenance struct {
	Deltas  int // replaced versions re-stored as deltas
	Removed int // blobs deleted
}

// CompactAttachments re-stores replaced versions as deltas (AttachmentHistoryAll) and deletes the
// standalone blobs nothing keeps: under AttachmentHistoryLatest anything a branch or tag head does
// not reference, under AttachmentHistoryAll anything no reachable commit references (an upload
// that was never committed). Maintain calls it; it is safe to call at any time.
func (rt *EmbeddedKdbRuntime) CompactAttachments(namespaceID string) (AttachmentMaintenance, error) {
	var out AttachmentMaintenance
	if rt.ReadOnly {
		return out, nil
	}
	rt.attach.mu.Lock()
	history := rt.attach.history
	replaced := rt.attach.replaced
	rt.attach.replaced = nil
	_, opened := rt.attach.stores[namespaceID]
	rt.attach.mu.Unlock()
	store := rt.AttachmentStore(namespaceID)
	if !opened {
		// Nothing written through this runtime yet; still collect what an earlier one left.
		if blobs, err := store.LargeBlobs(); err != nil || len(blobs) == 0 {
			return out, err
		}
	}
	var keep map[codec.Hash]bool
	var err error
	if history == AttachmentHistoryLatest {
		keep, err = rt.headAttachmentHashes(namespaceID)
	} else {
		keep, err = rt.referencedAttachmentHashes(namespaceID)
		for _, pair := range replaced {
			if !keep[pair[0]] || !keep[pair[1]] {
				continue
			}
			if did, err := store.StoreAsDelta(pair[0], pair[1]); err == nil && did {
				out.Deltas++
			}
		}
	}
	if err != nil {
		return out, err
	}
	out.Removed, err = store.Collect(func(h codec.Hash) bool { return keep[h] })
	return out, err
}
