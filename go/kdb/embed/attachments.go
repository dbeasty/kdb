package embed

import (
	"errors"
	"fmt"
	"github.com/limidus/kdb/go/kdb/storage"
	"io"
	"log"
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/attachstore"
	"github.com/limidus/kdb/go/kdb/storage/engine"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Attachments: binary attachments on documents (docs/kdb-attachments-plan.md). A document has any
// number; each has a permanent ID and a name unique within the document, and either one selects it
// (AttachmentSelector).
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

// ErrNoAttachment reports an attachment the document does not have (at the commit asked about).
var ErrNoAttachment = errors.New("kdb: no such attachment")

// ErrAttachmentNotRetained reports an attachment version a commit references whose bytes the
// engine no longer keeps (AttachmentHistoryLatest, or a history truncated past it).
var ErrAttachmentNotRetained = errors.New("kdb: attachment version is no longer retained")

// ErrNoSuchDocument reports an attachment written to a document that does not exist.
var ErrNoSuchDocument = errors.New("kdb: no such document")

// AttachmentInfo describes one attachment of a document.
type AttachmentInfo struct {
	DocID       codec.UUID
	ID          codec.UUID
	Name        string
	ContentHash codec.Hash
	Size        int64
	Mime        string
}

func infoOf(docID codec.UUID, a document.Attachment) AttachmentInfo {
	return AttachmentInfo{DocID: docID, ID: a.ID, Name: a.Name, ContentHash: a.ContentHash, Size: a.Size, Mime: a.Mime}
}

// AttachmentSelector picks one attachment of a document: by ID when ID is set, otherwise by Name.
type AttachmentSelector struct {
	ID   codec.UUID
	Name string
}

// AttachmentByID selects an attachment by its ID.
func AttachmentByID(id codec.UUID) AttachmentSelector { return AttachmentSelector{ID: id} }

// AttachmentByName selects an attachment by its name.
func AttachmentByName(name string) AttachmentSelector { return AttachmentSelector{Name: name} }

func (s AttachmentSelector) find(set document.AttachmentSet) (document.Attachment, bool) {
	if s.ID != (codec.UUID{}) {
		for _, a := range set {
			if a.ID == s.ID {
				return a, true
			}
		}
		return document.Attachment{}, false
	}
	return set.ByName(s.Name)
}

func (s AttachmentSelector) String() string {
	if s.ID != (codec.UUID{}) {
		return s.ID.String()
	}
	return fmt.Sprintf("%q", s.Name)
}

// PutAttachmentOptions says which attachment PutAttachment writes.
//
//   - ID set: that attachment - replaced if the document has it, created with that ID if not.
//   - Name set, no ID: the attachment of that name is replaced if there is one (keeping its ID);
//     otherwise a new attachment is created under the name, with a fresh ID.
//   - Neither: a new attachment, with a fresh ID and that ID as its name.
//
// Name, when set alongside an ID, renames the attachment; left empty it keeps its name (or, for a
// new attachment, takes the ID's string form).
type PutAttachmentOptions struct {
	ID   codec.UUID
	Name string
	Mime string
}

// attachmentState is a runtime's attachment stores, one per namespace, and its settings.
type attachmentState struct {
	mu      sync.Mutex
	stores  map[string]*attachstore.Store
	memShim *storio.InMemoryPlatformIO // for a runtime with no platform I/O of its own
	history AttachmentHistory
	config  attachstore.Config
}

// SetAttachmentHistory chooses how much attachment history this runtime keeps.
func (rt *EmbeddedKdbRuntime) SetAttachmentHistory(h AttachmentHistory) {
	rt.attach.mu.Lock()
	rt.attach.history = h
	rt.attach.mu.Unlock()
}

// SetAttachmentConfig tunes the attachment store (size limit, chunking, deltas, upload grace). It
// replaces the whole configuration, StorageOptions.AttachmentUploadGrace and
// AttachmentDeltaStepBytes and DisableAttachmentDeltas included. Takes effect for
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
	var small attachstore.SmallBlobs
	if sb, ok := rt.Storage.(attachstore.SmallBlobs); ok {
		small = sb
	}
	s := attachstore.New(shim, namespaceID, small, rt.attach.config)
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

// registerAttachmentBlobs lets the transaction engine's preflight find this runtime's attachment
// blobs (large ones live outside the storage adapter's blob store), for every commit path - the
// server's, the driver's - not only PutAttachment's. Undone when the runtime closes.
func (rt *EmbeddedKdbRuntime) registerAttachmentBlobs() {
	store := rt.Storage
	transaction.RegisterAttachmentBlobs(store, rt)
	// Through the address only: the hook must not keep rt reachable either.
	rt.AddCloseHook(func() { transaction.RegisterAttachmentBlobs[EmbeddedKdbRuntime](store, nil) })
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

// PutAttachment stores r's bytes as one of docID's attachments - which one, opts says - and commits
// it: the bytes are durable before the commit that references them is. The document's JSON, its
// content hash and its other attachments are untouched. It returns the commit and the attachment.
func (rt *EmbeddedKdbRuntime) PutAttachment(namespaceID string, docID codec.UUID, r io.Reader, opts PutAttachmentOptions) (codec.Hash, AttachmentInfo, error) {
	if err := rt.AssertWritable(); err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	if opts.Name != "" {
		if err := document.ValidateAttachmentName(opts.Name); err != nil {
			return codec.Hash{}, AttachmentInfo{}, err
		}
	}
	info, err := rt.AttachmentStore(namespaceID).Put(r)
	if err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	ref := document.AttachmentRef{Scheme: document.AttachmentHashSchemeChunked1, ContentHash: info.Hash, Size: info.Size, Mime: opts.Mime}
	return rt.putAttachmentRef(namespaceID, docID, opts.ID, opts.Name, ref)
}

// RenameAttachment gives one of docID's attachments a new name, keeping its ID and bytes.
func (rt *EmbeddedKdbRuntime) RenameAttachment(namespaceID string, docID codec.UUID, sel AttachmentSelector, newName string) (codec.Hash, AttachmentInfo, error) {
	if err := rt.AssertWritable(); err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	if err := document.ValidateAttachmentName(newName); err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	table, err := rt.AttachmentTableAt(head)
	if err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	a, ok := sel.find(table.List(docID))
	if !ok {
		return codec.Hash{}, AttachmentInfo{}, fmt.Errorf("%w: %s of %s", ErrNoAttachment, sel, docID)
	}
	return rt.putAttachmentRef(namespaceID, docID, a.ID, newName, a.AttachmentRef)
}

// putAttachmentRef resolves which attachment to write (see PutAttachmentOptions) and commits it.
func (rt *EmbeddedKdbRuntime) putAttachmentRef(namespaceID string, docID, id codec.UUID, name string, ref document.AttachmentRef) (codec.Hash, AttachmentInfo, error) {
	var put document.AttachmentPutOp
	c, err := rt.commitAttachmentOps(namespaceID, docID, func(set document.AttachmentSet) ([]document.Op, error) {
		var existing document.Attachment
		var found bool
		switch {
		case id != (codec.UUID{}):
			existing, found = AttachmentByID(id).find(set)
		case name != "":
			existing, found = set.ByName(name)
		}
		attID, attName := id, name
		if found {
			attID = existing.ID
			if attName == "" {
				attName = existing.Name
			}
		}
		if attID == (codec.UUID{}) {
			fresh, err := codec.RandomUUID()
			if err != nil {
				return nil, err
			}
			attID = fresh
		}
		if attName == "" {
			attName = attID.String()
		}
		put = document.AttachmentPutOp{DocID: docID, AttachmentID: attID, Name: attName,
			Scheme: ref.Scheme, ContentHash: ref.ContentHash, Size: ref.Size, Mime: ref.Mime}
		return []document.Op{put}, nil
	})
	if err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	return c, AttachmentInfo{DocID: docID, ID: put.AttachmentID, Name: put.Name,
		ContentHash: put.ContentHash, Size: put.Size, Mime: put.Mime}, nil
}

// DeleteAttachment removes one of docID's attachments, leaving the document and its others.
// Removing one that is not there is ErrNoAttachment.
func (rt *EmbeddedKdbRuntime) DeleteAttachment(namespaceID string, docID codec.UUID, sel AttachmentSelector) (codec.Hash, error) {
	if err := rt.AssertWritable(); err != nil {
		return codec.Hash{}, err
	}
	return rt.commitAttachmentOps(namespaceID, docID, func(set document.AttachmentSet) ([]document.Op, error) {
		a, ok := sel.find(set)
		if !ok {
			return nil, fmt.Errorf("%w: %s of %s", ErrNoAttachment, sel, docID)
		}
		return []document.Op{document.AttachmentDeleteOp{DocID: docID, AttachmentID: a.ID}}, nil
	})
}

// commitAttachmentOps appends a commit on main's head of the ops plan derives from docID's
// attachments there, after checking docID exists.
func (rt *EmbeddedKdbRuntime) commitAttachmentOps(namespaceID string, docID codec.UUID, plan func(document.AttachmentSet) ([]document.Op, error)) (codec.Hash, error) {
	head, err := rt.DAG.Head()
	if err != nil {
		return codec.Hash{}, err
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return codec.Hash{}, err
	}
	doc, err := rt.Storage.GetDocument(namespaceID, docID, commit.DocumentTreeHash)
	if err != nil {
		return codec.Hash{}, err
	}
	if doc == nil {
		return codec.Hash{}, fmt.Errorf("%w: %s", ErrNoSuchDocument, docID)
	}
	table, err := rt.AttachmentTableAt(head)
	if err != nil {
		return codec.Hash{}, err
	}
	ops, err := plan(table.List(docID))
	if err != nil {
		return codec.Hash{}, err
	}
	// The table checks what it can before anything is committed (a name another attachment has).
	if _, err := table.Apply(ops); err != nil {
		return codec.Hash{}, err
	}
	tree, err := rt.Storage.CommitTree(namespaceID, commit.DocumentTreeHash)
	if err != nil {
		return codec.Hash{}, err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return codec.Hash{}, err
	}
	author, err := codec.RandomUUID()
	if err != nil {
		return codec.Hash{}, err
	}
	tx := document.Transaction{ID: txID, BaseVersion: head, Operations: ops, Timestamp: codec.TimestampNow(), AuthorNodeID: author}
	c, err := rt.DAG.AppendCommit(tx, head, tree, commit.SchemaHash, "")
	if err != nil {
		return codec.Hash{}, err
	}
	return c.Hash, nil
}

// GetAttachment opens one of docID's attachments as of commit asOf (zero: main's head). The reader
// streams the bytes chunk by chunk, each checked against the content hash, and is pinned to this
// version: commits, delta compaction and GC meanwhile do not disturb it. The caller closes it.
func (rt *EmbeddedKdbRuntime) GetAttachment(namespaceID string, docID codec.UUID, sel AttachmentSelector, asOf codec.Hash) (*attachstore.Reader, AttachmentInfo, error) {
	info, err := rt.AttachmentInfoAt(namespaceID, docID, sel, asOf)
	if err != nil {
		return nil, AttachmentInfo{}, err
	}
	r, err := rt.AttachmentStore(namespaceID).Open(info.ContentHash)
	if errors.Is(err, attachstore.ErrNotFound) {
		return nil, AttachmentInfo{}, fmt.Errorf("%w: %s of %s", ErrAttachmentNotRetained, info.ContentHash.Hex(), docID)
	}
	if err != nil {
		return nil, AttachmentInfo{}, err
	}
	return r, info, nil
}

// AttachmentInfoAt describes one of docID's attachments at commit (zero: main's head), or
// ErrNoAttachment.
func (rt *EmbeddedKdbRuntime) AttachmentInfoAt(namespaceID string, docID codec.UUID, sel AttachmentSelector, commit codec.Hash) (AttachmentInfo, error) {
	set, err := rt.attachmentSetAt(docID, commit)
	if err != nil {
		return AttachmentInfo{}, err
	}
	a, ok := sel.find(set)
	if !ok {
		return AttachmentInfo{}, fmt.Errorf("%w: %s of %s", ErrNoAttachment, sel, docID)
	}
	return infoOf(docID, a), nil
}

// ListAttachments lists docID's attachments at commit (zero: main's head), sorted by name.
func (rt *EmbeddedKdbRuntime) ListAttachments(namespaceID string, docID codec.UUID, commit codec.Hash) ([]AttachmentInfo, error) {
	set, err := rt.attachmentSetAt(docID, commit)
	if err != nil {
		return nil, err
	}
	out := make([]AttachmentInfo, len(set))
	for i, a := range set {
		out[i] = infoOf(docID, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// CountAttachments is how many attachments docID has at commit (zero: main's head).
func (rt *EmbeddedKdbRuntime) CountAttachments(namespaceID string, docID codec.UUID, commit codec.Hash) (int, error) {
	set, err := rt.attachmentSetAt(docID, commit)
	return len(set), err
}

func (rt *EmbeddedKdbRuntime) attachmentSetAt(docID codec.UUID, commit codec.Hash) (document.AttachmentSet, error) {
	if commit == (codec.Hash{}) {
		head, err := rt.DAG.Head()
		if err != nil {
			return nil, err
		}
		commit = head
	}
	table, err := rt.AttachmentTableAt(commit)
	if err != nil {
		return nil, err
	}
	return table.List(docID), nil
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
		table.Walk(func(_ codec.UUID, a document.Attachment) bool {
			keep[a.ContentHash] = true
			return true
		})
	}
	return keep, nil
}

// attachmentChange is a commit whose attachment root differs from its first parent's, and the
// attachment puts it carries.
type attachmentChange struct {
	commit document.Commit
	puts   []document.AttachmentPutOp
}

// referencedAttachmentHashes is every content hash any commit reachable from a branch or tag
// references - what AttachmentHistoryAll keeps. It reads the operations only of commits whose
// attachment root differs from their first parent's. It also returns the heads it walked from
// and, oldest first, the changing commits wanted accepts (none when wanted is nil).
func (rt *EmbeddedKdbRuntime) referencedAttachmentHashes(namespaceID string, wanted func(codec.Hash) bool) (map[codec.Hash]bool, []codec.Hash, []attachmentChange, error) {
	keep, err := rt.headAttachmentHashes(namespaceID)
	if err != nil {
		return nil, nil, nil, err
	}
	d, err := rt.inMemoryDAG()
	if err != nil {
		return nil, nil, nil, err
	}
	var heads []codec.Hash
	for _, b := range d.ListBranches() {
		heads = append(heads, b.HeadHash)
	}
	for _, t := range d.ListTags() {
		heads = append(heads, t.CommitHash)
	}
	var changes []attachmentChange
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
				return nil, nil, nil, err
			}
			change := attachmentChange{commit: c}
			for _, op := range ops {
				if put, ok := op.(document.AttachmentPutOp); ok {
					keep[put.ContentHash] = true
					change.puts = append(change.puts, put)
				}
			}
			if wanted != nil && len(change.puts) > 0 && wanted(c.Hash) {
				changes = append(changes, change)
			}
		}
	}
	// Walk goes newest first; a parent's table is then usually cached when its child's is asked.
	for i, j := 0, len(changes)-1; i < j; i, j = i+1, j-1 {
		changes[i], changes[j] = changes[j], changes[i]
	}
	return keep, heads, changes, nil
}

// AttachmentMaintenance is what CompactAttachments did.
type AttachmentMaintenance struct {
	Deltas  int // replaced versions re-stored as deltas
	Removed int // blobs deleted
	// DeltaPending: the pass spent its delta budget (attachstore.Config.DeltaStepBytes) before
	// the replaced versions were all done; the next pass goes on where this one stopped.
	DeltaPending bool
}

// CompactAttachments re-stores replaced versions as deltas (AttachmentHistoryAll, unless
// attachstore.Config.DisableDeltas keeps them whole) and deletes the
// standalone blobs nothing keeps: under AttachmentHistoryLatest anything a branch or tag head does
// not reference, under AttachmentHistoryAll anything no reachable commit references (an upload
// that was never committed). Maintain calls it; it is safe to call at any time.
//
// Replacements are read from the DAG, so every way a commit arrives counts - PutAttachment, a
// transaction from a client or SQL, peer sync: a commit that puts an attachment whose first parent
// had a different version of it (same document, same attachment ID) makes the pair (old, new), and
// old is re-stored as a delta against new. The store's mark records the heads each pass went
// through, so the next one, after a restart too, looks only at commits since.
func (rt *EmbeddedKdbRuntime) CompactAttachments(namespaceID string) (AttachmentMaintenance, error) {
	var out AttachmentMaintenance
	if rt.ReadOnly {
		return out, nil
	}
	rt.attach.mu.Lock()
	history := rt.attach.history
	_, opened := rt.attach.stores[namespaceID]
	rt.attach.mu.Unlock()
	store := rt.AttachmentStore(namespaceID)
	if !opened {
		// Nothing written through this runtime yet; still collect (and delta) what an earlier
		// one or another commit path left.
		if blobs, err := store.LargeBlobs(); err != nil || len(blobs) == 0 {
			return out, err
		}
	}
	if history == AttachmentHistoryLatest {
		keep, err := rt.headAttachmentHashes(namespaceID)
		if err != nil {
			return out, err
		}
		out.Removed, err = store.Collect(func(h codec.Hash) bool { return keep[h] })
		return out, err
	}
	if store.Config().DisableDeltas {
		// Every version stays whole. The mark stays where it is, so turning deltas back on
		// diffs the versions replaced meanwhile.
		keep, _, _, err := rt.referencedAttachmentHashes(namespaceID, nil)
		if err != nil {
			return out, err
		}
		out.Removed, err = store.Collect(func(h codec.Hash) bool { return keep[h] })
		return out, err
	}
	d, err := rt.inMemoryDAG()
	if err != nil {
		return out, err
	}
	mark, err := store.Mark()
	if err != nil {
		return out, err
	}
	var known []codec.Hash
	for _, h := range mark {
		if d.HasCommit(h) {
			known = append(known, h)
		}
	}
	done := map[codec.Hash]struct{}{}
	if len(known) > 0 {
		done = d.AncestorSetOf(known)
	}
	keep, heads, changes, err := rt.referencedAttachmentHashes(namespaceID, func(h codec.Hash) bool {
		_, ok := done[h]
		return !ok
	})
	if err != nil {
		return out, err
	}
	deltaErr := rt.deltaReplaced(d, store, keep, changes, &out)
	if deltaErr == nil && len(changes) > 0 && !out.DeltaPending {
		deltaErr = store.SetMark(heads)
	}
	out.Removed, err = store.Collect(func(h codec.Hash) bool { return keep[h] })
	if deltaErr != nil {
		return out, deltaErr
	}
	return out, err
}

// deltaReplaced re-stores, for each put in changes, the version it replaced in its commit's first
// parent as a delta against it, when both are kept. A version gone missing or corrupt is left
// alone; any other failure stops the pass, so the mark is not moved past work not done. So does
// spending the pass's budget (attachstore.Config.DeltaStepBytes): the delta in progress carries
// over, and the next pass, finding the same changes, finishes it - a multi-GB version is diffed
// over several passes, none of them long.
func (rt *EmbeddedKdbRuntime) deltaReplaced(d *dag.InMemoryCommitDag, store *attachstore.Store, keep map[codec.Hash]bool, changes []attachmentChange, out *AttachmentMaintenance) error {
	budget := store.Config().DeltaStepBytes
	for _, ch := range changes {
		if len(ch.commit.ParentHashes) == 0 {
			continue
		}
		parent, err := d.AttachmentTableAt(ch.commit.ParentHashes[0])
		if err != nil {
			continue // a shallow root's parent: nothing here to delta against
		}
		for _, put := range ch.puts {
			prev, ok := parent.Get(put.DocID, put.AttachmentID)
			if !ok || prev.ContentHash == put.ContentHash || !keep[prev.ContentHash] || !keep[put.ContentHash] {
				continue
			}
			if budget <= 0 {
				out.DeltaPending = true
				return nil
			}
			did, used, err := store.DeltaStep(prev.ContentHash, put.ContentHash, budget)
			budget -= used
			if errors.Is(err, attachstore.ErrNotFound) || errors.Is(err, attachstore.ErrCorrupt) {
				continue
			}
			if err != nil {
				return err
			}
			switch did {
			case attachstore.DeltaStored:
				out.Deltas++
			case attachstore.DeltaPending:
				out.DeltaPending = true
				return nil
			}
		}
	}
	return nil
}

// attachmentsCommitted ends the upload hold (attachstore.Config.UploadGrace) on the blobs a
// durable commit references: from here on its references keep them.
func (rt *EmbeddedKdbRuntime) attachmentsCommitted(c document.Commit) {
	ns := c.NamespaceID
	if ns == "" {
		ns = rt.DefaultNamespace
	}
	var hashes []codec.Hash
	for _, op := range c.Operations {
		if put, ok := op.(document.AttachmentPutOp); ok {
			hashes = append(hashes, put.ContentHash)
		}
	}
	if err := rt.AttachmentStore(ns).Committed(hashes...); err != nil {
		log.Printf("kdb: namespace %s: attachment store: %v", ns, err)
	}
}

// PutAttachmentBlob stores r's bytes in namespaceID's attachment store, durably, without committing
// anything: a transaction that references them commits separately (SQL, the wire upload).
func (rt *EmbeddedKdbRuntime) PutAttachmentBlob(namespaceID string, r io.Reader) (codec.Hash, int64, error) {
	if err := rt.AssertWritable(); err != nil {
		return codec.Hash{}, 0, err
	}
	info, err := rt.AttachmentStore(namespaceID).Put(r)
	if err != nil {
		return codec.Hash{}, 0, err
	}
	return info.Hash, info.Size, nil
}

// HasAttachmentBlob reports whether namespaceID's attachment store holds h.
func (rt *EmbeddedKdbRuntime) HasAttachmentBlob(namespaceID string, h codec.Hash) (bool, error) {
	return rt.AttachmentStore(namespaceID).Has(h)
}

// ReadAttachmentBlob reads a whole blob, failing if it is over limit bytes.
func (rt *EmbeddedKdbRuntime) ReadAttachmentBlob(namespaceID string, h codec.Hash, limit int64) ([]byte, error) {
	return rt.AttachmentStore(namespaceID).ReadAll(h, limit)
}

func attachmentSeedsKey(namespaceID string) string { return "kdb:attachment-seeds:" + namespaceID }

// PersistAttachmentSeed makes an attachment table peer sync seeded (packed: dag.PackAttachmentTables)
// durable, for a commit this node holds no history below - a graft's or a boundary extension's root
// - whose table no replay could derive. Kept with the namespace's earlier seeds, deduplicated by
// root, and seeded again on every open.
func (r *EmbeddedKdbRuntime) PersistAttachmentSeed(packed []byte) error {
	if r.shim == nil || len(packed) == 0 {
		return nil
	}
	key := attachmentSeedsKey(r.DefaultNamespace)
	tables, err := document.ParsePackedAttachmentTables(packed)
	if err != nil {
		return err
	}
	if prev, err := r.shim.ReadSnapshot(key); err == nil && len(prev) > 0 {
		old, err := document.ParsePackedAttachmentTables(prev)
		if err != nil {
			return err
		}
		tables = append(old, tables...)
	}
	seen := map[codec.Hash]bool{}
	kept := tables[:0]
	for _, t := range tables {
		if root := t.Root(); root != nil && !seen[*root] {
			seen[*root] = true
			kept = append(kept, t)
		}
	}
	return r.shim.WriteSnapshot(key, document.AppendPackedAttachmentTables(nil, kept))
}

func seedPersistedAttachmentTables(shim storage.PlatformIOShim, d *dag.InMemoryCommitDag, namespaceID string) error {
	if shim == nil {
		return nil
	}
	raw, err := shim.ReadSnapshot(attachmentSeedsKey(namespaceID))
	if err != nil || len(raw) == 0 {
		return nil // none recorded
	}
	if err := d.SeedPackedAttachmentTables(raw); err != nil {
		return fmt.Errorf("kdb: namespace %s: persisted attachment tables: %w", namespaceID, err)
	}
	return nil
}
