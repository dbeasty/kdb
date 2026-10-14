package embed

import (
	"bytes"
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
// replaces the whole configuration, StorageOptions.AttachmentUploadGrace included. Takes effect for
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
	rt.installAttachmentReachability(namespaceID)
	return s
}

// installAttachmentReachability tells the engine's blob store, which under history=none keeps
// only what the live tree names, what else it must keep: attachment blobs packed into it, and the
// attachment sets the DAG's set pool spilled into it (engineSetStore).
func (rt *EmbeddedKdbRuntime) installAttachmentReachability(namespaceID string) {
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return
	}
	eng.SetExtraReachable(func() func(codec.Hash) bool {
		var written map[codec.Hash]struct{}
		d, err := rt.inMemoryDAG()
		if err == nil {
			pool := d.AttachmentSetPool()
			pool.TrackWritten(true)
			written = pool.TakeWritten()
		}
		keep, err := rt.headAttachmentHashes(namespaceID, true)
		if err != nil {
			return func(codec.Hash) bool { return true } // unsure: keep everything
		}
		if d != nil {
			// A cached table of an older commit may name sets about to go; asked again, it is
			// rebuilt from the heads' and seeds' tables kept here.
			d.ForgetAttachmentTablesExcept(branchAndTagHeads(d))
		}
		return func(h codec.Hash) bool {
			if keep[h] {
				return true
			}
			_, w := written[h]
			return w
		}
	})
}

// engineSetStore keeps the attachment sets a DAG's pool evicts in the namespace's blob store,
// unlogged: a set is rebuilt by whatever derived it, so a crash may lose the unflushed ones.
type engineSetStore struct{ eng *engine.ServerEngine }

func (s engineSetStore) WriteAttachmentSet(h codec.Hash, encoded []byte) error {
	return s.eng.WriteUnloggedBlob(h, encoded)
}

func (s engineSetStore) ReadAttachmentSet(h codec.Hash) ([]byte, bool, error) {
	b, err := s.eng.ReadBlob(h)
	return b, b != nil, err
}

// useAttachmentSetStore bounds d's attachment set pool by spilling to eng's blob store, when the
// engine writes one to disk and may (not read-only).
func useAttachmentSetStore(d *dag.InMemoryCommitDag, eng *engine.ServerEngine, readOnly bool, budget int64) error {
	if eng == nil || readOnly || !eng.SpillsBlobs() {
		return nil
	}
	if err := d.SetAttachmentSetStore(engineSetStore{eng}, budget); err != nil {
		return err
	}
	if eng.HistoryMode() == storage.HistoryModeNone {
		d.AttachmentSetPool().TrackWritten(true)
	}
	return nil
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
	set, err := table.List(docID)
	if err != nil {
		return codec.Hash{}, AttachmentInfo{}, err
	}
	a, ok := sel.find(set)
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
	set, err := table.List(docID)
	if err != nil {
		return codec.Hash{}, err
	}
	ops, err := plan(set)
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
	return table.List(docID)
}

// branchAndTagHeads are the commits garbage collection keeps history from, sorted and distinct.
func branchAndTagHeads(d *dag.InMemoryCommitDag) []codec.Hash {
	var heads []codec.Hash
	for _, b := range d.ListBranches() {
		heads = append(heads, b.HeadHash)
	}
	for _, t := range d.ListTags() {
		heads = append(heads, t.CommitHash)
	}
	return attachstore.SortHashes(heads)
}

// headAttachmentHashes is every content hash a branch or tag head's attachment table references.
// With sets it also holds the hashes of those tables' attachment sets and of the seeded tables'
// sets: what a store collecting attachment sets must keep (attachmentSetRule).
func (rt *EmbeddedKdbRuntime) headAttachmentHashes(namespaceID string, sets bool) (map[codec.Hash]bool, error) {
	d, err := rt.inMemoryDAG()
	if err != nil {
		return nil, err
	}
	keep := map[codec.Hash]bool{}
	for _, h := range branchAndTagHeads(d) {
		table, err := d.AttachmentTableAt(h)
		if err != nil {
			return nil, err
		}
		if sets {
			table.WalkDocuments(func(_ codec.UUID, set codec.Hash) bool {
				keep[set] = true
				return true
			})
		}
		if err := table.Walk(func(_ codec.UUID, a document.Attachment) bool {
			keep[a.ContentHash] = true
			return true
		}); err != nil {
			return nil, err
		}
	}
	if sets {
		for _, t := range d.PinnedAttachmentTables() {
			t.WalkDocuments(func(_ codec.UUID, set codec.Hash) bool {
				keep[set] = true
				return true
			})
		}
	}
	return keep, nil
}

// attachmentChange is a commit whose attachment root differs from its first parent's, and the
// attachment puts it carries.
type attachmentChange struct {
	commit document.Commit
	puts   []document.AttachmentPutOp
}

// attachmentReferences is every content hash a commit reachable from heads references - what
// AttachmentHistoryAll keeps - as the store's reference index brought up to date (attachstore
// RefIndex). When the recorded index still describes the history below its heads (the heads are
// below the current ones and the history floor is the same) only the commits above them are read,
// and of those only the operations of commits whose attachment root differs from their first
// parent's; otherwise all of retained history is read once and the index rebuilt. A commit whose
// history is not here (a shallow root, a truncated floor) contributes its whole table, which is
// where its references came from.
//
// It also returns, oldest first, the changing commits read that wanted accepts (none when wanted
// is nil).
func (rt *EmbeddedKdbRuntime) attachmentReferences(d *dag.InMemoryCommitDag, store *attachstore.Store, heads []codec.Hash, wanted func(codec.Hash) bool, out *AttachmentMaintenance) (attachstore.RefIndex, []attachmentChange, error) {
	floor := d.HistoryFloor()
	prev, ok, err := store.RefIndex()
	if err != nil {
		return attachstore.RefIndex{}, nil, err
	}
	incremental := ok && prev.Floor == floor && stillBelow(d, prev.Heads, heads)
	var stops, refs []codec.Hash
	if incremental {
		stops, refs = prev.Heads, prev.Refs
	}
	var added []codec.Hash
	var changes []attachmentChange
	fallback := false
	above := d.CommitsAbove(heads, stops)
	out.Scanned, out.Rebuilt = len(above), !incremental
	for _, h := range above {
		c, ok := d.GetCommit(h)
		if !ok || c.AttachmentTreeHash == nil {
			continue
		}
		if len(c.ParentHashes) > 0 {
			p, ok := d.GetCommit(c.ParentHashes[0])
			if !ok {
				table, err := d.AttachmentTableAt(h)
				if err != nil {
					// Nothing here can say what it holds: keep what the heads reference, as a
					// walk of the heads alone would.
					fallback = true
					continue
				}
				if err := table.Walk(func(_ codec.UUID, a document.Attachment) bool {
					added = append(added, a.ContentHash)
					return true
				}); err != nil {
					return attachstore.RefIndex{}, nil, err
				}
				continue
			}
			if p.AttachmentTreeHash != nil && *p.AttachmentTreeHash == *c.AttachmentTreeHash {
				continue
			}
		}
		ops, err := d.CommitOperations(h)
		if err != nil {
			return attachstore.RefIndex{}, nil, err
		}
		change := attachmentChange{commit: c}
		for _, op := range ops {
			if put, ok := op.(document.AttachmentPutOp); ok {
				added = append(added, put.ContentHash)
				change.puts = append(change.puts, put)
			}
		}
		if wanted != nil && len(change.puts) > 0 && wanted(h) {
			changes = append(changes, change)
		}
	}
	if fallback {
		keep, err := rt.headAttachmentHashes(d.NamespaceID, false)
		if err != nil {
			return attachstore.RefIndex{}, nil, err
		}
		for h := range keep {
			added = append(added, h)
		}
	}
	ix := attachstore.RefIndex{Heads: heads, Floor: floor, Refs: mergeHashes(refs, attachstore.SortHashes(added))}
	if !incremental || len(added) > 0 || !sameHashes(prev.Heads, heads) {
		if err := store.SetRefIndex(ix); err != nil {
			// The index in hand is right; only the next pass is slower.
			log.Printf("kdb: namespace %s: attachment reference index: %v", d.NamespaceID, err)
		}
	}
	// CommitsAbove goes newest first; a parent's table is then usually cached when its child's is
	// asked.
	for i, j := 0, len(changes)-1; i < j; i, j = i+1, j-1 {
		changes[i], changes[j] = changes[j], changes[i]
	}
	return ix, changes, nil
}

// stillBelow reports whether every commit of old is still here and at or below one of heads - so
// the history below old is still part of what heads keep, and nothing was taken from it.
func stillBelow(d *dag.InMemoryCommitDag, old, heads []codec.Hash) bool {
	tests := make([]func(codec.Hash) bool, len(heads))
	for i, h := range heads {
		tests[i] = d.AncestorTest(h)
	}
	for _, o := range old {
		if !d.HasCommit(o) {
			return false
		}
		below := false
		for _, t := range tests {
			if t(o) {
				below = true
				break
			}
		}
		if !below {
			return false
		}
	}
	return true
}

func sameHashes(a, b []codec.Hash) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// mergeHashes merges two sorted, distinct lists into one.
func mergeHashes(a, b []codec.Hash) []codec.Hash {
	if len(b) == 0 {
		return a
	}
	out := make([]codec.Hash, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch c := bytes.Compare(a[i].Bytes[:], b[j].Bytes[:]); {
		case c < 0:
			out = append(out, a[i])
			i++
		case c > 0:
			out = append(out, b[j])
			j++
		default:
			out = append(out, a[i])
			i, j = i+1, j+1
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// AttachmentMaintenance is what CompactAttachments did.
type AttachmentMaintenance struct {
	Deltas  int // replaced versions re-stored as deltas
	Removed int // blobs deleted
	// Under AttachmentHistoryAll: the commits read for the references they add (only those since
	// the last pass, unless the reference index was rebuilt).
	Scanned int
	Rebuilt bool // the reference index was built from all of retained history
}

// AttachmentSetPoolContains reports whether the attachment set pool holds h in memory.
func (rt *EmbeddedKdbRuntime) AttachmentSetPoolContains(h codec.Hash) bool {
	d, err := rt.inMemoryDAG()
	return err == nil && d.AttachmentSetPool().Contains(h)
}

// AttachmentSetPoolStats describes the attachment set pool of the runtime's DAG.
func (rt *EmbeddedKdbRuntime) AttachmentSetPoolStats() document.AttachmentSetPoolStats {
	d, err := rt.inMemoryDAG()
	if err != nil {
		return document.AttachmentSetPoolStats{}
	}
	return d.AttachmentSetPool().Stats()
}

// CompactAttachments re-stores replaced versions as deltas (AttachmentHistoryAll) and deletes the
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
		keep, err := rt.headAttachmentHashes(namespaceID, false)
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
	heads := branchAndTagHeads(d)
	// Deltas are wanted for commits the last pass that moved the mark did not go through. Usually
	// the mark is the index's heads, and every commit read is new to both.
	var known []codec.Hash
	for _, h := range mark {
		if d.HasCommit(h) {
			known = append(known, h)
		}
	}
	var markTests []func(codec.Hash) bool
	for _, m := range known {
		markTests = append(markTests, d.AncestorTest(m))
	}
	refs, changes, err := rt.attachmentReferences(d, store, heads, func(h codec.Hash) bool {
		for _, below := range markTests {
			if below(h) {
				return false
			}
		}
		return true
	}, &out)
	if err != nil {
		return out, err
	}
	keep := func(h codec.Hash) bool { return refs.Has(h) }
	deltaErr := rt.deltaReplaced(d, store, keep, changes, &out)
	if deltaErr == nil && len(changes) > 0 {
		deltaErr = store.SetMark(heads)
	}
	out.Removed, err = store.Collect(keep)
	if deltaErr != nil {
		return out, deltaErr
	}
	return out, err
}

// deltaReplaced re-stores, for each put in changes, the version it replaced in its commit's first
// parent as a delta against it, when both are kept. A version gone missing or corrupt is left
// alone; any other failure stops the pass, so the mark is not moved past work not done.
func (rt *EmbeddedKdbRuntime) deltaReplaced(d *dag.InMemoryCommitDag, store *attachstore.Store, keep func(codec.Hash) bool, changes []attachmentChange, out *AttachmentMaintenance) error {
	for _, ch := range changes {
		if len(ch.commit.ParentHashes) == 0 {
			continue
		}
		parent, err := d.AttachmentTableAt(ch.commit.ParentHashes[0])
		if err != nil {
			continue // a shallow root's parent: nothing here to delta against
		}
		for _, put := range ch.puts {
			prev, ok, err := parent.Get(put.DocID, put.AttachmentID)
			if err != nil {
				return err
			}
			if !ok || prev.ContentHash == put.ContentHash || !keep(prev.ContentHash) || !keep(put.ContentHash) {
				continue
			}
			did, err := store.StoreAsDelta(prev.ContentHash, put.ContentHash)
			if errors.Is(err, attachstore.ErrNotFound) || errors.Is(err, attachstore.ErrCorrupt) {
				continue
			}
			if err != nil {
				return err
			}
			if did {
				out.Deltas++
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
	out, err := document.AppendPackedAttachmentTables(nil, kept)
	if err != nil {
		return err
	}
	return r.shim.WriteSnapshot(key, out)
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
