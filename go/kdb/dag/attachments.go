package dag

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// Attachment tables (docs/kdb-attachments-plan.md).
//
// Every commit records the root of its namespace's attachment table (Commit.AttachmentTreeHash,
// nil for an empty table). The DAG derives it: a commit's table is its first parent's table with
// the commit's operations applied (document.AttachmentTable.Apply), so every way of appending a
// commit - a transaction, an embedded put, a revert, a merge whose operations carry the merged
// branch's net effect - records the right root without its caller computing anything.
//
// Tables are cached by root. A table not in the cache is rebuilt from the nearest ancestor whose
// table is (or whose root is nil, meaning empty), applying operations forward and checking each
// step against the root the commit recorded: a log or cache that disagrees with a commit is an
// error, never a wrong attachment. A checkpoint seeds the cache with its heads' tables
// (SeedAttachmentTable), so opening a namespace does not replay history to find them.

// attachmentCacheSize bounds the tables kept besides the ones seeded for heads. Versions of a table
// share their trie nodes, so a cached table costs its changed nodes, not its size.
const attachmentCacheSize = 64

type attachmentCache struct {
	tables map[codec.Hash]document.AttachmentTable
	order  []codec.Hash // insertion order, oldest first, for eviction
}

func (c *attachmentCache) get(root codec.Hash) (document.AttachmentTable, bool) {
	t, ok := c.tables[root]
	return t, ok
}

func (c *attachmentCache) put(root codec.Hash, t document.AttachmentTable) {
	if c.tables == nil {
		c.tables = make(map[codec.Hash]document.AttachmentTable)
	}
	if _, ok := c.tables[root]; ok {
		return
	}
	c.tables[root] = t
	c.order = append(c.order, root)
	for len(c.order) > attachmentCacheSize {
		delete(c.tables, c.order[0])
		c.order = c.order[1:]
	}
}

// AttachmentTableAt returns the attachment table at commit.
func (d *InMemoryCommitDag) AttachmentTableAt(commit codec.Hash) (document.AttachmentTable, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attachmentTableLocked(commit)
}

// SeedAttachmentTable caches table as the attachment table whose root it is - for a checkpoint
// restoring its heads' tables. An empty table needs no seed.
func (d *InMemoryCommitDag) SeedAttachmentTable(table document.AttachmentTable) {
	root := table.Root()
	if root == nil {
		return
	}
	d.mu.Lock()
	d.attachments.put(*root, table)
	d.mu.Unlock()
}

// attachmentTableLocked resolves commit's table, rebuilding it from the nearest known ancestor if
// it is not cached. Must hold mu.
func (d *InMemoryCommitDag) attachmentTableLocked(commit codec.Hash) (document.AttachmentTable, error) {
	var chain []document.Commit // commits whose table must be built, newest first
	at := commit
	base := document.EmptyAttachmentTable()
	for {
		c, ok := d.commitLocked(at)
		if !ok {
			return document.AttachmentTable{}, NewConsistencyError(
				"attachment table: commit not present", d.NamespaceID, &at)
		}
		if c.AttachmentTreeHash == nil {
			break // an empty table: the base
		}
		if t, ok := d.attachments.get(*c.AttachmentTreeHash); ok {
			base = t
			break
		}
		chain = append(chain, c)
		if len(c.ParentHashes) == 0 {
			break // a root commit with attachments: built from empty
		}
		at = c.ParentHashes[0]
	}
	table := base
	for i := len(chain) - 1; i >= 0; i-- {
		c := chain[i]
		if d.opsEvictedFor(c.Hash) {
			full, err := d.hydrate(c)
			if err != nil {
				return document.AttachmentTable{}, err
			}
			c = full
		}
		next, err := table.Apply(c.Operations)
		if err != nil {
			return document.AttachmentTable{}, err
		}
		if got := next.Root(); got == nil || *got != *c.AttachmentTreeHash {
			return document.AttachmentTable{}, NewConsistencyError(
				"attachment table: replaying the commit's operations does not reproduce its attachment root",
				d.NamespaceID, &c.Hash)
		}
		d.attachments.put(*c.AttachmentTreeHash, next)
		table = next
	}
	return table, nil
}

// attachmentRootLocked is the AttachmentTreeHash of the commit tx would make on parents, and the
// table it names. A transaction carrying a table (tx.Attachments, staged against the right base)
// has that recorded; otherwise the first parent's table has tx's operations applied - which, for a
// transaction that touches no attachment, is the parent's table unchanged. Must hold mu.
func (d *InMemoryCommitDag) attachmentRootLocked(tx document.Transaction, parents []codec.Hash) (*codec.Hash, error) {
	if tx.Attachments != nil {
		if root := tx.Attachments.Root(); root != nil {
			d.attachments.put(*root, *tx.Attachments)
			return root, nil
		}
		return nil, nil
	}
	var inherited *codec.Hash
	if len(parents) > 0 {
		if c, ok := d.commitLocked(parents[0]); ok {
			inherited = c.AttachmentTreeHash
		}
	}
	if !document.TouchesAttachments(tx.Operations, inherited != nil) {
		return inherited, nil
	}
	base := document.EmptyAttachmentTable()
	if inherited != nil {
		t, err := d.attachmentTableLocked(parents[0])
		if err != nil {
			return nil, err
		}
		base = t
	}
	next, err := base.Apply(tx.Operations)
	if err != nil {
		return nil, err
	}
	root := next.Root()
	if root != nil {
		d.attachments.put(*root, next)
	}
	return root, nil
}

// HasAttachments reports whether any commit this DAG holds or has held carries an attachment root
// - the case a checkpoint must be written in a format older builds refuse.
func (d *InMemoryCommitDag) HasAttachments() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.sawAttachments || len(d.nodes.attachRoots) > 0 {
		return true
	}
	for _, seg := range d.nodes.segs {
		if len(seg.sec[secAttr]) > 0 {
			return true
		}
	}
	return false
}
