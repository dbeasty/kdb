package dag

import (
	"fmt"

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
//
// Every table the DAG derives or is given shares one set pool (document.AttachmentSetPool). Left
// alone it keeps every set in memory; SetAttachmentSetStore backs it with a store, after which it
// keeps a bounded number in memory and reads the others back, checked against their hashes.

// attachmentCacheSize bounds the tables kept besides the ones seeded for heads. Versions of a table
// share their trie nodes, so a cached table costs its changed nodes, not its size.
const attachmentCacheSize = 64

type attachmentCache struct {
	pool   *document.AttachmentSetPool // nil until first needed
	tables map[codec.Hash]document.AttachmentTable
	order  []codec.Hash // insertion order, oldest first, for eviction
	// pinned are seeded tables - a checkpoint's heads', a snapshot's or graft's root's - never
	// evicted: a commit with no history below it here can only get its table from a seed.
	pinned map[codec.Hash]document.AttachmentTable
}

func (c *attachmentCache) get(root codec.Hash) (document.AttachmentTable, bool) {
	if t, ok := c.pinned[root]; ok {
		return t, true
	}
	t, ok := c.tables[root]
	return t, ok
}

func (c *attachmentCache) pin(root codec.Hash, t document.AttachmentTable) {
	if c.pinned == nil {
		c.pinned = make(map[codec.Hash]document.AttachmentTable)
	}
	c.pinned[root] = t
}

// setPool is the pool every table of this DAG shares.
func (c *attachmentCache) setPool() *document.AttachmentSetPool {
	if c.pool == nil {
		c.pool = document.NewAttachmentSetPool(nil, 0)
	}
	return c.pool
}

// own returns t with its sets in the DAG's pool - a table built elsewhere (a transaction's, a
// seed's) would otherwise carry a pool of its own into every table derived from it.
func (c *attachmentCache) own(t document.AttachmentTable) (document.AttachmentTable, error) {
	return t.InPool(c.setPool())
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

// SetAttachmentSetStore backs the DAG's attachment set pool with store, keeping about budgetBytes
// of sets in memory (document.DefaultAttachmentSetBudget when <= 0). Call it before tables are
// seeded or derived; tables already cached move to the new pool (pinned ones) or are dropped.
func (d *InMemoryCommitDag) SetAttachmentSetStore(store document.AttachmentSetStore, budgetBytes int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attachments.pool = document.NewAttachmentSetPool(store, budgetBytes)
	d.attachments.tables, d.attachments.order = nil, nil
	for root, t := range d.attachments.pinned {
		owned, err := d.attachments.own(t)
		if err != nil {
			return err
		}
		d.attachments.pinned[root] = owned
	}
	return nil
}

// AttachmentSetPool is the pool the DAG's attachment tables share.
func (d *InMemoryCommitDag) AttachmentSetPool() *document.AttachmentSetPool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.attachments.setPool()
}

// ForgetAttachmentTablesExcept drops the cached tables that were not seeded, except those of keep.
// A store collecting the sets no longer referenced calls it: a cached table of an old commit may
// name sets the collection drops, and is rebuilt from history if asked for again.
func (d *InMemoryCommitDag) ForgetAttachmentTablesExcept(keep []codec.Hash) {
	d.mu.Lock()
	defer d.mu.Unlock()
	kept := map[codec.Hash]document.AttachmentTable{}
	var order []codec.Hash
	for _, h := range keep {
		c, ok := d.commitLocked(h)
		if !ok || c.AttachmentTreeHash == nil {
			continue
		}
		if t, ok := d.attachments.tables[*c.AttachmentTreeHash]; ok {
			if _, dup := kept[*c.AttachmentTreeHash]; !dup {
				kept[*c.AttachmentTreeHash] = t
				order = append(order, *c.AttachmentTreeHash)
			}
		}
	}
	d.attachments.tables, d.attachments.order = kept, order
}

// PinnedAttachmentTables are the seeded tables, which are never dropped.
func (d *InMemoryCommitDag) PinnedAttachmentTables() []document.AttachmentTable {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]document.AttachmentTable, 0, len(d.attachments.pinned))
	for _, t := range d.attachments.pinned {
		out = append(out, t)
	}
	return out
}

// SeedAttachmentTable caches table as the attachment table whose root it is - for a checkpoint
// restoring its heads' tables. An empty table needs no seed.
func (d *InMemoryCommitDag) SeedAttachmentTable(table document.AttachmentTable) error {
	root := table.Root()
	if root == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	owned, err := d.attachments.own(table)
	if err != nil {
		return err
	}
	d.attachments.pin(*root, owned)
	d.sawAttachments = true
	return nil
}

// attachmentTableLocked resolves commit's table, rebuilding it from the nearest known ancestor if
// it is not cached. Must hold mu.
func (d *InMemoryCommitDag) attachmentTableLocked(commit codec.Hash) (document.AttachmentTable, error) {
	var chain []document.Commit // commits whose table must be built, newest first
	at := commit
	base := d.attachments.setPool().Empty()
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
				fmt.Sprintf("attachment table: replaying the commit's %d operations does not reproduce its attachment root (ops %+v base len %d got %v want %s)", len(c.Operations), c.Operations, table.Len(), next.Root(), c.AttachmentTreeHash.Hex()),
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
			owned, err := d.attachments.own(*tx.Attachments)
			if err != nil {
				return nil, err
			}
			d.attachments.put(*root, owned)
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
	base := d.attachments.setPool().Empty()
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

// PackAttachmentTables packs the attachment tables at commits (deduplicated by root; commits
// without attachments contribute nothing), for a checkpoint to carry its heads' tables.
//
// optional commits are packed when their tables can be found and skipped otherwise - a shallow
// root's, which a node that has since replaced its seed cannot rebuild.
func (d *InMemoryCommitDag) PackAttachmentTables(commits []codec.Hash, optional ...codec.Hash) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := map[codec.Hash]bool{}
	var tables []document.AttachmentTable
	for i, c := range append(append([]codec.Hash(nil), commits...), optional...) {
		t, err := d.attachmentTableLocked(c)
		if err != nil && i >= len(commits) {
			continue
		}
		if err != nil {
			return nil, err
		}
		root := t.Root()
		if root == nil || seen[*root] {
			continue
		}
		seen[*root] = true
		tables = append(tables, t)
	}
	if len(tables) == 0 {
		return nil, nil
	}
	return document.AppendPackedAttachmentTables(nil, tables)
}

// SeedPackedAttachmentTables caches the tables PackAttachmentTables packed. Each is keyed by the
// root its own entries hash to, so a table only ever answers for the commits that name that root.
func (d *InMemoryCommitDag) SeedPackedAttachmentTables(packed []byte) error {
	tables, err := document.ParsePackedAttachmentTablesInto(d.AttachmentSetPool(), packed)
	if err != nil {
		return err
	}
	for _, t := range tables {
		if err := d.SeedAttachmentTable(t); err != nil {
			return err
		}
	}
	return nil
}
