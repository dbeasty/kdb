package server

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/storage"
)

// Scrub and self-repair (docs/kdb-distributed-self-healing-research.md, Phase 13), after ZFS's
// scrub, HDFS's block scanner and Ceph's deep scrub: re-read every document the head's tree names,
// check each body against the content hash the tree records, and restore any that cannot be read
// or no longer match from a peer - by content hash, so the peer need not be trusted.
//
// A repair is a commit ("kdb:repair/1") that rewrites the damaged documents with their verified
// bodies: the same content, so the tree - and every reader's view - is unchanged, but the bodies are
// durable again, in the log a restart indexes. (The cold loader indexes the newest frame holding
// a content hash, so the repair's copy is the one found.) Peers receive it as an ordinary commit
// that changes nothing for them.

// ConflictDamaged is a document this node could not read and could not repair from any peer. The
// entry is cleared by the next scrub that finds the document readable.
const ConflictDamaged peersync.ConflictKind = "damaged"

// RepairMessage is the message of a scrub's repair commit.
const RepairMessage = "kdb:repair/1"

// BodyFetcher returns verified bodies for wanted (document id to content hash) from wherever it
// can - replication.Replicator.FetchBodies asks the configured peers. treeHex names the tree the
// documents are from, which a peer holding the same history can look in first.
type BodyFetcher func(ns string, wanted map[codec.UUID]codec.Hash, treeHex string) (map[codec.UUID]string, error)

// ScrubReport is what a scrub found and did.
type ScrubReport struct {
	Namespace string `json:"namespace"`
	HeadHex   string `json:"head"`
	TreeHex   string `json:"tree"`
	// Checked counts the documents read and verified.
	Checked int `json:"checked"`
	// Damaged lists the documents that could not be read or did not match their content hash.
	Damaged []string `json:"damaged,omitempty"`
	// Repaired lists those restored from a peer; RepairCommit is the commit that did it.
	Repaired     []string `json:"repaired,omitempty"`
	RepairCommit string   `json:"repairCommit,omitempty"`
	// Unrepaired lists damaged documents no peer could supply; each has a "damaged" queue entry.
	Unrepaired []string `json:"unrepaired,omitempty"`
	// FetchError is why fetching from peers failed, when it did.
	FetchError string `json:"fetchError,omitempty"`
}

const scrubBatch = 256

// Scrub verifies every document of the namespace's head and repairs what it can with fetch (nil:
// only report). It reads as the runtime and commits a repair as the runtime.
func (s *KdbServerRuntime) Scrub(fetch BodyFetcher) (ScrubReport, error) {
	ns := s.Runtime.DefaultNamespace
	head, headCommit, ok, err := s.dag.HeadCommit()
	if err != nil {
		return ScrubReport{}, err
	}
	rep := ScrubReport{Namespace: ns}
	if !ok {
		return rep, nil
	}
	rep.HeadHex, rep.TreeHex = head.Hex(), headCommit.DocumentTreeHash.Hex()
	walker, ok := s.Runtime.Storage.(storage.TreeWalker)
	if !ok {
		return rep, fmt.Errorf("scrub: storage for %s cannot walk trees", ns)
	}
	tree := headCommit.DocumentTreeHash
	entries := map[codec.UUID]codec.Hash{}
	if err := walker.WalkTree(ns, tree, func(id codec.UUID, h codec.Hash) bool {
		entries[id] = h
		return true
	}); err != nil {
		return rep, fmt.Errorf("scrub: the head's tree %s cannot be read: %w", tree.Hex(), err)
	}
	ids := make([]codec.UUID, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })

	damaged := map[codec.UUID]codec.Hash{}
	for start := 0; start < len(ids); start += scrubBatch {
		batch := ids[start:min(start+scrubBatch, len(ids))]
		docs, err := s.Runtime.Storage.GetDocuments(ns, batch, tree)
		if err != nil || len(docs) != len(batch) {
			// One unreadable body can fail the whole batch; read each on its own to find which.
			docs = make([]*document.Document, len(batch))
			for i, id := range batch {
				if d, err := s.Runtime.Storage.GetDocument(ns, id, tree); err == nil {
					docs[i] = d
				}
			}
		}
		for i, id := range batch {
			rep.Checked++
			if !bodyMatches(docs[i], entries[id]) {
				damaged[id] = entries[id]
			}
		}
	}
	for id := range damaged {
		rep.Damaged = append(rep.Damaged, id.String())
	}
	sort.Strings(rep.Damaged)
	s.clearHealed(damaged)
	if len(damaged) == 0 {
		return rep, nil
	}

	bodies := map[codec.UUID]string{}
	if fetch != nil {
		got, err := fetch(ns, damaged, rep.TreeHex)
		if err != nil {
			rep.FetchError = err.Error()
		}
		for id, body := range got {
			// Verified by the fetcher; checked again here, since a repair must never write a body
			// that is not the one the tree names.
			if want, asked := damaged[id]; asked && bodyMatches(&document.Document{ID: id, JSON: body}, want) {
				bodies[id] = body
			}
		}
	}
	if len(bodies) > 0 {
		ops := make([]document.Op, 0, len(bodies))
		for _, id := range ids {
			if body, ok := bodies[id]; ok {
				ops = append(ops, document.WriteOp{DocID: id, Patch: body})
				rep.Repaired = append(rep.Repaired, id.String())
			}
		}
		c, err := s.systemCommit(replacing(ops), RepairMessage)
		if err != nil {
			return rep, fmt.Errorf("scrub: committing the repair: %w", err)
		}
		rep.RepairCommit = c.Hash.Hex()
		if c.DocumentTreeHash != tree {
			// Impossible unless a write landed between the walk and the repair - the repair then
			// sat on top of it and wrote only verified content, so it is still correct; say so.
			rep.FetchError = strings.TrimSpace(rep.FetchError + " (the head moved during the scrub; the repair was applied on top of it)")
		}
	}
	for _, idStr := range rep.Damaged {
		id, _ := codec.ParseUUID(idStr)
		if _, ok := bodies[id]; ok {
			_ = s.Conflicts.Remove(damagedID(ns, idStr))
			continue
		}
		rep.Unrepaired = append(rep.Unrepaired, idStr)
		_, _ = s.Conflicts.Record(peersync.ConflictEntry{
			ID: damagedID(ns, idStr), Kind: ConflictDamaged, Namespace: ns, IncomingHex: rep.TreeHex,
			Detail: fmt.Sprintf("document %s (content %s) could not be read, and no peer supplied it", idStr, damaged[id].Hex()),
		})
	}
	if len(rep.Unrepaired) > 0 {
		return rep, &ScrubUnrepairedError{Namespace: ns, Documents: rep.Unrepaired}
	}
	return rep, nil
}

// ScrubUnrepairedError reports documents a scrub found damaged and could not repair.
type ScrubUnrepairedError struct {
	Namespace string
	Documents []string
}

func (e *ScrubUnrepairedError) Error() string {
	return fmt.Sprintf("scrub: %s: %d damaged document(s) could not be repaired: %s", e.Namespace, len(e.Documents), strings.Join(e.Documents, ", "))
}

// IsScrubUnrepaired reports whether err is a ScrubUnrepairedError.
func IsScrubUnrepaired(err error) bool {
	var e *ScrubUnrepairedError
	return errors.As(err, &e)
}

func damagedID(ns, doc string) string { return peersync.ConflictID(ConflictDamaged, ns, doc) }

// clearHealed drops "damaged" entries for documents that are no longer damaged.
func (s *KdbServerRuntime) clearHealed(damaged map[codec.UUID]codec.Hash) {
	for _, e := range s.Conflicts.List() {
		if e.Kind != ConflictDamaged {
			continue
		}
		stillDamaged := false
		for id := range damaged {
			if e.ID == damagedID(s.Runtime.DefaultNamespace, id.String()) {
				stillDamaged = true
			}
		}
		if !stillDamaged {
			_ = s.Conflicts.Remove(e.ID)
		}
	}
}

func bodyMatches(d *document.Document, want codec.Hash) bool {
	if d == nil {
		return false
	}
	h, err := d.ContentHash()
	return err == nil && h == want
}

// HeadTree is the document tree at the namespace's main head.
func (s *KdbServerRuntime) HeadTree() (document.DocumentTree, error) {
	_, head, ok, err := s.dag.HeadCommit()
	if err != nil {
		return document.DocumentTree{}, err
	}
	if !ok {
		return document.DocumentTree{}, errors.New("namespace has no head")
	}
	if r, ok := s.Runtime.Storage.(storage.TreeResolver); ok {
		if t, found, err := r.TreeAt(head.DocumentTreeHash); err != nil || found {
			return t, err
		}
	}
	w, ok := s.Runtime.Storage.(storage.TreeWalker)
	if !ok {
		return document.DocumentTree{}, errors.New("storage cannot resolve trees")
	}
	entries := map[codec.UUID]codec.Hash{}
	if err := w.WalkTree(s.Runtime.DefaultNamespace, head.DocumentTreeHash, func(id codec.UUID, h codec.Hash) bool {
		entries[id] = h
		return true
	}); err != nil {
		return document.DocumentTree{}, err
	}
	return document.BuildDocumentTree(entries)
}
