package peersync

import (
	"context"
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Read-through for filtered projections (docs/kdb-distributed-self-healing-research.md, Phase 14).
// A projection holds only the documents of its source that match its filter. DOC_FETCH lets it
// read any other document of the source on demand - as of the source commit the projection is at,
// so what it reads is consistent with what it holds - with a Merkle proof of what that commit's
// tree holds for the id. The replica checks the commit hashes to the one it asked for and every
// body against its proof, so a wrong or stale answer is refused rather than served.

// MaxDocFetchIDs bounds one DOC_FETCH request.
const MaxDocFetchIDs = 256

// docFetch answers DOC_FETCH: read rights on the namespace, and on each document, as a projection
// needs - not sync rights.
func (h *V2Host) docFetch(m wire.DocFetchMessage) (wire.Message, error) {
	if len(m.DocIDs) > MaxDocFetchIDs {
		return nil, fmt.Errorf("peer sync: DOC_FETCH asks for %d documents, at most %d", len(m.DocIDs), MaxDocFetchIDs)
	}
	h.mu.Lock()
	principal := h.principal
	h.mu.Unlock()
	authz := h.auth.Authorizer()
	if err := authz.Authorize(context.Background(), principal, auth.StreamSubscribeAction{Namespace: m.Namespace}); err != nil {
		return nil, err
	}
	env, err := h.cfg.Namespaces.Env(m.Namespace, false)
	if err != nil {
		return nil, err
	}
	at, err := env.DAG.Head()
	if err != nil {
		return nil, err
	}
	if m.AtHex != "" {
		if at, err = codec.HashFromHex(m.AtHex); err != nil {
			return nil, err
		}
	}
	commit, err := env.DAG.GetCommitOrThrow(at)
	if err != nil {
		return nil, fmt.Errorf("peer sync: DOC_FETCH at %s: %w", at.Hex(), err)
	}
	tree, err := treeAt(env.Storage, env.NamespaceID, commit.DocumentTreeHash)
	if err != nil {
		return nil, err
	}
	res := wire.DocFetchResultMessage{H: header(wire.MsgDocFetchResult, m.H.CorrelationID), Namespace: m.Namespace, Commit: commit}
	for _, s := range m.DocIDs {
		id, err := codec.ParseUUID(s)
		if err != nil {
			return nil, err
		}
		if authz.Authorize(context.Background(), principal, auth.DocumentReadAction{Namespace: m.Namespace, DocID: s}) != nil {
			res.Docs = append(res.Docs, wire.FetchedDoc{DocID: s, Forbidden: true})
			continue
		}
		fd := wire.FetchedDoc{DocID: s, Proof: proofToWire(tree.Proof(id))}
		if _, present := tree.HashFor(id); present {
			doc, err := env.Storage.GetDocument(env.NamespaceID, id, commit.DocumentTreeHash)
			if err != nil {
				return nil, err
			}
			if doc == nil {
				return nil, fmt.Errorf("peer sync: document %s of %s is in the tree but unreadable here", s, at.Hex())
			}
			fd.Body, fd.Present = doc.JSON, true
		}
		res.Docs = append(res.Docs, fd)
	}
	return res, nil
}

func proofToWire(p document.TreeProof) wire.DocProofInfo {
	out := wire.DocProofInfo{HasLeaf: p.HasLeaf}
	for _, level := range p.Levels {
		row := make([]string, 16)
		for i, h := range level {
			row[i] = hexOrEmpty(h)
		}
		out.Levels = append(out.Levels, row)
	}
	if p.HasLeaf {
		out.LeafID, out.LeafHash = p.LeafID.String(), p.LeafHash.Hex()
	}
	return out
}

func proofFromWire(id codec.UUID, w wire.DocProofInfo) (document.TreeProof, error) {
	p := document.TreeProof{DocID: id, HasLeaf: w.HasLeaf}
	for _, row := range w.Levels {
		if len(row) != 16 {
			return p, fmt.Errorf("document proof level has %d children", len(row))
		}
		var level [16]codec.Hash
		for i, s := range row {
			if s == "" {
				continue
			}
			h, err := codec.HashFromHex(s)
			if err != nil {
				return p, err
			}
			level[i] = h
		}
		p.Levels = append(p.Levels, level)
	}
	if w.HasLeaf {
		var err error
		if p.LeafID, err = codec.ParseUUID(w.LeafID); err != nil {
			return p, err
		}
		if p.LeafHash, err = codec.HashFromHex(w.LeafHash); err != nil {
			return p, err
		}
	}
	return p, nil
}

// ProvenDoc is a document a peer proved the state of at a commit.
type ProvenDoc struct {
	// Present and Body: the commit's tree holds the document with exactly this body.
	Present bool
	Body    string
	// Forbidden: the peer would not let this principal read it; nothing was proved.
	Forbidden bool
}

// ErrUnproven is an answer to DOC_FETCH that does not check out: a commit that does not hash to
// itself or to the one asked for, a proof that does not lead to its tree, or a body that does not
// match what the proof says. Nothing from such an answer is used.
var ErrUnproven = errors.New("peer sync: the peer's documents do not check out against its commit")

// FetchDocs reads ids from the peer as of commit atHex (empty: the peer's head), verifying every
// answer. It returns the commit read at.
func (s *RepairSession) FetchDocs(ns, atHex string, ids []codec.UUID) (codec.Hash, map[codec.UUID]ProvenDoc, error) {
	if !containsString(s.caps, wire.SyncCapDocFetch) {
		return codec.Hash{}, nil, NewError("peer does not support DOC_FETCH", nil)
	}
	out := make(map[codec.UUID]ProvenDoc, len(ids))
	var anchor codec.Hash
	for start := 0; start < len(ids); start += MaxDocFetchIDs {
		batch := ids[start:min(start+MaxDocFetchIDs, len(ids))]
		strs := make([]string, len(batch))
		for i, id := range batch {
			strs[i] = id.String()
		}
		at := atHex
		if at == "" && anchor != (codec.Hash{}) {
			at = anchor.Hex() // later batches read at the commit the first one answered for
		}
		reply, err := s.conn.request(wire.DocFetchMessage{H: header(wire.MsgDocFetch, s.conn.next()), Namespace: ns, AtHex: at, DocIDs: strs})
		if err != nil {
			return codec.Hash{}, nil, err
		}
		res, ok := reply.(wire.DocFetchResultMessage)
		if !ok {
			return codec.Hash{}, nil, NewError(fmt.Sprintf("expected DOC_FETCH_RESULT, got %T", reply), nil)
		}
		asked := map[string]codec.UUID{}
		for i, sid := range strs {
			asked[sid] = batch[i]
		}
		docs, err := verifyDocFetch(res, at, asked)
		if err != nil {
			return codec.Hash{}, nil, err
		}
		anchor = res.Commit.Hash
		for id, d := range docs {
			out[id] = d
		}
	}
	return anchor, out, nil
}

// OpenDocSession connects for DOC_FETCH only: a projection's principal, with read rights, says
// hello asking for no namespace to be granted for sync.
func OpenDocSession(w wire.Codec, transport stream.Transport, cfg V2ClientConfig) (*RepairSession, error) {
	return openSession(w, transport, cfg, wire.SyncCapDocFetch)
}

// verifyDocFetch checks a DOC_FETCH answer: the commit hashes to itself (and to at, when a commit
// was asked for), every asked-for id is answered exactly once, and every answer's proof leads to
// the commit's tree with a body that is the one the tree names.
func verifyDocFetch(res wire.DocFetchResultMessage, at string, asked map[string]codec.UUID) (map[codec.UUID]ProvenDoc, error) {
	c := res.Commit
	if h, err := document.ComputeCommitHash(c); err != nil || h != c.Hash {
		return nil, fmt.Errorf("%w: commit %s does not hash to itself", ErrUnproven, c.Hash.Hex())
	}
	if at != "" && c.Hash.Hex() != at {
		return nil, fmt.Errorf("%w: asked for commit %s, answered for %s", ErrUnproven, at, c.Hash.Hex())
	}
	pending := make(map[string]codec.UUID, len(asked))
	for k, v := range asked {
		pending[k] = v
	}
	out := make(map[codec.UUID]ProvenDoc, len(asked))
	for _, d := range res.Docs {
		id, ok := pending[d.DocID]
		if !ok {
			return nil, fmt.Errorf("%w: answered for %s, which was not asked or was answered twice", ErrUnproven, d.DocID)
		}
		delete(pending, d.DocID)
		if d.Forbidden {
			out[id] = ProvenDoc{Forbidden: true}
			continue
		}
		proof, err := proofFromWire(id, d.Proof)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrUnproven, err)
		}
		content, present, err := document.VerifyTreeProof(c.DocumentTreeHash, proof)
		if err != nil || present != d.Present {
			return nil, fmt.Errorf("%w: the proof for %s does not hold", ErrUnproven, d.DocID)
		}
		if !present {
			out[id] = ProvenDoc{}
			continue
		}
		if h, err := (document.Document{ID: id, JSON: d.Body}).ContentHash(); err != nil || h != content {
			return nil, fmt.Errorf("%w: the body of %s is not the one the tree names", ErrUnproven, d.DocID)
		}
		out[id] = ProvenDoc{Present: true, Body: d.Body}
	}
	if len(pending) > 0 {
		return nil, fmt.Errorf("%w: %d asked-for document(s) were not answered", ErrUnproven, len(pending))
	}
	return out, nil
}
