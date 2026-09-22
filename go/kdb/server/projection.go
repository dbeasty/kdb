package server

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/storage"
)

// ProjectionTarget is this runtime as the local home of a filtered projection.
func (s *KdbServerRuntime) ProjectionTarget() peersync.ProjectionTarget { return projectionTarget{s} }

type projectionTarget struct{ rt *KdbServerRuntime }

// LastSource walks back from head to the most recent commit that completed a transfer. Pages of
// an interrupted transfer sit above it; the next transfer starts again from here, which is safe
// because pages carry final states, not steps.
func (p projectionTarget) LastSource() (string, error) {
	head, err := p.rt.dag.Head()
	if err != nil {
		return "", err
	}
	for h, n := head, 0; n < 100000; n++ {
		c, ok := p.rt.dag.GetCommit(h)
		if !ok || len(c.ParentHashes) == 0 {
			return "", nil
		}
		if source, complete, ok := peersync.ParseProjectionMessage(c.Message); ok && complete {
			return source, nil
		}
		h = c.ParentHashes[0]
	}
	return "", nil
}

func (p projectionTarget) DocIDs() ([]codec.UUID, error) {
	_, head, ok, err := p.rt.dag.HeadCommit()
	if err != nil || !ok {
		return nil, err
	}
	walker, isWalker := p.rt.Runtime.Storage.(storage.TreeWalker)
	if !isWalker {
		return nil, nil
	}
	var ids []codec.UUID
	err = walker.WalkTree(p.rt.Runtime.DefaultNamespace, head.DocumentTreeHash, func(id codec.UUID, _ codec.Hash) bool {
		ids = append(ids, id)
		return true
	})
	return ids, err
}

func (p projectionTarget) Apply(ops []document.Op, source string, complete bool) error {
	if p.rt.writeBackOn() {
		// A document with a local write the source has not decided on keeps its local value:
		// the decision, not the page, says what it becomes.
		st := &p.rt.writeBack
		st.mu.Lock()
		defer st.mu.Unlock()
		pending, err := p.rt.pendingDocsLocked()
		if err != nil {
			return err
		}
		kept := ops[:0:0]
		for _, op := range ops {
			if !pending[opDocID(op)] {
				kept = append(kept, op)
			}
		}
		ops = kept
	}
	_, err := p.rt.systemCommit(replacing(ops), peersync.ProjectionMessage(source, complete))
	return err
}

// replacing turns each write into a replacement. A page carries a document's final state, but a
// write op merges into the document it lands on, so a key the source removed would otherwise
// stay. A delete followed by a write of the same document is the engine's spelling of replace; a
// delete of a document that is not there is a no-op.
func replacing(ops []document.Op) []document.Op {
	out := make([]document.Op, 0, len(ops)*2)
	for _, op := range ops {
		if w, ok := op.(document.WriteOp); ok {
			out = append(out, document.DeleteOp{DocID: w.DocID})
		}
		out = append(out, op)
	}
	return out
}
