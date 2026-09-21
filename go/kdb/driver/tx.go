package driver

import (
	"sort"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// connTx is one connection's open transaction: the writes it has buffered in each namespace it
// touched and, for a snapshot transaction, where it reads each one.
type connTx struct {
	snapshot bool
	readOnly bool

	mu sync.Mutex
	// writes are the buffered operations per namespace, with the base version fixed by the
	// transaction's first write there.
	writes map[*namespace]*txWrites
	// heads are a snapshot transaction's read points, fixed at begin for every namespace open
	// then and at first touch for any opened later. Each is pinned until the transaction ends.
	heads map[*namespace]codec.Hash
	pins  []func()
}

type txWrites struct {
	base codec.Hash
	ops  []document.Op
}

func newConnTx(db *database, snapshot, readOnly bool) *connTx {
	tx := &connTx{snapshot: snapshot, readOnly: readOnly, writes: make(map[*namespace]*txWrites)}
	if snapshot {
		tx.heads = make(map[*namespace]codec.Hash)
		// One instant for every open namespace: taking each namespace's write lock, in the same
		// order a cross-namespace commit takes them, means no commit - and in particular no
		// cross-namespace one - can be half-applied while the heads are read.
		open := db.openNamespaces()
		for _, ns := range open {
			ns.write.Lock()
		}
		for _, ns := range open {
			if h, err := ns.head(); err == nil {
				tx.heads[ns] = h
				tx.pins = append(tx.pins, ns.d.Pin(h))
			}
		}
		for i := len(open) - 1; i >= 0; i-- {
			open[i].write.Unlock()
		}
	}
	return tx
}

// readHead is where a snapshot transaction reads ns.
func (tx *connTx) readHead(ns *namespace) (codec.Hash, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if h, ok := tx.heads[ns]; ok {
		return h, nil
	}
	h, err := ns.head()
	if err != nil {
		return codec.Hash{}, err
	}
	tx.heads[ns] = h
	tx.pins = append(tx.pins, ns.d.Pin(h))
	return h, nil
}

// add buffers ops for ns. base is the head the statement read at; only the first write in a
// namespace sets the part's base, so a concurrent writer arriving later in the transaction still
// conflicts.
func (tx *connTx) add(ns *namespace, base codec.Hash, ops []document.Op) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	w, ok := tx.writes[ns]
	if !ok {
		w = &txWrites{base: base}
		tx.writes[ns] = w
	}
	w.ops = append(w.ops, ops...)
}

func (tx *connTx) hasWrites() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	for _, w := range tx.writes {
		if len(w.ops) > 0 {
			return true
		}
	}
	return false
}

// parts returns the transaction's writes as commit parts, one per namespace, in namespace order.
func (tx *connTx) parts() []commitPart {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	out := make([]commitPart, 0, len(tx.writes))
	for ns, w := range tx.writes {
		if len(w.ops) == 0 {
			continue
		}
		out = append(out, commitPart{ns: ns, tx: document.Transaction{BaseVersion: w.base, Operations: w.ops}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ns.id < out[j].ns.id })
	return out
}

// release drops the transaction's read pins.
func (tx *connTx) release() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	for _, p := range tx.pins {
		p()
	}
	tx.pins = nil
}
