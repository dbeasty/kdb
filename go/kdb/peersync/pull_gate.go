package peersync

import (
	"sync"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// A node syncing one namespace with several peers at once - the replicator runs a loop per peer -
// used to compute each pull's haves before any other pull had stored anything, so a joining node
// was sent the shared history once per peer. The pull gate makes the pulls of one namespace on
// this node take turns: a pull waits for the one in progress, then computes its haves from what
// that pull left - usually the peer's head already held, so nothing is fetched at all.
//
// Only pulls take turns; pushes, and pulls of other namespaces, still run in parallel. A pull
// waits at most maxWait for its turn and then goes ahead on its own, so a peer that stalls
// mid-pull delays the others by at most that, never blocks them. The tips of every page stored
// while others wait are shared with them: a pull whose peer dropped mid-way leaves its partial
// progress as haves for the next, which fetches only the rest.
//
// The gate is keyed by the namespace's DAG, not its name: two nodes in one process (tests, an
// embedded cluster) each have their own.

type pullGate struct {
	turn  chan struct{} // holds a token while some pull of the namespace has its turn
	users int           // pulls holding or waiting for a turn; the gate is dropped at zero
	tips  []codec.Hash  // tips of pages stored while others waited
}

// maxSharedTips bounds the tips a gate keeps for waiting pulls; each is only a have.
const maxSharedTips = 64

var pullGates = struct {
	sync.Mutex
	m map[*dag.InMemoryCommitDag]*pullGate
}{m: map[*dag.InMemoryCommitDag]*pullGate{}}

// enterPull waits, up to maxWait, for d's pulls to give this one its turn. It returns the gate
// and whether the turn was taken; leavePull must be called with both either way.
func enterPull(d *dag.InMemoryCommitDag, maxWait time.Duration) (*pullGate, bool) {
	pullGates.Lock()
	g := pullGates.m[d]
	if g == nil {
		g = &pullGate{turn: make(chan struct{}, 1)}
		pullGates.m[d] = g
	}
	g.users++
	pullGates.Unlock()
	select {
	case g.turn <- struct{}{}:
		return g, true
	default:
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case g.turn <- struct{}{}:
		return g, true
	case <-timer.C:
		return g, false
	}
}

func leavePull(d *dag.InMemoryCommitDag, g *pullGate, held bool) {
	if held {
		<-g.turn
	}
	pullGates.Lock()
	defer pullGates.Unlock()
	g.users--
	if g.users == 0 {
		delete(pullGates.m, d)
	}
}

// share records the tips of a page just stored, for pulls waiting their turn. A gate lives only
// while some pull holds or awaits it, so the list is short-lived as well as bounded.
func (g *pullGate) share(tips []codec.Hash) {
	pullGates.Lock()
	defer pullGates.Unlock()
	g.tips = append(g.tips, tips...)
	if len(g.tips) > maxSharedTips {
		g.tips = append([]codec.Hash(nil), g.tips[len(g.tips)-maxSharedTips:]...)
	}
}

// shared is the tips other pulls stored, to add as haves.
func (g *pullGate) shared() []codec.Hash {
	pullGates.Lock()
	defer pullGates.Unlock()
	return append([]codec.Hash(nil), g.tips...)
}
