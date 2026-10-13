package replication

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/peersync"
)

// maxPendingWalk bounds the ancestry walk pendingFrontier makes to drop tips another tip (or
// main) already reaches. The tips are a few pages apart, so a short walk finds the redundant ones;
// one it does not reach is kept, which costs a have, never correctness.
const maxPendingWalk = 20_000

// pendingFrontier merges the tips an interrupted sync received into the ones already pending for
// a namespace, keeping only a frontier: tips this node holds (a have it does not hold is useless -
// the pull drops it anyway), that neither another kept tip nor local main already reaches, at most
// peersync.MaxPendingTips of them, newest kept. PendingTips used to grow by every page's tips on
// every failed sync, without limit, and every one went out as a have on every later sync
// (plan bug 6).
//
// d may be nil (the namespace is not open here): then only duplicates and the cap apply.
func pendingFrontier(d *dag.InMemoryCommitDag, pending []string, received []codec.Hash) []string {
	var cands []codec.Hash
	seen := map[codec.Hash]bool{}
	add := func(h codec.Hash) {
		if seen[h] {
			return
		}
		seen[h] = true
		if d != nil && !d.HasCommit(h) {
			return
		}
		cands = append(cands, h)
	}
	for _, hex := range pending {
		if h, err := codec.HashFromHex(hex); err == nil {
			add(h)
		}
	}
	for _, h := range received {
		add(h)
	}
	if d != nil && len(cands) > 0 {
		starts := append([]codec.Hash(nil), cands...)
		if head, err := d.Head(); err == nil {
			starts = append(starts, head)
		}
		below := ancestorsWithin(d, starts, maxPendingWalk)
		head, _ := d.Head()
		kept := cands[:0]
		for _, h := range cands {
			if !below[h] && h != head {
				kept = append(kept, h)
			}
		}
		cands = kept
	}
	if len(cands) > peersync.MaxPendingTips {
		cands = cands[len(cands)-peersync.MaxPendingTips:]
	}
	out := make([]string, len(cands))
	for i, h := range cands {
		out[i] = h.Hex()
	}
	return out
}

// ancestorsWithin returns the proper ancestors of starts that a breadth-first walk reaches within
// limit commits.
func ancestorsWithin(d *dag.InMemoryCommitDag, starts []codec.Hash, limit int) map[codec.Hash]bool {
	below := map[codec.Hash]bool{}
	var queue []codec.Hash
	for _, s := range starts {
		if c, ok := d.GetCommit(s); ok {
			queue = append(queue, c.ParentHashes...)
		}
	}
	for len(queue) > 0 && len(below) < limit {
		h := queue[0]
		queue = queue[1:]
		if below[h] {
			continue
		}
		below[h] = true
		if c, ok := d.GetCommit(h); ok && len(queue) < limit {
			queue = append(queue, c.ParentHashes...)
		}
	}
	return below
}
