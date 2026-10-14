package peersync

import (
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// MaxPendingTips bounds the tips a sync remembers of what it received - NamespaceSyncResult's
// ReceivedTips, and the replicator's PendingTips built from them. They are haves for resuming an
// interrupted pull: hints that save re-sending, never needed for correctness. A pull's pages are
// ancestors-first, so all it received is usually reached from one or two tips; more than this
// means many side branches, and dropping the oldest costs at most re-sending some of them.
const MaxPendingTips = 64

// foldFrontier folds one page of received commits into frontier, the tips of everything received
// so far: a commit is a tip until a received commit names it as a parent. Pages arrive
// ancestors-first, so a later page's commits retire earlier pages' tips as they build on them,
// and the frontier stays the size of the history's width, not its length. Capped at max, oldest
// dropped first. Returns a new slice; frontier is not modified.
func foldFrontier(frontier []codec.Hash, page []document.Commit, max int) []codec.Hash {
	named := namedParents(page)
	return foldTips(frontier, named, tipsOf(page, named), max)
}

// foldTips is foldFrontier given the page's namedParents and its tips.
func foldTips(frontier []codec.Hash, named map[codec.Hash]struct{}, tips []codec.Hash, max int) []codec.Hash {
	out := make([]codec.Hash, 0, len(frontier)+2)
	seen := make(map[codec.Hash]struct{}, len(frontier)+2)
	for _, h := range frontier {
		if _, gone := named[h]; gone {
			continue
		}
		if _, dup := seen[h]; dup {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	for _, h := range tips {
		if _, dup := seen[h]; dup {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	if max > 0 && len(out) > max {
		out = append([]codec.Hash(nil), out[len(out)-max:]...)
	}
	return out
}

// pageHaves is the haves for the next page of a transfer: the ones it started with, plus the last
// page's tips - what the host's cursor checks to continue the transfer (missingCursor.continues)
// and, should it have to work the missing set out afresh, what it builds on.
//
// The haves used to grow by every page's tips, so a long transfer's requests grew with it, and
// a host without a cursor walked all of them per page. The last page's tips stand for every page
// before them on the line of history being fetched; what they do not reach - a side branch an
// earlier page finished - is only re-sent if the host loses its cursor, and storing a commit
// already held is a no-op.
func pageHaves(base, lastTips []codec.Hash) []codec.Hash {
	out := make([]codec.Hash, 0, len(base)+len(lastTips))
	out = append(out, base...)
	return append(out, lastTips...)
}
