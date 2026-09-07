package storage

import "fmt"

// HistoryStrategy selects how a namespace makes reads at historical
// commits possible after a restart.
//
// A checkpoint restores the live tree and the commit graph, never the
// document tree every past commit had. Recovering those is the whole
// question, and the two answers have opposite cost shapes, so this is a
// choice a deployment makes rather than one the engine makes for it.
//
// The two are not interchangeable on the same data directory: one leaves
// objects on disk the other never writes, so a namespace records which one
// it was built with and refuses to open under the other. See
// embed.MigrateHistoryStrategy for the offline conversion.
type HistoryStrategy int

const (
	// HistoryStrategyUnset means "whatever the namespace already is", and
	// for a new namespace, the default.
	HistoryStrategyUnset HistoryStrategy = iota

	// HistoryStrategyReplay writes nothing extra and rebuilds historical
	// trees by replaying the delta log the first time one is asked for,
	// re-hashing every document to recover the mapping each commit had.
	//
	// Free on the write path, expensive exactly once on a read that
	// touches history, and nothing at all for a namespace only ever read
	// at its head. The right choice when history is genuinely never read,
	// or when writes are the scarce resource.
	HistoryStrategyReplay

	// HistoryStrategyObjects records each commit's tree as an object
	// addressed by that tree's own hash, so a historical read resolves by
	// lookup - commit names tree, tree names documents - the way git
	// reaches an old revision without walking anything.
	//
	// Costs a small write per commit (the entries that changed, plus a
	// whole tree periodically to bound how far a read has to walk back)
	// and turns a historical read from a full log scan into a handful of
	// fetches. The default for new namespaces.
	HistoryStrategyObjects
)

func (h HistoryStrategy) String() string {
	switch h {
	case HistoryStrategyReplay:
		return "replay"
	case HistoryStrategyObjects:
		return "objects"
	default:
		return ""
	}
}

// ParseHistoryStrategy reads a strategy name. The empty string is
// HistoryStrategyUnset rather than an error, so an absent setting and an
// absent marker mean the same thing everywhere.
func ParseHistoryStrategy(s string) (HistoryStrategy, error) {
	switch s {
	case "":
		return HistoryStrategyUnset, nil
	case "replay":
		return HistoryStrategyReplay, nil
	case "objects":
		return HistoryStrategyObjects, nil
	default:
		return HistoryStrategyUnset, fmt.Errorf(
			"kdb: unknown history strategy %q: want \"replay\" or \"objects\"", s)
	}
}

// DefaultHistoryStrategy is what a namespace created today uses.
//
// Objects, because the cost it adds falls on the write path in small
// constant amounts while the cost it removes - a full log scan, once, to
// answer one historical read - is the kind that surprises people in
// production. A namespace that will never read history can opt into
// Replay and pay neither.
const DefaultHistoryStrategy = HistoryStrategyObjects
