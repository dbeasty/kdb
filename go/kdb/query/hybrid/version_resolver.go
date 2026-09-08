package hybrid

import (
	"fmt"
	"strings"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// VersionResolver resolves version clauses to commit hashes.
type VersionResolver interface {
	Resolve(d dag.CommitDAG, clause VersionClause, activeCheckout *CheckoutHandle) (codec.Hash, error)
}

// DefaultVersionResolver resolves every clause against the DAG.
//
// It used to answer head for AtTag and AtTime, with a comment saying tag
// and time resolution needed DAG APIs that did not exist yet. Those APIs
// exist now (dag.HistoryNavigator), and the interim behaviour was the
// worst available: a query naming a tag that was never created, or a time
// before the namespace existed, returned current data and reported
// success. Every clause now resolves for real or fails.
type DefaultVersionResolver struct{}

// NewDefaultVersionResolver returns the default resolver.
func NewDefaultVersionResolver() *DefaultVersionResolver {
	return &DefaultVersionResolver{}
}

func (DefaultVersionResolver) Resolve(d dag.CommitDAG, clause VersionClause, activeCheckout *CheckoutHandle) (codec.Hash, error) {
	if clause == nil {
		if activeCheckout != nil {
			return activeCheckout.CommitHash, nil
		}
		return d.Head()
	}
	nav, ok := d.(dag.HistoryNavigator)
	if !ok {
		// A DAG that cannot navigate cannot answer a version clause. Say
		// so rather than resolving to head, which is the whole point of
		// this rewrite.
		return codec.Hash{}, fmt.Errorf(
			"kdb: this namespace's commit graph does not support version-pinned reads")
	}
	switch c := clause.(type) {
	case AtCommit:
		// A commit clause accepts a full revision specification, so
		// "AT COMMIT 'head~10'" works alongside a pasted hash.
		return nav.ResolveRevision(strings.TrimSpace(c.Hex))
	case AtTag:
		return nav.ResolveRef(dag.RefByTag{Name: strings.TrimSpace(c.Tag)})
	case AtTime:
		ts, err := parseISO8601(c.ISO8601)
		if err != nil {
			return codec.Hash{}, err
		}
		return nav.ResolveRef(dag.RefByTime{Timestamp: ts})
	default:
		return codec.Hash{}, fmt.Errorf("kdb: unsupported version clause %T", clause)
	}
}

// parseISO8601 reads the timestamp an AT TIME clause carries.
//
// RFC 3339 with and without sub-second precision, plus a bare date, which
// is what people type. A date alone means midnight UTC, which reads at the
// start of that day rather than the end of it - the conservative choice,
// since the alternative silently includes a day of writes the caller did
// not ask for.
func parseISO8601(s string) (codec.Timestamp, error) {
	raw := strings.TrimSpace(s)
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			micros := t.UnixMicro()
			return codec.Timestamp{
				EpochMillis:    micros / 1000,
				MicroRemainder: int(micros % 1000),
			}, nil
		}
	}
	return codec.Timestamp{}, fmt.Errorf(
		"kdb: unparseable AT TIME value %q: want an RFC 3339 timestamp like \"2026-09-08T12:00:00Z\"", s)
}
