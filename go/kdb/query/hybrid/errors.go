package hybrid

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/policy"
	"github.com/limidus/kdb/go/kdb/storage"
)

// HistoryDisabledError reports a version-pinned read against a namespace
// that keeps no history to pin to.
//
// The spec (kdb-spec-layer6-component17 §6) calls for this whenever a
// version clause meets HistoryMode.NONE. This implementation raises it
// only when the namespace retains *nothing* beyond its checkpoint, and
// uses HistoryNotRetainedError for the ordinary case, because a "none"
// namespace here keeps a configurable window of recent history and reads
// inside that window are perfectly answerable. Refusing them on the
// strength of the mode name alone would be refusing data the namespace
// actually has.
type HistoryDisabledError struct {
	NamespaceID string
}

func (e *HistoryDisabledError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q runs with history=NONE and a zero retention window, "+
			"so there is no past to read at; configure retain.duration to keep one",
		e.NamespaceID)
}

// HistoryNotRetainedError reports a version that the namespace once had
// and has since reclaimed - the ordinary way a read fails under
// HistoryModeNone.
//
// Distinct from a plain "not found" because the remedy is different: this
// one is fixed by a longer retention window, and nothing else. Detect with
// errors.As.
type HistoryNotRetainedError struct {
	NamespaceID string
	Spec        string
	Window      storage.RetentionWindow
	Err         error
}

func (e *HistoryNotRetainedError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q does not retain %s: it runs with history=NONE and a retention window of %s, "+
			"so commits older than that have been reclaimed",
		e.NamespaceID, e.Spec, e.Window)
}

func (e *HistoryNotRetainedError) Unwrap() error { return e.Err }

// ReadOnlyCheckoutError reports a write attempted while the session is
// reading at something other than head.
//
// A checkout is a view of a past state, and this engine has exactly one
// live tree that every write continues from (see
// engine.ServerEngine.CommitTree), so a write "at" a past commit has
// nowhere to land. Undo is a revert - a new commit that restores the old
// tree - not a write into the past.
type ReadOnlyCheckoutError struct {
	NamespaceID string
	AtCommit    string
}

func (e *ReadOnlyCheckoutError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q is read-only at %s; to undo, revert to that commit instead of writing at it",
		e.NamespaceID, e.AtCommit)
}

// historyModeOf reports the history mode a namespace's policy declares,
// defaulting to full for a namespace with no policy on file - which is
// most of them, and which must not be read as "keeps nothing".
func historyModeOf(reg policy.Registry, namespaceID string) (policy.HistoryMode, storage.RetentionWindow) {
	if reg == nil {
		return policy.HistoryModeFull, storage.RetentionWindow{}
	}
	p, err := reg.Get(namespaceID)
	if err != nil {
		return policy.HistoryModeFull, storage.RetentionWindow{}
	}
	return p.History, p.Retain
}
