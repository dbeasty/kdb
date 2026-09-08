package embed

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/limidus/kdb/go/kdb/storage"
)

// namespaceMeta is the small marker file kept beside a namespace's
// segments. It has always carried the namespace's own id; it now also
// records which history strategy the namespace was built with.
type namespaceMeta struct {
	NamespaceID string `json:"namespaceId"`
	// HistoryStrategy is "replay", "objects", or absent. Absent means the
	// namespace predates the setting, and is therefore replay: nothing
	// ever wrote the tree objects the other strategy reads.
	HistoryStrategy string `json:"historyStrategy,omitempty"`
	// HistoryMode is "full", "none", or absent. Absent means the namespace
	// predates the setting, and is therefore full: every commit it ever
	// wrote is still there, because nothing has ever reclaimed one.
	HistoryMode string `json:"historyMode,omitempty"`
}

func namespaceMetaPath(dataRoot, namespaceID string) string {
	return filepath.Join(dataRoot, "ns", filepath.FromSlash(namespaceID), "meta.json")
}

func readNamespaceMeta(dataRoot, namespaceID string) (namespaceMeta, bool) {
	raw, err := os.ReadFile(namespaceMetaPath(dataRoot, namespaceID))
	if err != nil {
		return namespaceMeta{}, false
	}
	var m namespaceMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return namespaceMeta{}, false
	}
	return m, true
}

func writeNamespaceMeta(dataRoot, namespaceID string, m namespaceMeta) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	path := namespaceMetaPath(dataRoot, namespaceID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0o644)
}

// HistoryStrategyMismatchError reports a namespace being opened under a
// history strategy other than the one it was built with. Detect with
// errors.As.
//
// This is refused rather than accommodated because the two strategies do
// not describe the same bytes on disk. A namespace built under "objects"
// has a tree object per commit and nothing else can read them; one built
// under "replay" has none, so opening it as "objects" would find no
// objects for any commit already written and quietly fall back to
// scanning - which is the strategy the operator just said not to use, at
// the moment they least expect it. Converting is a real operation on real
// data, so it is an explicit, offline one.
type HistoryStrategyMismatchError struct {
	NamespaceID string
	DataRoot    string
	Recorded    storage.HistoryStrategy
	Requested   storage.HistoryStrategy
}

func (e *HistoryStrategyMismatchError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q was built with the %q history strategy but is being opened as %q; "+
			"the two keep different things on disk and cannot share a data directory. "+
			"Convert it offline with: kdb-inspect migrate-history --data-dir %s --namespace %s --to %s",
		e.NamespaceID, e.Recorded, e.Requested, e.DataRoot, e.NamespaceID, e.Requested)
}

// HistoryModeMismatchError reports a namespace being opened under a
// history mode other than the one it was built with. Detect with
// errors.As.
//
// Refused rather than accommodated, for the same reason the strategy
// mismatch is: the two modes do not leave the same bytes on disk. A
// namespace built as "none" has had segments deleted out from under its
// history, so opening it as "full" would present a database that claims a
// complete past and has a hole in it. Opening a "full" namespace as
// "none" is the more dangerous direction - it would start deleting
// segments that the operator believes are the record - so it is an
// explicit, offline conversion either way.
type HistoryModeMismatchError struct {
	NamespaceID string
	DataRoot    string
	Recorded    storage.HistoryMode
	Requested   storage.HistoryMode
}

func (e *HistoryModeMismatchError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q was built with the %q history mode but is being opened as %q; "+
			"the two keep different things on disk and cannot share a data directory. "+
			"Convert it offline with: kdb-inspect migrate-history --data-dir %s --namespace %s --history-mode %s",
		e.NamespaceID, e.Recorded, e.Requested, e.DataRoot, e.NamespaceID, e.Requested)
}

// HistoryModeStrategyConflictError reports a caller asking for the one
// combination that cannot exist: no history, served by tree objects.
//
// The objects strategy writes a tree object per commit so a historical
// read resolves by lookup. Under HistoryModeNone those commits are
// reclaimed on a window, so the objects would be written for states that
// are about to stop existing - pure write amplification for a read that
// will be refused. Rather than silently downgrade the strategy behind the
// caller's back, a request for both is an error, and a request for the
// mode alone resolves the strategy to replay.
type HistoryModeStrategyConflictError struct {
	NamespaceID string
	Strategy    storage.HistoryStrategy
}

func (e *HistoryModeStrategyConflictError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q cannot use the %q history strategy with the \"none\" history mode: "+
			"tree objects exist to serve reads at commits that this mode reclaims. "+
			"Ask for the mode alone and the strategy resolves to \"replay\"",
		e.NamespaceID, e.Strategy)
}

// resolveNamespaceHistory decides which history strategy and which history
// mode this open runs under, and records them for a namespace that does
// not have them yet.
//
// One function rather than two because the answers are coupled - none
// implies replay - and because they share a marker file: two independent
// resolvers would each read-modify-write meta.json, and the second would
// have to be careful not to erase what the first had just decided.
//
// The rules, in order, and the same for both fields:
//
//   - A namespace with a recorded value keeps it. A caller asking for a
//     different one is refused, never silently accommodated.
//   - A namespace with no marker takes the value its bytes actually
//     support: replay (it was written before tree objects existed) and
//     full (nothing has ever reclaimed a commit from it).
//   - A brand new namespace takes what the caller asked for, or the
//     package default.
//
// readOnly opens never write the marker; an unmarked namespace read this
// way resolves for the life of the runtime without recording anything.
func resolveNamespaceHistory(
	dataRoot, namespaceID string,
	requestedStrategy storage.HistoryStrategy,
	requestedMode storage.HistoryMode,
	namespaceIsNew, readOnly, force bool,
) (storage.HistoryStrategy, storage.HistoryMode, error) {
	if force && requestedStrategy != storage.HistoryStrategyUnset {
		// The migration path, which holds the maintenance lock and is in
		// the middle of making the marker true. It converts one axis at a
		// time, so the other keeps whatever is recorded.
		meta, _ := readNamespaceMeta(dataRoot, namespaceID)
		recordedMode, err := storage.ParseHistoryMode(meta.HistoryMode)
		if err != nil {
			return 0, 0, err
		}
		if requestedMode != storage.HistoryModeUnset {
			// The mode migration forces both axes together, because
			// converting to none must open under none for the bodies it
			// writes to land where the none read path looks for them.
			return requestedStrategy, requestedMode, nil
		}
		if recordedMode == storage.HistoryModeUnset {
			recordedMode = storage.HistoryModeFull
		}
		return requestedStrategy, recordedMode, nil
	}

	meta, found := readNamespaceMeta(dataRoot, namespaceID)
	recordedStrategy, err := storage.ParseHistoryStrategy(meta.HistoryStrategy)
	if err != nil {
		return 0, 0, err
	}
	recordedMode, err := storage.ParseHistoryMode(meta.HistoryMode)
	if err != nil {
		return 0, 0, err
	}

	mode, err := resolveModeLocked(dataRoot, namespaceID, requestedMode, recordedMode, found, namespaceIsNew)
	if err != nil {
		return 0, 0, err
	}

	// The coupling, applied before the strategy is resolved so that a
	// none-mode namespace never records "objects" in its marker.
	if mode == storage.HistoryModeNone {
		if requestedStrategy == storage.HistoryStrategyObjects {
			return 0, 0, &HistoryModeStrategyConflictError{
				NamespaceID: namespaceID, Strategy: requestedStrategy,
			}
		}
		if recordedStrategy == storage.HistoryStrategyObjects {
			// Only reachable through a hand-edited marker or an
			// interrupted migration; the conversion is what fixes it.
			return 0, 0, &HistoryModeStrategyConflictError{
				NamespaceID: namespaceID, Strategy: recordedStrategy,
			}
		}
		requestedStrategy = storage.HistoryStrategyReplay
	}

	strategy, err := resolveStrategyLocked(
		dataRoot, namespaceID, requestedStrategy, recordedStrategy, found, namespaceIsNew)
	if err != nil {
		return 0, 0, err
	}

	if readOnly {
		return strategy, mode, nil
	}
	if !found {
		meta.NamespaceID = namespaceID
	}
	meta.HistoryStrategy = strategy.String()
	meta.HistoryMode = mode.String()
	if err := writeNamespaceMeta(dataRoot, namespaceID, meta); err != nil {
		return 0, 0, err
	}
	return strategy, mode, nil
}

// resolveStrategyLocked applies the strategy rules against what was read
// from the marker. Split out of resolveNamespaceHistory only so the two
// axes read as the parallel decisions they are.
func resolveStrategyLocked(
	dataRoot, namespaceID string,
	requested, recorded storage.HistoryStrategy,
	found, namespaceIsNew bool,
) (storage.HistoryStrategy, error) {
	if recorded != storage.HistoryStrategyUnset {
		if requested != storage.HistoryStrategyUnset && requested != recorded {
			return 0, &HistoryStrategyMismatchError{
				NamespaceID: namespaceID, DataRoot: dataRoot,
				Recorded: recorded, Requested: requested,
			}
		}
		return recorded, nil
	}
	resolved := storage.DefaultHistoryStrategy
	switch {
	case found && !namespaceIsNew:
		// Pre-existing namespace, no marker: replay, because that is what
		// its bytes actually support.
		resolved = storage.HistoryStrategyReplay
		if requested != storage.HistoryStrategyUnset && requested != resolved {
			return 0, &HistoryStrategyMismatchError{
				NamespaceID: namespaceID, DataRoot: dataRoot,
				Recorded: resolved, Requested: requested,
			}
		}
	case requested != storage.HistoryStrategyUnset:
		resolved = requested
	}
	return resolved, nil
}

// resolveModeLocked applies the mode rules against what was read from the
// marker.
func resolveModeLocked(
	dataRoot, namespaceID string,
	requested, recorded storage.HistoryMode,
	found, namespaceIsNew bool,
) (storage.HistoryMode, error) {
	if recorded != storage.HistoryModeUnset {
		if requested != storage.HistoryModeUnset && requested != recorded {
			return 0, &HistoryModeMismatchError{
				NamespaceID: namespaceID, DataRoot: dataRoot,
				Recorded: recorded, Requested: requested,
			}
		}
		return recorded, nil
	}
	resolved := storage.DefaultHistoryMode
	switch {
	case found && !namespaceIsNew:
		// Pre-existing namespace, no marker: full. Nothing has ever
		// reclaimed a commit from it, so that is what it actually is, and
		// claiming otherwise would licence deleting segments that are
		// still the only record of their commits.
		resolved = storage.HistoryModeFull
		if requested != storage.HistoryModeUnset && requested != resolved {
			return 0, &HistoryModeMismatchError{
				NamespaceID: namespaceID, DataRoot: dataRoot,
				Recorded: resolved, Requested: requested,
			}
		}
	case requested != storage.HistoryModeUnset:
		resolved = requested
	}
	return resolved, nil
}
