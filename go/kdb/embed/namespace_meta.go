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

// resolveHistoryStrategy decides which strategy this open runs under, and
// records it for a namespace that does not have one yet.
//
// The rules, in order:
//
//   - A namespace with a recorded strategy keeps it. A caller asking for a
//     different one is refused, never silently accommodated.
//   - A namespace with no marker is "replay", whatever the caller asked
//     for: it was written before the setting existed, so it has no tree
//     objects, and calling it "objects" would mean claiming objects exist
//     for commits that never wrote any. It is recorded as replay so the
//     answer stops depending on how it is opened.
//   - A brand new namespace takes what the caller asked for, or
//     storage.DefaultHistoryStrategy.
//
// readOnly opens never write the marker; an unmarked namespace read this
// way resolves to replay for the life of the runtime without recording it.
func resolveHistoryStrategy(
	dataRoot, namespaceID string,
	requested storage.HistoryStrategy,
	namespaceIsNew, readOnly, force bool,
) (storage.HistoryStrategy, error) {
	if force && requested != storage.HistoryStrategyUnset {
		return requested, nil
	}
	meta, found := readNamespaceMeta(dataRoot, namespaceID)
	recorded, err := storage.ParseHistoryStrategy(meta.HistoryStrategy)
	if err != nil {
		return 0, err
	}

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

	if readOnly {
		return resolved, nil
	}
	if !found {
		meta.NamespaceID = namespaceID
	}
	meta.HistoryStrategy = resolved.String()
	if err := writeNamespaceMeta(dataRoot, namespaceID, meta); err != nil {
		return 0, err
	}
	return resolved, nil
}
