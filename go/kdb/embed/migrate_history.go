package embed

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// MigrateHistoryStrategy converts one namespace between history
// strategies, offline.
//
// Offline is not a limitation to work around later: the conversion writes
// the tree objects for every commit the namespace has (or stops writing
// them), and a live writer appending commits underneath that would leave a
// namespace whose marker says one thing and whose objects say another -
// exactly the state the marker exists to make impossible. It takes the
// data directory's exclusive maintenance lock, so it cannot run while any
// service, embedded runtime or read-only replica has the directory open,
// and none can start while it runs.
//
// Converting to objects replays the log once and records each commit's
// tree; converting to replay drops the marker and leaves the objects in
// place as dead weight, since they are addressed by tree hash and nothing
// will ask for them. Both are idempotent, and a failure part-way leaves
// the old marker intact - the namespace stays openable as what it was.
func MigrateHistoryStrategy(dataRoot, namespaceID string, to storage.HistoryStrategy) error {
	if to != storage.HistoryStrategyReplay && to != storage.HistoryStrategyObjects {
		return fmt.Errorf("kdb: migrate: target strategy must be \"replay\" or \"objects\"")
	}
	release, err := LockDataDir(dataRoot)
	if err != nil {
		return err
	}
	defer release()

	meta, found := readNamespaceMeta(dataRoot, namespaceID)
	if !found {
		return fmt.Errorf("kdb: migrate: namespace %q has no meta.json under %s", namespaceID, dataRoot)
	}
	from, err := storage.ParseHistoryStrategy(meta.HistoryStrategy)
	if err != nil {
		return err
	}
	if from == storage.HistoryStrategyUnset {
		from = storage.HistoryStrategyReplay
	}
	if from == to {
		// Still record it, so a namespace that was only implicitly replay
		// comes out of this explicitly marked.
		meta.HistoryStrategy = to.String()
		return writeNamespaceMeta(dataRoot, namespaceID, meta)
	}

	if to == storage.HistoryStrategyObjects {
		if err := buildTreeObjects(dataRoot, namespaceID); err != nil {
			return err
		}
	}
	meta.HistoryStrategy = to.String()
	return writeNamespaceMeta(dataRoot, namespaceID, meta)
}

// buildTreeObjects replays the namespace once under the objects strategy,
// so every commit's tree is recorded as an object, and flushes them.
//
// Deliberately goes through the ordinary open path with the strategy
// forced rather than reimplementing replay: the trees this produces have
// to be byte-identical to the ones the write path produces, and the only
// way to be sure of that is to use the same code.
func buildTreeObjects(dataRoot, namespaceID string) error {
	opts := FileRuntimeOptionsFromEnv()
	opts.Storage.HistoryStrategy = storage.HistoryStrategyObjects
	// The marker still says replay at this point, so the open would be
	// refused as a mismatch. forceHistoryStrategy is the one caller
	// allowed to bypass that check, holding the maintenance lock.
	opts.forceHistoryStrategy = true
	opts.alreadyLocked = true

	rt, err := OpenFileRuntimeWithOptions(dataRoot, CatalogFromNamespace(namespaceID), namespaceID, schema.None(), opts)
	if err != nil {
		return err
	}
	defer rt.Close()

	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return fmt.Errorf("kdb: migrate: namespace %q is not backed by the server engine", namespaceID)
	}
	return recordTreeObjectsForHistory(eng, rt.deltaReader)
}

// MigrateHistoryMode converts one namespace between history modes,
// offline.
//
// The two directions are not symmetric, and neither is reversible in the
// way an operator might hope:
//
//   - full → none is destructive. It does not delete anything itself, but
//     it puts the namespace under a retention window, and the next
//     maintenance pass will delete every segment past it. The history that
//     goes is gone; there is no undo but a backup.
//   - none → full keeps everything from the conversion onward, and cannot
//     invent what was already reclaimed. The namespace becomes
//     full-history with a history that starts now.
//
// Both record the new marker only after the work that backs it has
// succeeded, so a failure part-way leaves the namespace openable as what
// it was.
//
// Converting to none also forces the replay strategy: tree objects serve
// reads at commits this mode reclaims, so continuing to write them would
// be pure waste. Existing objects are left in place as dead weight,
// exactly as MigrateHistoryStrategy leaves them.
func MigrateHistoryMode(dataRoot, namespaceID string, to storage.HistoryMode) error {
	if to != storage.HistoryModeFull && to != storage.HistoryModeNone {
		return fmt.Errorf("kdb: migrate: target history mode must be \"full\" or \"none\"")
	}
	release, err := LockDataDir(dataRoot)
	if err != nil {
		return err
	}
	defer release()

	meta, found := readNamespaceMeta(dataRoot, namespaceID)
	if !found {
		return fmt.Errorf("kdb: migrate: namespace %q has no meta.json under %s", namespaceID, dataRoot)
	}
	from, err := storage.ParseHistoryMode(meta.HistoryMode)
	if err != nil {
		return err
	}
	if from == storage.HistoryModeUnset {
		from = storage.HistoryModeFull
	}
	if from == to {
		meta.HistoryMode = to.String()
		return writeNamespaceMeta(dataRoot, namespaceID, meta)
	}

	if to == storage.HistoryModeNone {
		// Everything the live tree names has to be durable outside the log
		// before this namespace is allowed to start deleting the log. Doing
		// it here, inside the maintenance lock, means the very first
		// maintenance pass after the conversion is already safe rather
		// than depending on a session having run under the new mode first.
		if err := prepareForNoneMode(dataRoot, namespaceID); err != nil {
			return err
		}
		meta.HistoryStrategy = storage.HistoryStrategyReplay.String()
	}
	meta.HistoryMode = to.String()
	return writeNamespaceMeta(dataRoot, namespaceID, meta)
}

// prepareForNoneMode opens the namespace under the target mode and makes
// every live document version durable in the blob store.
//
// Goes through the ordinary open path with the mode forced, for the same
// reason buildTreeObjects does: the bodies this writes have to be exactly
// what the read path will look for, and using the same code is the only
// way to be sure.
func prepareForNoneMode(dataRoot, namespaceID string) error {
	opts := FileRuntimeOptionsFromEnv()
	opts.Storage.HistoryMode = storage.HistoryModeNone
	opts.Storage.HistoryStrategy = storage.HistoryStrategyReplay
	opts.forceHistoryStrategy = true
	opts.alreadyLocked = true

	rt, err := OpenFileRuntimeWithOptions(dataRoot, CatalogFromNamespace(namespaceID), namespaceID, schema.None(), opts)
	if err != nil {
		return err
	}
	defer rt.Close()

	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return fmt.Errorf("kdb: migrate: namespace %q is not backed by the server engine", namespaceID)
	}
	return eng.PrepareForTruncation()
}
