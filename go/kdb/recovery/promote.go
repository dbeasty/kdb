package recovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/limidus/kdb/go/kdb/integrity"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Promotion: making a staged restore the live namespace.
//
// A restore into a staging directory can run while the server serves (see the control plane's
// restore.go). Promoting the result cannot: the live namespace's directory is inside the data root
// this process holds an exclusive lock on, and replacing a directory out from under open segment
// files is not something to attempt while anything is reading them.
//
// So promotion is split across a restart, which is what docs/kdb-control-ui-plan.md §8.4 means by a
// supervised flow. Two halves:
//
//   - While the server runs: the staged namespace is copied into the *live* data root, under
//     .kdb.promote/, and an intent file records what should happen. Copying here rather than at
//     boot is deliberate. The staging directory is meant to be on a different volume from the data
//     ("a restore is most needed exactly when the data volume is the problem"), so the move cannot
//     be a rename; doing the copy while the server is up means it can fail before anything is
//     committed to, an operator can watch it, and the restart stays short - a supervisor's start
//     timeout is not the place to discover a multi-gigabyte copy.
//   - At the next startup, before the data root is opened and while nothing holds its lock: Apply
//     verifies the payload, moves the live namespace aside, and renames the payload into place.
//     Both renames are inside one directory tree, so each is atomic.
//
// What makes this safe to interrupt is that every step is recoverable from what is on disk:
//
//   - The live namespace is *moved*, never deleted. superseded/ holds it until an operator removes
//     it, which is the difference between a promotion and a bet.
//   - Apply re-derives where it is from the filesystem, so a crash between the two renames is
//     resumed rather than compounded. See applyPayload.
//   - The payload is verified against the commit count recorded when it was staged, before the
//     live namespace is touched. A copy interrupted by a power cut is a truncated namespace, and
//     promoting one of those would be the worst outcome this whole path can produce.
//   - A failed Apply always clears the intent. A promotion that retried itself on every boot would
//     turn one bad request into a boot loop, which is worse than a promotion that did not happen.

const (
	// promoteIntentFile records a promotion the next startup should carry out.
	promoteIntentFile = ".kdb.promote.json"
	// promoteOutcomeFile records what the last one did, so the control plane can report the result
	// of a request whose whole point was that the process it was made to no longer exists.
	promoteOutcomeFile = ".kdb.promote.last.json"
	// promotePayloadDir holds the copied namespace, inside the data root so the install is a
	// rename rather than a copy.
	promotePayloadDir = ".kdb.promote"
	// SupersededDir is where a replaced namespace is kept. Not deleted: an operator who promotes
	// the wrong backup needs the old one back, and "it is still on disk" is the only answer that
	// works at that moment.
	SupersededDir = "superseded"
)

// PromotionIntent is a promotion requested by a running server, to be applied by the next startup.
type PromotionIntent struct {
	Namespace   string    `json:"namespace"`
	JobID       string    `json:"jobId,omitempty"`
	BackupID    string    `json:"backupId,omitempty"`
	RequestedAt time.Time `json:"requestedAt"`
	RequestedBy string    `json:"requestedBy,omitempty"`
	// StagedFrom is the staging root the payload was copied from, for the record only - by the time
	// Apply runs, nothing reads from it.
	StagedFrom string `json:"stagedFrom,omitempty"`
	// Commits is how many CRC-verified commits the payload held when it was staged. Apply refuses
	// a payload that no longer matches, which is what catches a copy cut short by a crash.
	Commits int `json:"commits"`
	// Bytes is the size of the copy, for reporting.
	Bytes int64 `json:"bytes"`
}

// PromotionOutcome is what an Apply did.
type PromotionOutcome struct {
	PromotionIntent
	// State is "promoted" or "failed".
	State     string    `json:"state"`
	AppliedAt time.Time `json:"appliedAt"`
	// Superseded is where the namespace that was replaced now lives, relative to the data root.
	Superseded string `json:"superseded,omitempty"`
	Error      string `json:"error,omitempty"`
}

// NamespaceDir is where a namespace's segments live under a data root.
func NamespaceDir(dataRoot, namespaceID string) string {
	return filepath.Join(dataRoot, "ns", filepath.FromSlash(namespaceID))
}

// checkpointPath is the namespace's open-time checkpoint, which describes the delta log it was
// written from. A checkpoint left behind after the log underneath it has been replaced would let
// the next open skip replaying data it has never seen, so promotion always moves it with - or
// removes it alongside - the segments it describes.
func checkpointPath(dataRoot, namespaceID string) string {
	return filepath.Join(dataRoot, "snap", filepath.FromSlash("kdb_checkpoint_"+namespaceID))
}

// StagePromotion copies a staged namespace into the live data root and writes the intent.
//
// It does not touch the live namespace: everything it writes is under .kdb.promote/, and until the
// intent file lands nothing will act on any of it. Returns the intent as recorded.
// marker is the namespace marker file (meta.json) the payload should carry when the staged copy
// has none of its own. A restore rebuilds the delta log and nothing else, so it never writes one,
// and the caller is the one that knows what the namespace it is replacing was built as. Nil means
// "leave it absent", which every reader treats as replay and full.
func StagePromotion(dataRoot, stagedRoot, namespaceID string, in PromotionIntent, marker []byte) (*PromotionIntent, error) {
	source := NamespaceDir(stagedRoot, namespaceID)
	info, err := os.Stat(source)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%s does not hold namespace %s (nothing at %s)",
			stagedRoot, namespaceID, source)
	}
	// Counted from the source, before the copy, so the number recorded is a property of the thing
	// being promoted rather than of the copy that might have gone wrong.
	commits, err := countVerifiedCommits(stagedRoot, namespaceID)
	if err != nil {
		return nil, fmt.Errorf("verifying the staged copy: %w", err)
	}
	if commits == 0 {
		return nil, fmt.Errorf("the staged copy of %s holds no verified commits; promoting it "+
			"would replace the live namespace with an empty one", namespaceID)
	}

	payload := filepath.Join(dataRoot, promotePayloadDir)
	// A leftover payload is from an abandoned request, not from this one.
	if err := os.RemoveAll(payload); err != nil {
		return nil, fmt.Errorf("clearing a previous staged promotion: %w", err)
	}
	bytes, err := copyTree(source, NamespaceDir(payload, namespaceID))
	if err != nil {
		_ = os.RemoveAll(payload)
		return nil, fmt.Errorf("copying the staged namespace into the data root: %w", err)
	}
	// The marker, when the staged copy has none. Written into the payload rather than into the
	// staged directory: the staged copy belongs to the restore job and may still be inspected, and
	// nothing should learn a new opinion about it because a promotion was requested.
	if len(marker) > 0 {
		if _, err := os.Stat(filepath.Join(source, "meta.json")); os.IsNotExist(err) {
			dest := filepath.Join(NamespaceDir(payload, namespaceID), "meta.json")
			if err := os.WriteFile(dest, marker, 0o644); err != nil {
				_ = os.RemoveAll(payload)
				return nil, fmt.Errorf("writing the namespace marker: %w", err)
			}
			bytes += int64(len(marker))
		}
	}

	// The staged copy's own checkpoint, if the restore left one. Absent is the normal case and not
	// a problem: without a checkpoint the next open replays the delta log, which is slower and
	// exactly as correct.
	if _, err := os.Stat(checkpointPath(stagedRoot, namespaceID)); err == nil {
		n, err := copyFile(checkpointPath(stagedRoot, namespaceID), checkpointPath(payload, namespaceID))
		if err != nil {
			_ = os.RemoveAll(payload)
			return nil, fmt.Errorf("copying the staged checkpoint: %w", err)
		}
		bytes += n
	}

	in.Namespace = namespaceID
	in.StagedFrom = stagedRoot
	in.Commits = commits
	in.Bytes = bytes
	if in.RequestedAt.IsZero() {
		in.RequestedAt = time.Now().UTC()
	}
	if err := writeJSONFile(filepath.Join(dataRoot, promoteIntentFile), in); err != nil {
		_ = os.RemoveAll(payload)
		return nil, fmt.Errorf("recording the promotion: %w", err)
	}
	return &in, nil
}

// PendingPromotion reports the promotion the next startup will carry out, if there is one.
func PendingPromotion(dataRoot string) (*PromotionIntent, error) {
	var in PromotionIntent
	found, err := readJSONFile(filepath.Join(dataRoot, promoteIntentFile), &in)
	if err != nil || !found {
		return nil, err
	}
	return &in, nil
}

// AbandonPromotion cancels a pending promotion and removes the copy it staged.
func AbandonPromotion(dataRoot string) (bool, error) {
	in, err := PendingPromotion(dataRoot)
	if err != nil || in == nil {
		return false, err
	}
	if err := os.Remove(filepath.Join(dataRoot, promoteIntentFile)); err != nil && !os.IsNotExist(err) {
		return false, err
	}
	// The intent goes first: with it gone, nothing will act on the payload even if removing it
	// fails halfway.
	if err := os.RemoveAll(filepath.Join(dataRoot, promotePayloadDir)); err != nil {
		return true, fmt.Errorf("the promotion was cancelled, but its staged copy could not be "+
			"removed: %w", err)
	}
	return true, nil
}

// LastPromotion reports what the most recent Apply did, if any.
func LastPromotion(dataRoot string) (*PromotionOutcome, error) {
	var out PromotionOutcome
	found, err := readJSONFile(filepath.Join(dataRoot, promoteOutcomeFile), &out)
	if err != nil || !found {
		return nil, err
	}
	return &out, nil
}

// ApplyPromotion carries out a pending promotion. Call it at startup, before the data root is
// opened: it renames directories inside the root and must be the only thing touching them.
//
// Returns nil when there is nothing to do. A promotion that fails is reported both in the returned
// outcome and in the outcome file, and the intent is cleared either way - see this file's header.
func ApplyPromotion(dataRoot string, now func() time.Time) (*PromotionOutcome, error) {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	in, err := PendingPromotion(dataRoot)
	if err != nil {
		return nil, err
	}
	if in == nil {
		return nil, nil
	}

	out := PromotionOutcome{PromotionIntent: *in, AppliedAt: now().UTC(), State: "promoted"}
	superseded, err := applyPayload(dataRoot, *in, now)
	if err != nil {
		out.State, out.Error = "failed", err.Error()
	}
	out.Superseded = superseded

	// The intent is cleared whatever happened, and before the outcome is written: if writing the
	// outcome fails, the promotion must still not be attempted again.
	if rmErr := os.Remove(filepath.Join(dataRoot, promoteIntentFile)); rmErr != nil && !os.IsNotExist(rmErr) {
		return &out, fmt.Errorf("could not clear the promotion intent at %s, so the next startup "+
			"would attempt it again: %w", dataRoot, rmErr)
	}
	_ = os.RemoveAll(filepath.Join(dataRoot, promotePayloadDir))
	if wErr := writeJSONFile(filepath.Join(dataRoot, promoteOutcomeFile), out); wErr != nil && err == nil {
		err = fmt.Errorf("the promotion succeeded but could not be recorded: %w", wErr)
	}
	return &out, err
}

// applyPayload does the two renames, resuming from wherever a previous attempt stopped.
//
// The filesystem is the state machine. Which step is next is derived from what exists, not from
// anything recorded, because a record written between two renames can itself be the thing that was
// lost.
func applyPayload(dataRoot string, in PromotionIntent, now func() time.Time) (superseded string, err error) {
	payload := filepath.Join(dataRoot, promotePayloadDir)
	if _, statErr := os.Stat(payload); statErr != nil {
		return "", fmt.Errorf("no staged copy at %s: the promotion of %s was recorded but its "+
			"payload is gone, so nothing was changed", payload, in.Namespace)
	}
	source := NamespaceDir(payload, in.Namespace)
	live := NamespaceDir(dataRoot, in.Namespace)

	if _, statErr := os.Stat(source); statErr == nil {
		// The payload is still where it was staged, so this is a first attempt (or one that stopped
		// before the second rename). Verify it before anything irreversible happens.
		commits, vErr := countVerifiedCommits(payload, in.Namespace)
		if vErr != nil {
			return "", fmt.Errorf("the staged copy could not be verified, so it was not promoted: %w", vErr)
		}
		if commits != in.Commits {
			return "", fmt.Errorf("the staged copy holds %d verified commit(s) but %d were recorded "+
				"when it was staged; it is incomplete or was changed, so it was not promoted",
				commits, in.Commits)
		}

		// Move the live namespace aside. Absent is fine and not a special case: a promotion into a
		// namespace this root does not have is a restore of something that was lost.
		if _, liveErr := os.Stat(live); liveErr == nil {
			stamp := now().UTC().Format("20060102T150405Z")
			rel := filepath.Join(SupersededDir, filepath.FromSlash(in.Namespace)+"-"+stamp)
			dest := filepath.Join(dataRoot, rel)
			if mkErr := os.MkdirAll(filepath.Dir(dest), 0o755); mkErr != nil {
				return "", mkErr
			}
			if mvErr := os.Rename(live, dest); mvErr != nil {
				return "", fmt.Errorf("moving the live namespace aside: %w", mvErr)
			}
			superseded = filepath.ToSlash(rel)
			// The old checkpoint describes the log that just moved, so it goes with it.
			if _, cErr := os.Stat(checkpointPath(dataRoot, in.Namespace)); cErr == nil {
				_ = os.MkdirAll(filepath.Join(dest, "_checkpoint"), 0o755)
				_ = os.Rename(checkpointPath(dataRoot, in.Namespace),
					filepath.Join(dest, "_checkpoint", "checkpoint"))
			}
		}

		if mkErr := os.MkdirAll(filepath.Dir(live), 0o755); mkErr != nil {
			return superseded, mkErr
		}
		if mvErr := os.Rename(source, live); mvErr != nil {
			return superseded, fmt.Errorf("installing the staged namespace: %w", mvErr)
		}
	}

	// Past the second rename, whether this attempt did it or a previous one did. What is left is
	// the checkpoint, which is idempotent either way.
	if _, cErr := os.Stat(checkpointPath(payload, in.Namespace)); cErr == nil {
		dest := checkpointPath(dataRoot, in.Namespace)
		if mkErr := os.MkdirAll(filepath.Dir(dest), 0o755); mkErr != nil {
			return superseded, mkErr
		}
		if mvErr := os.Rename(checkpointPath(payload, in.Namespace), dest); mvErr != nil {
			// A missing checkpoint costs a full replay, not correctness, so this is not worth
			// failing a completed promotion over - but a *stale* one would be read as describing
			// the new log, so it must go.
			_ = os.Remove(dest)
		}
	} else {
		// No checkpoint came with the payload: the live one, if any, describes a log that is no
		// longer there.
		_ = os.Remove(checkpointPath(dataRoot, in.Namespace))
	}
	if _, statErr := os.Stat(live); statErr != nil {
		return superseded, fmt.Errorf("the namespace is not in place at %s after the promotion", live)
	}
	return superseded, nil
}

// SameFilesystem reports whether two paths are on one filesystem, which is what makes the install
// a rename. Checked at request time so a promotion that could only half-work is refused before the
// operator is asked to restart anything.
//
// The nearest existing ancestor is used for a path that does not exist yet.
func SameFilesystem(a, b string) (bool, error) {
	da, err := deviceOf(a)
	if err != nil {
		return false, err
	}
	db, err := deviceOf(b)
	if err != nil {
		return false, err
	}
	return da == db, nil
}

func deviceOf(path string) (uint64, error) {
	for {
		info, err := os.Stat(path)
		if err == nil {
			return deviceID(info)
		}
		if !os.IsNotExist(err) {
			return 0, err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return 0, errors.New("no existing ancestor of " + path)
		}
		path = parent
	}
}

func countVerifiedCommits(dataRoot, namespaceID string) (int, error) {
	shim, err := shimFor(dataRoot)
	if err != nil {
		return 0, err
	}
	commits, err := integrity.ScanVerifiedCommits(shim, namespaceID)
	if err != nil {
		return 0, err
	}
	return len(commits), nil
}

func shimFor(dataRoot string) (storage.PlatformIOShim, error) {
	root := dataRoot
	store, err := storio.NewOSByteStore(storio.PlatformIOConfig{RootDirectory: &root})
	if err != nil {
		return nil, err
	}
	return storio.NewFileBackedPlatformIO(storio.PlatformIOConfig{RootDirectory: &root}, store), nil
}

// copyTree copies a directory recursively, returning the bytes written. Files are synced: this copy
// has to survive a crash between staging and the restart that installs it.
func copyTree(from, to string) (int64, error) {
	var total int64
	err := filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(to, rel)
		if d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			return os.MkdirAll(dest, info.Mode().Perm())
		}
		if !d.Type().IsRegular() {
			// A namespace directory holds regular files. Anything else is not something to guess at.
			return fmt.Errorf("%s is not a regular file", path)
		}
		n, err := copyFile(path, dest)
		total += n
		return err
	})
	return total, err
}

func copyFile(from, to string) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		return 0, err
	}
	src, err := os.Open(from)
	if err != nil {
		return 0, err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return 0, err
	}
	dst, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(dst, src)
	if err != nil {
		dst.Close()
		return n, err
	}
	if err := dst.Sync(); err != nil {
		dst.Close()
		return n, err
	}
	return n, dst.Close()
}

func writeJSONFile(path string, v any) error {
	body, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSONFile(path string, v any) (bool, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if err := json.Unmarshal(body, v); err != nil {
		return false, fmt.Errorf("%s is unreadable: %w", path, err)
	}
	return true, nil
}
