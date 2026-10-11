package embed

import (
	"log/slog"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

// RecoveryReport is what opening a file-backed namespace found while bringing it back from its
// delta log. Read it with EmbeddedKdbRuntime.Recovery; it is fixed once open returns.
type RecoveryReport struct {
	// ReplayedInFull is true when open read the whole delta log rather than a checkpoint plus
	// the segments after it.
	ReplayedInFull bool `json:"replayedInFull"`
	// CommitsNotApplied counts logged commits replay stored in the history without applying them
	// to main, because main's head was not among their parents when they were read (and they did
	// not supersede it - see dag.SupersedesHead). A peer's commits, logged ahead of the merge that
	// adopts them, land here and are expected. In a log only one writer ever wrote this is zero.
	CommitsNotApplied int `json:"commitsNotApplied"`
	// CommitsOffMain is the part of CommitsNotApplied that main still did not reach once the
	// whole log had been read: no logged merge adopted them. Each was logged at WARN with its
	// namespace, commit and the head it did not extend. Legitimate only for a peer's commits a
	// crash cut off from their merge, which the next sync merges again; anything else is a
	// commit the log holds and the live data does not.
	CommitsOffMain int `json:"commitsOffMain"`
}

// Recovery reports what open found in this namespace's delta log. The zero report for a runtime
// that did not recover from a log (a pure in-memory one).
func (rt *EmbeddedKdbRuntime) Recovery() RecoveryReport {
	if rt == nil {
		return RecoveryReport{}
	}
	return rt.recovery
}

// maxOffMainWarnings bounds the per-commit WARN lines one replay emits; the count is always
// logged in full.
const maxOffMainWarnings = 20

// unappliedCommits collects, during one replay, the commits applyReplayedCommit stored without
// applying, with the head each one failed to extend.
type unappliedCommits struct {
	commits []unappliedCommit
}

type unappliedCommit struct {
	commit codec.Hash
	head   codec.Hash
}

func (u *unappliedCommits) add(commit, head codec.Hash) {
	if u == nil {
		return
	}
	u.commits = append(u.commits, unappliedCommit{commit: commit, head: head})
}

// report classifies the collected commits against main's head now that the log has been read,
// logs every one main never reached, and adds the counts to into (which may be nil).
func (u *unappliedCommits) report(d *dag.InMemoryCommitDag, into *RecoveryReport) {
	if u == nil || len(u.commits) == 0 {
		return
	}
	mainHead, err := d.Head()
	offMain := 0
	for _, c := range u.commits {
		if err == nil && (c.commit == mainHead || d.IsAncestor(c.commit, mainHead)) {
			continue // a merge on main adopted it
		}
		offMain++
		if offMain <= maxOffMainWarnings {
			slog.Warn("kdb: delta replay: logged commit stored but not applied - it does not extend main and no logged merge adopted it",
				"namespace", d.NamespaceID, "commit", c.commit.Hex(), "head", c.head.Hex(), "mainHead", mainHead.Hex())
		}
	}
	if offMain > maxOffMainWarnings {
		slog.Warn("kdb: delta replay: more logged commits stored but not applied than were listed",
			"namespace", d.NamespaceID, "offMain", offMain, "listed", maxOffMainWarnings)
	}
	if into != nil {
		into.CommitsNotApplied += len(u.commits)
		into.CommitsOffMain += offMain
	}
}
