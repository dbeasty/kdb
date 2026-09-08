package storage

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// HistoryMode selects how long a namespace keeps the past.
//
// This is a *retention* decision, not a data-model one. A commit hash is
// this engine's concurrency control - tx.BaseVersion is a commit hash,
// conflict detection is ancestry, idempotent retry is a transaction id
// mapped to the commit it produced - so both modes commit, hash, order and
// detect conflicts identically. What differs is how long a commit's
// remains survive: under HistoryModeFull, forever; under HistoryModeNone,
// as long as the retention window and no longer.
//
// Orthogonal to HistoryStrategy, which answers a different question (how a
// historical tree is *recovered*, by lookup or by replay) and only has an
// answer at all when there is history to recover. The two are coupled in
// one direction only: HistoryModeNone implies HistoryStrategyReplay,
// because tree objects would be written for commits that are about to be
// reclaimed.
//
// Like HistoryStrategy, a namespace records which mode it was built with
// and refuses to open under the other, because the two do not leave the
// same bytes on disk. See embed.MigrateHistoryMode for the offline
// conversion.
type HistoryMode int

const (
	// HistoryModeUnset means "whatever the namespace already is", and for
	// a new namespace, the default.
	HistoryModeUnset HistoryMode = iota

	// HistoryModeFull keeps every commit forever. Reads at any past
	// commit resolve, the commit graph is complete, and nothing is ever
	// reclaimed. What KDB has always done.
	HistoryModeFull

	// HistoryModeNone keeps only the current dataset plus a bounded
	// window of recent history (see RetentionWindow). Delta segments
	// wholly below both the checkpoint and the window are deleted, so the
	// on-disk footprint is a function of the dataset and the window
	// rather than of how many commits have ever been made.
	//
	// Inside the window a namespace behaves exactly as it would under
	// Full; outside it, the past does not exist and every operation that
	// would need it fails rather than answering from head.
	HistoryModeNone
)

func (h HistoryMode) String() string {
	switch h {
	case HistoryModeFull:
		return "full"
	case HistoryModeNone:
		return "none"
	default:
		return ""
	}
}

// ParseHistoryMode reads a mode name. The empty string is
// HistoryModeUnset rather than an error, so an absent setting and an
// absent marker mean the same thing everywhere - matching
// ParseHistoryStrategy.
func ParseHistoryMode(s string) (HistoryMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return HistoryModeUnset, nil
	case "full":
		return HistoryModeFull, nil
	case "none":
		return HistoryModeNone, nil
	default:
		return HistoryModeUnset, fmt.Errorf(
			"kdb: unknown history mode %q: want \"full\" or \"none\"", s)
	}
}

// DefaultHistoryMode is what a namespace created today uses. Full, because
// history is what this engine is for, and losing it must be something an
// operator asked for in as many words.
const DefaultHistoryMode = HistoryModeFull

// DefaultRetentionDuration is the window HistoryModeNone keeps when the
// caller does not choose one.
//
// A day, because the journal has to hold recent frames for crash recovery
// anyway, so the first hours of retention are very nearly free, and
// because "what did this look like yesterday" is the question people
// actually ask of a database that does not claim to keep history.
const DefaultRetentionDuration = 24 * time.Hour

// RetainNothing is the Duration that means "keep nothing beyond what the
// checkpoint has yet to absorb", as distinct from the zero value, which
// means "unset, take the default".
//
// A sentinel rather than a second boolean field because every consumer
// already has to handle the duration; a bool would let the two disagree.
// It is the only negative duration a window may carry, and validation
// rejects the rest.
const RetainNothing = time.Duration(-1)

// RetentionWindow bounds how much of the past HistoryModeNone keeps.
//
// Two independent floors, and a commit is reclaimable only when it is past
// *both*: older than Duration, and outside the most recent Commits. Set
// one, the other, or both; the more conservative wins.
//
// The window is a floor and never a ceiling. Deletion happens at
// sealed-segment granularity, because a segment is the unit that can be
// deleted, so a segment survives until its *newest* commit is outside the
// window - which means actual retention overshoots the window by up to one
// segment's time span. A deployment needing a hard upper bound on
// retention needs time-based segment rolling, which this is not.
//
// The window applies to HistoryModeNone alone. Under HistoryModeFull it is
// ignored: nothing is ever reclaimed there, whatever it says.
type RetentionWindow struct {
	// Duration keeps commits younger than this. Zero means the field is
	// unset - see Resolve - and RetainNothing means keep nothing beyond
	// what the checkpoint has yet to absorb.
	Duration time.Duration
	// Commits keeps this many of the most recent commits. Zero means
	// unset; the duration floor then decides alone.
	Commits int64
}

// IsZero reports whether neither floor was set.
func (w RetentionWindow) IsZero() bool { return w.Duration == 0 && w.Commits == 0 }

// Resolve fills in DefaultRetentionDuration when nothing was chosen, and
// normalizes "keep nothing" to a zero duration so callers downstream have
// one representation of it rather than two.
func (w RetentionWindow) Resolve() RetentionWindow {
	if w.IsZero() {
		return RetentionWindow{Duration: DefaultRetentionDuration}
	}
	if w.Duration < 0 {
		w.Duration = 0
	}
	if w.Commits < 0 {
		w.Commits = 0
	}
	return w
}

// String renders the window the way the configuration writes it.
func (w RetentionWindow) String() string {
	r := w.Resolve()
	switch {
	case r.Commits > 0 && r.Duration > 0:
		return fmt.Sprintf("%s or %d commits, whichever keeps more", r.Duration, r.Commits)
	case r.Commits > 0:
		return fmt.Sprintf("%d commits", r.Commits)
	case r.Duration > 0:
		return r.Duration.String()
	default:
		return "nothing beyond the checkpoint"
	}
}

// ParseRetentionDuration reads a window duration written the way an
// operator writes one: "24h", "80h", "7d", "90m", or "0" for none.
//
// Go's own time.ParseDuration has no day unit, and a retention window is
// one of the few places days are the natural unit, so "d" is accepted and
// expanded here rather than making every configuration say "168h".
func ParseRetentionDuration(s string) (time.Duration, error) {
	raw := strings.ToLower(strings.TrimSpace(s))
	if raw == "" {
		return 0, nil
	}
	if raw == "0" {
		// Explicit "keep nothing", which must be distinguishable from
		// unset by the caller that cares; see RetentionWindow.Resolve.
		return RetainNothing, nil
	}
	if strings.HasSuffix(raw, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(raw, "d"), 64)
		if err != nil {
			return 0, fmt.Errorf("kdb: unparseable retention duration %q", s)
		}
		if days < 0 {
			return 0, fmt.Errorf("kdb: negative retention duration %q", s)
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("kdb: unparseable retention duration %q: want a duration like \"24h\", \"7d\" or \"0\"", s)
	}
	if d < 0 {
		return 0, fmt.Errorf("kdb: negative retention duration %q", s)
	}
	return d, nil
}
