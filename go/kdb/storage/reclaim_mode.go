package storage

import (
	"fmt"
	"strings"
	"time"
)

// How eagerly a namespace reclaims what its retention window has released.
//
// This is a separate axis from HistoryMode, and keeping them separate is the
// point. HistoryMode says what a namespace is *permitted* to reclaim;
// ReclaimMode says how eagerly it actually does it. Conflating them is what
// makes switching to HistoryModeNone feel irreversible: if turning the mode
// on also started deleting, an operator evaluating it would be committing to
// it. With the axes apart, the mode switch is metadata and the deletion is a
// separate decision - see docs/kdb-runtime-configurability-plan.md §3.
type ReclaimMode int

const (
	// ReclaimUnset means "whatever this namespace is already doing", and
	// for a new one, ReclaimBalanced.
	ReclaimUnset ReclaimMode = iota

	// ReclaimManual never reclaims anything on its own. Maintenance still
	// writes a checkpoint - that is not a deletion, and it is what lets the
	// next open skip the log - but no segment is deleted and no SSTable is
	// merged until someone asks explicitly.
	//
	// This is the mode a namespace should land in when it is switched to
	// HistoryModeNone, because turning a mode on should not by itself start
	// destroying data. Its honest cost is that disk is unbounded until
	// somebody acts, so anything reporting this mode should also report how
	// much is currently eligible.
	ReclaimManual

	// ReclaimImmediate reclaims as soon as there is anything to reclaim: a
	// pass runs when a segment is sealed, rather than waiting for the next
	// tick, and load never defers it. The smallest footprint, paid for in
	// continuous background work.
	ReclaimImmediate

	// ReclaimBalanced is the default: a pass every few minutes, only when
	// there is something to do, deferring to write load but never
	// indefinitely.
	ReclaimBalanced

	// ReclaimLazy trades disk for quiet. Long intervals, heavy deference to
	// load - for a namespace where reclamation matters but is not urgent.
	ReclaimLazy
)

func (r ReclaimMode) String() string {
	switch r {
	case ReclaimManual:
		return "manual"
	case ReclaimImmediate:
		return "immediate"
	case ReclaimBalanced:
		return "balanced"
	case ReclaimLazy:
		return "lazy"
	default:
		return ""
	}
}

// DefaultReclaimMode is what a namespace reclaims at when nothing says
// otherwise.
const DefaultReclaimMode = ReclaimBalanced

// ParseReclaimMode reads a mode name. The empty string is ReclaimUnset
// rather than an error, matching ParseHistoryMode: an absent setting and an
// absent marker mean the same thing.
func ParseReclaimMode(s string) (ReclaimMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return ReclaimUnset, nil
	case "manual":
		return ReclaimManual, nil
	case "immediate":
		return ReclaimImmediate, nil
	case "balanced":
		return ReclaimBalanced, nil
	case "lazy":
		return ReclaimLazy, nil
	default:
		return ReclaimUnset, fmt.Errorf(
			"kdb: unknown reclaim mode %q: want \"manual\", \"immediate\", \"balanced\" or \"lazy\"", s)
	}
}

// Reclaims reports whether this mode reclaims without being asked.
// ReclaimManual is the only one that does not.
func (r ReclaimMode) Reclaims() bool {
	return r.Resolve() != ReclaimManual
}

// Resolve turns ReclaimUnset into the default and leaves everything else
// alone. Idempotent, deliberately - RetentionWindow.Resolve was not, once,
// and a second resolve quietly changed the answer.
func (r ReclaimMode) Resolve() ReclaimMode {
	if r == ReclaimUnset {
		return DefaultReclaimMode
	}
	return r
}

// ReclaimCadence is the scheduler tuning a mode implies: how often to wake,
// how long an idle namespace may go without a pass, whether to wait for a
// quiet moment, and how many ticks load may postpone one.
//
// A preset, not a lock: a caller may set any of these individually
// afterwards, and the control plane reports what a preset resolved to rather
// than only the preset's name.
type ReclaimCadence struct {
	Interval time.Duration
	Sweep    time.Duration
	// RespectLoad is whether a pass waits for a moment with no write in
	// flight. False for ReclaimImmediate, which is what "immediate" has to
	// mean if it means anything.
	RespectLoad bool
	// MaxDefer bounds how long load may postpone a due pass. Only consulted
	// when RespectLoad is true. Always positive for a mode that defers at
	// all: the busiest server is the one whose log grows fastest, so
	// unbounded deferral would hand back the unbounded growth
	// HistoryModeNone exists to prevent.
	MaxDefer int
	// SealTriggered runs a pass as soon as a delta segment is sealed,
	// instead of waiting for the next tick. A sealed segment is the event
	// that creates something to reclaim - nothing below the open segment
	// changes until one is - so this is the difference between reacting to
	// the opportunity and happening upon it.
	SealTriggered bool
}

// Cadence is the scheduler tuning this mode implies.
func (r ReclaimMode) Cadence() ReclaimCadence {
	switch r.Resolve() {
	case ReclaimManual:
		// Still ticks, and still checkpoints: a checkpoint deletes nothing
		// and is what keeps the next open cheap. What it will not do is
		// reclaim, which the pass itself enforces rather than this cadence.
		return ReclaimCadence{Interval: 30 * time.Minute, Sweep: 6 * time.Hour, RespectLoad: true, MaxDefer: 24}
	case ReclaimImmediate:
		return ReclaimCadence{Interval: 10 * time.Second, Sweep: time.Minute, RespectLoad: false, SealTriggered: true}
	case ReclaimLazy:
		return ReclaimCadence{Interval: 30 * time.Minute, Sweep: 6 * time.Hour, RespectLoad: true, MaxDefer: 24}
	default: // ReclaimBalanced
		return ReclaimCadence{Interval: 5 * time.Minute, Sweep: 30 * time.Minute, RespectLoad: true, MaxDefer: 6}
	}
}
