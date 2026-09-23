// Package script runs KDB's stored procedures on the Go runtime.
//
// It is the Go half of Component 32 (docs/kdb-spec-layer11-component32-stored-procedures.md),
// whose first implementation was Kotlin's kdb-script on GraalJS. The two run the same restricted
// JavaScript: a procedure is a program that defines main(args) and returns a value, and the same
// source is expected to give the same answer on either runtime.
//
// Procedures run in one of two modes. ModePure has no access to anything outside its arguments -
// no clock, no randomness, no database - because its result is covered by a merge commit's hash
// and so must be identical on every node that makes that merge. ModeRead may read the database;
// it runs after a merge, on one node, where a decision is an ordinary write.
package script

import (
	"errors"
	"time"
)

// Limits bound one procedure call. They mirror Kotlin's ProcLimits, minus the statement counter,
// which GraalVM provides and goja does not: on Go, a runaway procedure is stopped by WallClock.
type Limits struct {
	// WallClock is how long a call may run before it is interrupted.
	WallClock time.Duration
	// MaxOutputBytes caps the JSON a procedure may return.
	MaxOutputBytes int
	// MaxLogBytes caps what kdb.log may accumulate.
	MaxLogBytes int
	// MaxHostCalls caps kdb.* calls in ModeRead.
	MaxHostCalls int
}

// DefaultLimits matches Kotlin's ProcLimits.DEFAULT where the two overlap.
var DefaultLimits = Limits{
	WallClock:      5 * time.Second,
	MaxOutputBytes: 1 << 20,
	MaxLogBytes:    64 * 1024,
	MaxHostCalls:   1000,
}

func (l Limits) withDefaults() Limits {
	if l.WallClock <= 0 {
		l.WallClock = DefaultLimits.WallClock
	}
	if l.MaxOutputBytes <= 0 {
		l.MaxOutputBytes = DefaultLimits.MaxOutputBytes
	}
	if l.MaxLogBytes <= 0 {
		l.MaxLogBytes = DefaultLimits.MaxLogBytes
	}
	if l.MaxHostCalls <= 0 {
		l.MaxHostCalls = DefaultLimits.MaxHostCalls
	}
	return l
}

// Errors a call can fail with. They are distinguished because the in-merge caller treats every
// one of them the same way - it holds the merge - while the authority caller retries some and
// gives up on others.
var (
	// ErrCompile: the source is not valid JavaScript, or defines no main.
	ErrCompile = errors.New("procedure failed to compile")
	// ErrTimeout: the call ran past Limits.WallClock and was interrupted.
	ErrTimeout = errors.New("procedure timed out")
	// ErrRuntime: the procedure threw.
	ErrRuntime = errors.New("procedure threw")
	// ErrOutput: the procedure returned something that is not a valid result.
	ErrOutput = errors.New("procedure returned an invalid result")
	// ErrDenied: the procedure called a host function its mode does not allow.
	ErrDenied = errors.New("procedure made a call its mode does not allow")
	// ErrNotFound: no procedure of that name is defined in the namespace.
	ErrNotFound = errors.New("no such procedure")
)
