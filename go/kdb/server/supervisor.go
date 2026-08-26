package server

import (
	"io"
	"log"
	"os"
	"time"
)

// AbortExitCode is the process exit code an orderly abort uses - EX_TEMPFAIL (BSD sysexits.h):
// "temporary failure... something like a host being unreachable... a request may succeed later."
// Distinguishing this from a config error (which should not be retried in a loop) is what lets a
// supervisor (Docker --restart=on-failure, systemd Restart=on-failure) know this is safe to
// restart, as opposed to something a human needs to fix first.
const AbortExitCode = 75

// AbortWatchdog triggers an orderly, logged shutdown when sustained memory pressure indicates the
// server cannot make progress - kdb-spec-layer13 Component 50, the direct answer to "better to
// crash and restart clean than linger in a degraded state - and that should not be needed."
//
// The "should not be needed" half is the design center: Components 47-49 (a restart path that
// never needs recovery, memory admission that is accurate and reversible rather than a permanent
// latch, and bounded write queues that reject before piling up) exist specifically to make sustained,
// unrecoverable pressure unreachable in normal operation. If this watchdog ever actually fires in
// production, that is itself a signal that the memory budget or admission thresholds need
// retuning - not a routine load-shedding mechanism to lean on.
//
// The "crash and restart clean" half is what actually fires: stop admitting new work
// (KdbServerRuntime.BeginDraining, so every new request gets a clear, typed *UnavailableError
// instead of a connection that just stops responding), give in-flight work a brief grace period,
// then flush and seal storage via EmbeddedKdbRuntime.Close (kdb-spec-layer13 Component 47 §4.5 -
// this was built specifically so shutdown never needs a separate "recovery mode": the same
// replay path that runs after a kill -9 also runs after this orderly exit), and finally exit with
// AbortExitCode so a supervisor restarts the process. This process never restarts itself - see
// this type's own doc comment on why that's someone else's job.
type AbortWatchdog struct {
	runtime      *KdbServerRuntime
	listener     io.Closer // may be nil if there's nothing wire-level to stop accepting on
	abortAfter   time.Duration
	pollInterval time.Duration
	exit         func(code int) // overridable for tests; defaults to os.Exit
	stop         chan struct{}
	done         chan struct{}
}

// NewAbortWatchdog returns a watchdog that triggers an orderly abort once runtime's memory guard
// has reported sustained pressure (ShouldReject() staying true) for at least abortAfter.
// listener, if non-nil, is closed as part of the abort sequence (stop accepting new connections
// before draining existing work) - typically the *Listener from ListenSqlWire. Returns nil (a
// nil-safe no-op per method) if abortAfter <= 0, matching MemoryGuard's own opt-in-by-nonzero
// convention.
func NewAbortWatchdog(runtime *KdbServerRuntime, listener io.Closer, abortAfter time.Duration) *AbortWatchdog {
	if abortAfter <= 0 {
		return nil
	}
	return &AbortWatchdog{
		runtime:      runtime,
		listener:     listener,
		abortAfter:   abortAfter,
		pollInterval: 200 * time.Millisecond,
		exit:         os.Exit,
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Start begins polling in the background. Nil-safe (a disabled watchdog does nothing).
func (w *AbortWatchdog) Start() {
	if w == nil {
		return
	}
	go w.run()
}

// Stop halts polling without triggering an abort - e.g. on an ordinary, deliberate shutdown that
// isn't a response to pressure. Nil-safe. Safe to call even if the watchdog already aborted (the
// abort path itself closes w.done before calling exit, in case exit is overridden for a test and
// doesn't actually terminate the process).
func (w *AbortWatchdog) Stop() {
	if w == nil {
		return
	}
	select {
	case <-w.done:
		return // already aborted (or already stopped) - closing w.stop again would panic
	default:
	}
	close(w.stop)
	<-w.done
}

func (w *AbortWatchdog) run() {
	defer close(w.done)
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()
	var pressureSince time.Time
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			if !w.runtime.memGuard.ShouldReject() {
				pressureSince = time.Time{}
				continue
			}
			if pressureSince.IsZero() {
				pressureSince = time.Now()
				continue
			}
			if time.Since(pressureSince) >= w.abortAfter {
				w.abort()
				return
			}
		}
	}
}

func (w *AbortWatchdog) abort() {
	log.Printf(
		"kdb: orderly abort triggered - memory pressure sustained for >= %s with no recovery "+
			"(kdb-spec-layer13 Component 50). This should not happen under normal operation - if "+
			"it did, the memory budget or admission thresholds most likely need retuning, not a "+
			"reason to rely on this path routinely.",
		w.abortAfter)

	w.runtime.BeginDraining()
	if w.listener != nil {
		if err := w.listener.Close(); err != nil {
			log.Printf("kdb: abort: closing listener: %v", err)
		}
	}
	// Brief grace period: WriteTimeout-bounded in-flight commits get a chance to finish (or hit
	// their own deadline and return *DeadlineExceededError to their caller) before storage closes
	// out from under them. Not load-bearing for correctness either way - see Close's own doc
	// comment - just kinder to whatever was already in flight.
	time.Sleep(250 * time.Millisecond)

	if w.runtime.Runtime != nil {
		w.runtime.Runtime.Close()
	}
	log.Printf("kdb: orderly abort complete - exiting with code %d for the supervisor to restart "+
		"(Docker --restart=on-failure / systemd Restart=on-failure)", AbortExitCode)
	// w.exit is os.Exit in production, which never returns - run()'s own deferred close(w.done)
	// never executes and that's fine, the process is gone. A test that overrides exit to record
	// the call instead of terminating relies on that same defer to close w.done once abort()
	// returns here, which is why abort() must not close it itself (that would double-close it).
	w.exit(AbortExitCode)
}
