package client

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The server's hint is preferred over the client's own schedule because only the server can see
// the whole queue - and it has already jittered the number per response, which is the part a
// client cannot do for the herd it is a member of.
func TestRetryHintPrefersServerValue(t *testing.T) {
	if got := retryHint(&ConflictError{RetryAfterMs: 42}); got != 42*time.Millisecond {
		t.Fatalf("conflict hint: %v", got)
	}
	if got := retryHint(&PreconditionError{RetryAfterMs: 17}); got != 17*time.Millisecond {
		t.Fatalf("precondition hint: %v", got)
	}
	// No hint (an older server) must read as zero, so the caller falls back to its own backoff
	// rather than treating "unset" as "retry immediately".
	if got := retryHint(&ConflictError{}); got != 0 {
		t.Fatalf("absent hint should be 0, got %v", got)
	}
	if got := retryHint(errors.New("something else")); got != 0 {
		t.Fatalf("unrelated error should carry no hint, got %v", got)
	}
}

// Without a server hint the client backs off on its own, and the delay must stay bounded however
// many attempts have failed - an unbounded exponential parks a caller for minutes, and an
// unguarded shift wraps negative.
func TestBackoffDelayIsBoundedWithoutHint(t *testing.T) {
	plain := errors.New("no hint here")
	for _, attempt := range []int{0, 1, 5, 20, 100, 1 << 20} {
		for i := 0; i < 200; i++ {
			d := backoffDelay(attempt, plain)
			if d < 0 || d > backoffCapMs*time.Millisecond {
				t.Fatalf("attempt %d drew %v, outside [0, %dms]", attempt, d, backoffCapMs)
			}
		}
	}
}

// Early attempts must actually be short: a schedule that jumped straight to the cap would turn a
// single unlucky collision into a quarter-second stall.
func TestBackoffDelayGrowsWithAttempt(t *testing.T) {
	plain := errors.New("no hint here")
	for i := 0; i < 200; i++ {
		if d := backoffDelay(0, plain); d > backoffBaseMs*time.Millisecond {
			t.Fatalf("first attempt drew %v, past the %dms initial ceiling", d, backoffBaseMs)
		}
	}
}

// A backoff that ignored ctx would turn a cancelled caller into one still asleep for the cap.
func TestWaitBackoffHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := waitBackoff(ctx, 0, &ConflictError{RetryAfterMs: 5_000})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// Full jitter is the load-bearing part: N clients handed the same delay collide again at that
// delay. Distinct draws are what spread them out.
func TestBackoffDelayJitters(t *testing.T) {
	plain := errors.New("no hint here")
	seen := map[time.Duration]int{}
	for i := 0; i < 200; i++ {
		seen[backoffDelay(6, plain)]++
	}
	if len(seen) < 5 {
		t.Fatalf("expected jittered delays, got %d distinct values across 200 draws", len(seen))
	}
}
