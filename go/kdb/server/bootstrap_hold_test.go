package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
)

// A long hold refuses writers at once - both those that arrive during it and those already queued
// when it began - while an internal acquireThroughHold still waits its turn, and once the hold
// ends the gate admits as before.
func TestWriteGateLongHoldRefusesWritersAtOnce(t *testing.T) {
	g := newWriteGate(8)
	release, err := g.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	queued := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r, err := g.acquire(ctx)
		if err == nil {
			r()
		}
		queued <- err
	}()
	through := make(chan error, 1)
	go func() {
		r, err := g.acquireThroughHold(context.Background())
		if err == nil {
			r()
		}
		through <- err
	}()
	time.Sleep(20 * time.Millisecond) // both are waiting for the running slot

	hold := &BusyError{RetryAfterMs: 1000, Reason: "bootstrapping"}
	g.beginLongHold(hold)
	select {
	case err := <-queued:
		var busy *BusyError
		if !errors.As(err, &busy) || busy != hold {
			t.Fatalf("a writer queued before the hold: %v, want the hold's busy error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a writer queued before the hold was not refused when it began")
	}
	start := time.Now()
	if _, err := g.acquire(context.Background()); err != hold {
		t.Fatalf("a writer during the hold: %v, want the hold's busy error", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("refusal took %v; it must not wait", d)
	}
	select {
	case err := <-through:
		t.Fatalf("acquireThroughHold returned during the hold: %v", err)
	default:
	}

	g.endLongHold()
	release()
	if err := <-through; err != nil {
		t.Fatalf("acquireThroughHold after the hold: %v", err)
	}
	r, err := g.acquire(context.Background())
	if err != nil {
		t.Fatalf("after the hold: %v", err)
	}
	r()
}

// While a peer snapshot installs, a client write to the namespace is refused at once with a BUSY
// that says why, instead of waiting out the write timeout behind the install; afterwards writes go
// through.
func TestWritesDuringSnapshotBootstrapAreRefusedAtOnce(t *testing.T) {
	rt := newTestRuntime(t)
	rt.WriteTimeout = 5 * time.Second
	node := serverLocalNode{rt: rt}

	inside, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- node.ExclusiveBootstrap(func() error {
			close(inside)
			<-finish
			return nil
		})
	}()
	<-inside

	start := time.Now()
	_, err := rt.Upsert("app/data", mustRandomUUID(t), `{"n":1}`, auth.Principal{})
	var busy *BusyError
	if !errors.As(err, &busy) {
		t.Fatalf("a write during the bootstrap: %v, want a busy error", err)
	}
	if !strings.Contains(busy.Reason, "bootstrapped") || busy.RetryAfterMs <= 0 {
		t.Fatalf("busy error %+v should say the namespace is being bootstrapped and when to retry", busy)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the write was refused after %v; it must not wait out the %v write timeout", d, rt.WriteTimeout)
	}

	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Upsert("app/data", mustRandomUUID(t), `{"n":2}`, auth.Principal{}); err != nil {
		t.Fatalf("a write after the bootstrap: %v", err)
	}
}
