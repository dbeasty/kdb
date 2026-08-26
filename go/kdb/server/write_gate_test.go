package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
)

// TestWriteGateRejectsWhenQueueFull proves kdb-spec-layer13 Component 49 §6.2's first outcome:
// a caller arriving once the queue is already at capacity is rejected immediately with
// *BusyError, not left to block indefinitely alongside everyone else.
func TestWriteGateRejectsWhenQueueFull(t *testing.T) {
	g := newWriteGate(1)

	// Fill the single running slot so every queued() slot is genuinely occupied waiting on it.
	releaseRunning, err := g.acquire(context.Background())
	if err != nil {
		t.Fatalf("expected the first acquire to succeed immediately: %v", err)
	}
	defer releaseRunning()

	// Occupy the one queue slot with a waiter that will sit blocked on <-running until we say so.
	blockedDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := g.acquire(ctx); err != nil {
			t.Logf("blocked waiter: %v", err)
		}
		close(blockedDone)
	}()
	// Give the goroutine time to actually reach the queued<- send before probing capacity.
	time.Sleep(50 * time.Millisecond)

	_, err = g.acquire(context.Background())
	var busy *BusyError
	if !errors.As(err, &busy) {
		t.Fatalf("expected *BusyError once the queue is at capacity, got %T: %v", err, err)
	}
	if busy.RetryAfterMs <= 0 {
		t.Fatalf("expected a positive RetryAfterMs hint, got %d", busy.RetryAfterMs)
	}

	releaseRunning()
	<-blockedDone
}

// TestWriteGateRejectsOnDeadlineExceeded proves the second outcome: a caller that queues
// successfully but whose own deadline passes before its turn arrives gets
// *DeadlineExceededError, not an eventual (likely useless, by then) success.
func TestWriteGateRejectsOnDeadlineExceeded(t *testing.T) {
	g := newWriteGate(4)
	release, err := g.acquire(context.Background())
	if err != nil {
		t.Fatalf("expected the first acquire to succeed: %v", err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err = g.acquire(ctx)
	var deadline *DeadlineExceededError
	if !errors.As(err, &deadline) {
		t.Fatalf("expected *DeadlineExceededError, got %T: %v", err, err)
	}
}

// TestCommitWithReturnsBusyWhenWriteQueueIsFull proves the wiring reaches KdbServerRuntime, not
// just the gate in isolation.
func TestCommitWithReturnsBusyWhenWriteQueueIsFull(t *testing.T) {
	srv := newTestRuntime(t)
	srv.writeGate = newWriteGate(1)
	srv.WriteTimeout = 2 * time.Second

	release, err := srv.writeGate.acquire(context.Background())
	if err != nil {
		t.Fatalf("expected the running slot to be free initially: %v", err)
	}
	defer release()

	blockedDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		g := srv.writeGate
		if _, err := g.acquire(ctx); err != nil {
			t.Logf("blocked waiter: %v", err)
		}
		close(blockedDone)
	}()
	time.Sleep(50 * time.Millisecond)

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = srv.Upsert("app/data", docID, `{"v":1}`, auth.Principal{})
	var busy *BusyError
	if !errors.As(err, &busy) {
		t.Fatalf("expected Upsert to return *BusyError while the write queue is full, got %T: %v", err, err)
	}

	release()
	<-blockedDone
}

// TestBeginDrainingRejectsNewWrites proves kdb-spec-layer13 Component 50's first step of an
// orderly shutdown: once draining, every new write is rejected with *UnavailableError
// immediately, ahead of the memory-pressure check and the write gate both.
func TestBeginDrainingRejectsNewWrites(t *testing.T) {
	srv := newTestRuntime(t)
	srv.BeginDraining()

	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = srv.Upsert("app/data", docID, `{"v":1}`, auth.Principal{})
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("expected *UnavailableError once draining, got %T: %v", err, err)
	}
}
