package transaction_test

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/transaction"
)

func TestLockManagerAcquireAndRelease(t *testing.T) {
	ns := "demo/users"
	doc, _ := codec.RandomUUID()
	locks := transaction.NewLockManager()

	if err := locks.TryAcquire(ns, doc, "sess-a"); err != nil {
		t.Fatal(err)
	}
	locks.Release(ns, doc, "sess-a")
	if err := locks.TryAcquire(ns, doc, "sess-b"); err != nil {
		t.Fatal(err)
	}
}

func TestLockManagerConflictDifferentSession(t *testing.T) {
	ns := "demo/users"
	doc, _ := codec.RandomUUID()
	locks := transaction.NewLockManager()

	if err := locks.TryAcquire(ns, doc, "sess-a"); err != nil {
		t.Fatal(err)
	}
	err := locks.TryAcquire(ns, doc, "sess-b")
	var locked *kdberr.DocumentLockedError
	if !errors.As(err, &locked) {
		t.Fatalf("expected DocumentLockedError, got %v", err)
	}
}

func TestLockManagerAcquireAllForTransactionReleasesPartialAcquisitionOnFailure(t *testing.T) {
	ns := "demo/users"
	doc, _ := codec.RandomUUID()
	doc2, _ := codec.RandomUUID()
	locks := transaction.NewLockManager()

	// doc2 is already held by another session, so AcquireAllForTransaction must fail...
	if err := locks.TryAcquire(ns, doc2, "sess-b"); err != nil {
		t.Fatal(err)
	}

	tx := newTx(codec.Hash{},
		document.WriteOp{DocID: doc, Patch: `{}`},
		document.DeleteOp{DocID: doc2},
	)
	_, err := locks.AcquireAllForTransaction(ns, "sess-a", tx, 0)
	var locked *kdberr.DocumentLockedError
	if !errors.As(err, &locked) {
		t.Fatalf("expected DocumentLockedError, got %v", err)
	}

	// ...and must not leak the lock it newly granted on `doc` before hitting the conflict.
	if err := locks.TryAcquire(ns, doc, "sess-c"); err != nil {
		t.Fatalf("expected doc lock to be released after failed AcquireAllForTransaction, got %v", err)
	}
}

func TestLockManagerAcquireAllForTransactionLeavesPreexistingLocksHeldOnFailure(t *testing.T) {
	ns := "demo/users"
	doc, _ := codec.RandomUUID()
	doc2, _ := codec.RandomUUID()
	locks := transaction.NewLockManager()

	// sess-a already holds `doc` from prior per-statement locking...
	if err := locks.TryAcquire(ns, doc, "sess-a"); err != nil {
		t.Fatal(err)
	}
	// ...and doc2 is held by a different session, so the whole call must fail.
	if err := locks.TryAcquire(ns, doc2, "sess-b"); err != nil {
		t.Fatal(err)
	}

	tx := newTx(codec.Hash{},
		document.WriteOp{DocID: doc, Patch: `{}`},
		document.DeleteOp{DocID: doc2},
	)
	_, err := locks.AcquireAllForTransaction(ns, "sess-a", tx, 0)
	var locked *kdberr.DocumentLockedError
	if !errors.As(err, &locked) {
		t.Fatalf("expected DocumentLockedError, got %v", err)
	}

	// The pre-existing lock on `doc` must survive the failed call, not be released.
	if err := locks.TryAcquire(ns, doc, "sess-c"); !errors.As(err, &locked) {
		t.Fatalf("expected doc to remain locked by sess-a, got %v", err)
	}
}
