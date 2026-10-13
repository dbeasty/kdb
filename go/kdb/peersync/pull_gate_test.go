package peersync

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
)

func newGateDag(t *testing.T) *dag.InMemoryCommitDag {
	t.Helper()
	d, err := dag.NewInMemoryCommitDag("app/gate")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestPullGateTakesTurns: a second pull of the same DAG waits for the first, and sees the tips it
// stored; a pull of another DAG - another node, even with the same namespace name - does not wait.
func TestPullGateTakesTurns(t *testing.T) {
	d, other := newGateDag(t), newGateDag(t)
	g1, held := enterPull(d, time.Second)
	if !held {
		t.Fatal("the first pull must get its turn at once")
	}
	if g, held := enterPull(other, 10*time.Millisecond); !held {
		t.Fatal("a pull of another DAG must not wait")
	} else {
		leavePull(other, g, held)
	}
	tip := codec.Hash{Bytes: [32]byte{1}}
	g1.share([]codec.Hash{tip})
	got := make(chan []codec.Hash)
	go func() {
		g2, held := enterPull(d, 5*time.Second)
		defer leavePull(d, g2, held)
		if !held {
			got <- nil
			return
		}
		got <- g2.shared()
	}()
	select {
	case <-got:
		t.Fatal("the second pull went ahead while the first held its turn")
	case <-time.After(50 * time.Millisecond):
	}
	leavePull(d, g1, true)
	if tips := <-got; len(tips) != 1 || tips[0] != tip {
		t.Fatalf("expected the first pull's tip as a have, got %v", tips)
	}
	pullGates.Lock()
	left := len(pullGates.m)
	pullGates.Unlock()
	if left != 0 {
		t.Fatalf("gates must be dropped once no pull holds or awaits them, %d left", left)
	}
}

// TestPullGateWaitIsBounded: a pull stuck holding its turn (a stalled peer) delays another by at
// most the wait, then the other goes ahead on its own.
func TestPullGateWaitIsBounded(t *testing.T) {
	d := newGateDag(t)
	g1, _ := enterPull(d, time.Second)
	start := time.Now()
	g2, held := enterPull(d, 30*time.Millisecond)
	if held {
		t.Fatal("expected the wait to time out")
	}
	if waited := time.Since(start); waited < 30*time.Millisecond || waited > time.Second {
		t.Fatalf("waited %v", waited)
	}
	leavePull(d, g2, held)
	leavePull(d, g1, true)
	// The turn is free again.
	g3, held := enterPull(d, 10*time.Millisecond)
	if !held {
		t.Fatal("the turn was not released")
	}
	leavePull(d, g3, held)
}
