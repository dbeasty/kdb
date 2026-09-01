package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
)

func newDocID(t *testing.T) string {
	t.Helper()
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	return id.String()
}

// TestPutIfAbsentCreatesExactlyOnce is the insert-if-not-exists contract: the first caller wins,
// the second is refused, and the stored value is the first one's.
func TestPutIfAbsentCreatesExactlyOnce(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	docID := newDocID(t)

	if _, err := c.PutIfAbsent(ctx, "app/data", docID, []byte(`{"owner":"first"}`)); err != nil {
		t.Fatalf("first PutIfAbsent should succeed: %v", err)
	}
	_, err := c.PutIfAbsent(ctx, "app/data", docID, []byte(`{"owner":"second"}`))
	if !errors.Is(err, client.ErrPreconditionFailed) {
		t.Fatalf("second PutIfAbsent should fail the precondition, got %v", err)
	}

	body, _, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"owner":"first"}` {
		t.Fatalf("the loser overwrote the winner: %s", body)
	}
}

// TestReplaceIfFailsOnIdenticalContent is the sharp edge of compare-and-set, and the reason it
// cannot be built on top of KDB's ordinary conflict detection. Conflict detection is
// content-addressed: a write whose content already equals what is stored is indistinguishable
// from a no-op and passes. A CAS asserts something different - that the state is *still the one
// I read* - so it must fail on a stale hash even when the incoming bytes happen to match.
func TestReplaceIfFailsOnIdenticalContent(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	docID := newDocID(t)

	if _, err := c.PutIfAbsent(ctx, "app/data", docID, []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	_, staleHash, err := c.GetJSONWithHash(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}

	// Somebody else moves the document on.
	if _, err := c.Upsert(ctx, "app/data", docID, []byte(`{"v":2}`)); err != nil {
		t.Fatal(err)
	}

	// Now write content identical to what is currently stored, but under the stale hash. Ordinary
	// conflict detection would wave this through as a no-op; the precondition must not.
	_, err = c.ReplaceIf(ctx, "app/data", docID, []byte(`{"v":2}`), staleHash)
	if !errors.Is(err, client.ErrPreconditionFailed) {
		t.Fatalf("a content-identical write under a stale hash was accepted: %v", err)
	}

	var pre *client.PreconditionError
	if errors.As(err, &pre) && pre.ActualHash == "" {
		t.Fatal("the failure did not report the hash that actually beat it, so a caller cannot recover")
	}
}

// TestReplaceIfSucceedsOnCurrentHash is the negative control for the test above.
func TestReplaceIfSucceedsOnCurrentHash(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	docID := newDocID(t)

	if _, err := c.PutIfAbsent(ctx, "app/data", docID, []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	_, hash, err := c.GetJSONWithHash(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReplaceIf(ctx, "app/data", docID, []byte(`{"v":2}`), hash); err != nil {
		t.Fatalf("CAS against the current hash should succeed: %v", err)
	}
	body, _, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"v":2}` {
		t.Fatalf("CAS did not apply: %s", body)
	}
}

// TestCompareAndSwapNoLostUpdates is the multi-writer guarantee for read-modify-write: eight
// concurrent clients each increment the same counter, and the final value is exactly eight.
//
// Without preconditions this is the textbook lost update - two clients read 3, both write 4, and
// one increment vanishes with no error reported to anybody.
func TestCompareAndSwapNoLostUpdates(t *testing.T) {
	addr, _ := startTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	docID := newDocID(t)

	seed := connectTestClient(t, addr)
	if _, err := seed.PutIfAbsent(ctx, "app/data", docID, []byte(`{"n":0}`)); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := connectTestClient(t, addr)
			<-start
			// Generous attempt budget: with eight writers contending, a given one can lose
			// several rounds in a row without anything being wrong.
			_, err := c.CompareAndSwap(ctx, "app/data", docID, 50, func(current []byte) ([]byte, error) {
				var doc struct {
					N int `json:"n"`
				}
				if err := json.Unmarshal(current, &doc); err != nil {
					return nil, err
				}
				doc.N++
				return json.Marshal(doc)
			})
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d failed: %v", i, err)
		}
	}

	body, _, err := seed.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	var final struct {
		N int `json:"n"`
	}
	if err := json.Unmarshal(body, &final); err != nil {
		t.Fatal(err)
	}
	if final.N != writers {
		t.Fatalf("lost update: expected %d increments, counter reads %d (%s)", writers, final.N, body)
	}
}

// TestCompareAndSwapSeedsMissingDocument covers the create branch: update sees nil when nothing
// is there, and CompareAndSwap switches to insert-if-absent on its behalf.
func TestCompareAndSwapSeedsMissingDocument(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	docID := newDocID(t)

	sawNil := false
	if _, err := c.CompareAndSwap(ctx, "app/data", docID, 3, func(current []byte) ([]byte, error) {
		if current == nil {
			sawNil = true
			return []byte(`{"n":1}`), nil
		}
		return nil, fmt.Errorf("expected a missing document, got %s", current)
	}); err != nil {
		t.Fatalf("seeding CAS failed: %v", err)
	}
	if !sawNil {
		t.Fatal("update was not told the document was missing")
	}
	body, _, err := c.GetJSON(ctx, "app/data", docID)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"n":1}` {
		t.Fatalf("seeded value not stored: %s", body)
	}
}

// TestCompareAndSwapDoesNotRetryNonRaceErrors: a schema or constraint failure will fail
// identically on every attempt, so retrying it only burns the caller's budget and the server's
// write gate. Asserted by counting how many times update ran.
func TestCompareAndSwapDoesNotRetryNonRaceErrors(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	docID := newDocID(t)

	calls := 0
	sentinel := errors.New("caller decided to stop")
	_, err := c.CompareAndSwap(ctx, "app/data", docID, 5, func(current []byte) ([]byte, error) {
		calls++
		return nil, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the caller's own error to surface unchanged, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("update ran %d times for a non-retryable failure, want 1", calls)
	}
}
