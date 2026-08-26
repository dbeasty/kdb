// kdb-pressure-test drives sustained write load against a running kdb-service instance to verify
// kdb-spec-layer13's resource-governance behavior end to end, over the real wire protocol
// (go/kdb/client) - not just in-process unit tests. Three things a load-throughput tool like
// kdb-loadtest doesn't check, which matter specifically for this layer:
//
//  1. Under pressure, clients see typed, actionable errors (errors.Is(err, client.ErrBusy) /
//     client.ErrUnavailable), not dropped connections or generic failures - kdb-spec-layer13
//     Component 51.
//  2. Pressure is reversible: once load eases, writes succeed again without restarting the
//     server or reconfiguring anything - the "no zombie" requirement (Component 48 §2.5).
//  3. Whatever got written before a restart (forced or otherwise) is still there afterward - a
//     -verify-doc-id/-verify-doc-value pair lets the driving script check this across a
//     container restart specifically (Component 47).
//
// Prints one final machine-parseable "RESULT ..." line the driving shell script greps for.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "kdb-service SQL wire address")
	namespace := flag.String("namespace", "demo/pressure", "namespace to write into")
	concurrency := flag.Int("concurrency", 32, "concurrent client connections during the burst")
	burstDuration := flag.Duration("burst-duration", 20*time.Second, "how long to hammer writes")
	docBytes := flag.Int("doc-bytes", 2000, "approximate JSON document size in bytes - larger documents reach a given memory budget in fewer writes")
	docPool := flag.Int("doc-pool", 300, "fixed number of documents to cycle through during the burst, matching kdb-loadtest's own bounded-working-set design (see that tool's doc comment): repeatedly overwriting the same documents is what -recovered actually needs to measure - an ever-growing set of brand-new document ids instead measures kdb-spec-layer13 §10's separate, already-documented 'uncompacted DAG grows without bound' limitation, which is a real but different question from whether transient pressure clears (§2.5)")
	maxSuccessfulWrites := flag.Int("max-successful-writes", 800, "stop the burst once this many writes have actually succeeded, even if -burst-duration hasn't elapsed. This is what actually bounds permanent DAG growth for the -recovered check: cycling a bounded -doc-pool bounds distinct *documents*, but every successful Upsert is still a brand new permanent commit regardless of whether its document id repeats (kdb-spec-layer13 §2.11's 'every write is a permanent commit, nothing evicts history') - without this cap, a long enough or fast enough burst against a small memory budget will permanently outgrow it and never show recovered=true, which would be correctly measuring §10's separate, already-documented compaction gap instead of the §2.5 latch fix this flag exists to isolate. 0 disables the cap (burst-duration alone decides)")
	cooldown := flag.Duration("cooldown", 3*time.Second, "idle period after the burst, before checking whether pressure has cleared")
	verifyWrites := flag.Int("verify-writes", 20, "writes attempted after the cooldown, to check whether pressure recovered")
	verifyDocID := flag.String("verify-doc-id", "", "if set, read this doc id back at the very end (see -verify-doc-write) and confirm it matches -verify-doc-value - proves data survives whatever happened during the run (a restart, an abort) with no corruption")
	verifyDocValue := flag.String("verify-doc-value", "", "the JSON body expected at -verify-doc-id")
	verifyDocWrite := flag.Bool("verify-doc-write", true, "write -verify-doc-id/-verify-doc-value once before the burst starts (the normal case: nothing exists there yet). Set false for a post-restart check run against a server that already has this document from an earlier run - a write here would go through the same memory-pressure gate real writes do, which defeats the point of a check that should work purely off GetJSON (never gated - see MemoryGuard's own doc comment) regardless of the server's current write-admission state")
	connectTimeout := flag.Duration("connect-timeout", 60*time.Second, "how long to wait for the server to accept connections (e.g. while it's replaying a delta log after a restart)")
	flag.Parse()

	pad := ""
	for len(pad) < *docBytes-40 {
		pad += "x"
	}

	if *verifyDocID != "" && *verifyDocWrite {
		if err := retryingUpsert(*addr, *namespace, *verifyDocID, []byte(*verifyDocValue), *connectTimeout); err != nil {
			fmt.Fprintf(os.Stderr, "pre-burst verify-doc write failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("pre-burst: wrote verify doc %s\n", *verifyDocID)
	}

	var (
		successes, busy, unavailable, deadlineExceeded, otherErrs int64
	)
	classify := func(err error) {
		switch {
		case err == nil:
			atomic.AddInt64(&successes, 1)
		case errors.Is(err, client.ErrBusy):
			atomic.AddInt64(&busy, 1)
		case errors.Is(err, client.ErrUnavailable):
			atomic.AddInt64(&unavailable, 1)
		case errors.Is(err, client.ErrDeadlineExceeded):
			atomic.AddInt64(&deadlineExceeded, 1)
		default:
			atomic.AddInt64(&otherErrs, 1)
		}
	}

	docIDs := make([]string, *docPool)
	for i := range docIDs {
		id, err := codec.RandomUUID()
		if err != nil {
			fmt.Fprintf(os.Stderr, "generating doc pool: %v\n", err)
			os.Exit(1)
		}
		docIDs[i] = id.String()
	}

	fmt.Printf("burst: %d workers writing ~%dB documents (pool of %d) for %s ...\n", *concurrency, *docBytes, *docPool, *burstDuration)
	burstCtx, burstCancel := context.WithTimeout(context.Background(), *burstDuration+*connectTimeout)
	defer burstCancel()
	// A context.Done() channel, not a time.Timer.C one: Timer.C delivers its value to exactly
	// one receiver ever, so with N worker goroutines all doing a non-blocking `case <-stop.C`
	// check, only the first one to observe it ever actually stops - every other worker's select
	// falls through to default forever and keeps writing well past burstDuration. A cancelled
	// context's Done() channel is *closed*, which every concurrent receiver observes.
	burstDeadlineCtx, burstDeadlineCancel := context.WithTimeout(burstCtx, *burstDuration)
	defer burstDeadlineCancel()
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c, err := connectWithRetry(burstCtx, *addr, *connectTimeout)
			if err != nil {
				atomic.AddInt64(&otherErrs, 1)
				return
			}
			defer c.Close()
			rng := rand.New(rand.NewSource(int64(worker) + time.Now().UnixNano()))
			i := 0
			for {
				select {
				case <-burstDeadlineCtx.Done():
					return
				default:
				}
				if *maxSuccessfulWrites > 0 && atomic.LoadInt64(&successes) >= int64(*maxSuccessfulWrites) {
					return
				}
				docID := docIDs[rng.Intn(len(docIDs))]
				body := []byte(fmt.Sprintf(`{"w":%d,"i":%d,"pad":%q}`, worker, i, pad))
				_, err := c.Upsert(burstCtx, *namespace, docID, body)
				classify(err)
				if errors.Is(err, client.ErrClosed) {
					return // this connection is gone; don't spin
				}
				i++
			}
		}(w)
	}
	wg.Wait()

	fmt.Printf("burst done: successes=%d busy=%d unavailable=%d deadline_exceeded=%d other_errors=%d\n",
		successes, busy, unavailable, deadlineExceeded, otherErrs)

	fmt.Printf("cooling down for %s ...\n", *cooldown)
	time.Sleep(*cooldown)

	fmt.Printf("verify: attempting %d writes after cooldown ...\n", *verifyWrites)
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), *connectTimeout+30*time.Second)
	defer verifyCancel()
	vc, err := connectWithRetry(verifyCtx, *addr, *connectTimeout)
	recovered := false
	verifySuccesses := 0
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: connect failed: %v\n", err)
	} else {
		defer vc.Close()
		for i := 0; i < *verifyWrites; i++ {
			docID := docIDs[i%len(docIDs)] // same bounded pool as the burst - see docPool's doc comment
			_, err := vc.Upsert(verifyCtx, *namespace, docID, []byte(fmt.Sprintf(`{"verify":%d}`, i)))
			// Classified the same way as burst-phase writes: pressure can plausibly still be
			// climbing from writes that landed right at the end of the burst and only becomes
			// visible a moment later, so a busy/unavailable/etc. response here is just as real a
			// signal as one during the burst itself - the RESULT line's totals should include it.
			classify(err)
			if err == nil {
				verifySuccesses++
			}
			time.Sleep(50 * time.Millisecond)
		}
		// Recovered means pressure is not a permanent zombie: at least most post-cooldown
		// writes succeed. Not literally 100% - a real deployment's background load can keep
		// some pressure alive - but a guard that never clears would show ~0 successes here.
		recovered = verifySuccesses >= (*verifyWrites*3)/4
	}
	fmt.Printf("verify done: successes=%d/%d recovered=%v\n", verifySuccesses, *verifyWrites, recovered)

	dataOK := true
	if *verifyDocID != "" {
		got, _, err := retryingGetJSON(*addr, *namespace, *verifyDocID, *connectTimeout)
		if err != nil {
			fmt.Fprintf(os.Stderr, "post-run verify-doc read failed: %v\n", err)
			dataOK = false
		} else if string(got) != *verifyDocValue {
			fmt.Fprintf(os.Stderr, "post-run verify-doc mismatch: want %q got %q\n", *verifyDocValue, string(got))
			dataOK = false
		} else {
			fmt.Printf("post-run: verify doc %s intact\n", *verifyDocID)
		}
	}

	fmt.Printf("RESULT successes=%d busy=%d unavailable=%d deadline_exceeded=%d other_errors=%d recovered=%v data_intact=%v\n",
		successes, busy, unavailable, deadlineExceeded, otherErrs, recovered, dataOK)

	if !dataOK {
		os.Exit(1)
	}
}

// connectWithRetry retries Connect for up to timeout - a server replaying a delta log after a
// restart, or one whose listener just hasn't come up yet, refuses connections for a window that
// isn't itself a bug to report as one.
func connectWithRetry(ctx context.Context, addr string, timeout time.Duration) (*client.Client, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		connectCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		c, err := client.Connect(connectCtx, addr, "")
		cancel()
		if err == nil {
			return c, nil
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	return nil, fmt.Errorf("giving up after %s: %w", timeout, lastErr)
}

func retryingUpsert(addr, namespace, docID string, body []byte, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout+10*time.Second)
	defer cancel()
	c, err := connectWithRetry(ctx, addr, timeout)
	if err != nil {
		return err
	}
	defer c.Close()
	_, err = c.Upsert(ctx, namespace, docID, body)
	return err
}

func retryingGetJSON(addr, namespace, docID string, timeout time.Duration) ([]byte, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout+10*time.Second)
	defer cancel()
	c, err := connectWithRetry(ctx, addr, timeout)
	if err != nil {
		return nil, "", err
	}
	defer c.Close()
	return c.GetJSON(ctx, namespace, docID)
}
