// loadtest drives sustained small-message read/write traffic against a running kdb-service
// instance over its real wire protocol (go/kdb/client, TCP - the same path a real Zolik client
// uses), reporting throughput and latency percentiles. Built to run from the host against a
// Docker container whose resources are constrained to approximate a Lightsail tier - see
// docs/benchmarks/lightsail-sim/README.md.
//
// Workload: pre-populates a fixed pool of documents, then has every worker repeatedly read or
// Upsert a random document from that pool - a bounded working set, matching the repository
// pattern go/kdb/client itself is built for (read/write one document by id; see the client
// package doc comment), not an ever-growing insert stream. An unbounded-insert workload is a
// different, also-real question (does the engine need periodic DAG compaction under sustained
// novel writes - it currently does, since nothing evicts commit history from memory yet) but
// answering it isn't this tool's job; keeping the pool fixed here isolates read/write throughput
// from that separate, already-known memory-growth characteristic.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9090", "kdb-service SQL wire address")
	namespace := flag.String("namespace", "demo/loadtest", "namespace to write into")
	concurrency := flag.Int("concurrency", 16, "concurrent client connections")
	duration := flag.Duration("duration", 30*time.Second, "how long to run the measured window")
	readRatio := flag.Float64("read-ratio", 0.7, "fraction of ops that are reads (0..1); rest are Upserts")
	docBytes := flag.Int("doc-bytes", 200, "approximate JSON document size in bytes")
	docPool := flag.Int("doc-pool", 2000, "fixed number of documents to pre-populate and cycle through (bounded working set)")
	warmup := flag.Duration("warmup", 3*time.Second, "warmup period excluded from reported stats")
	flag.Parse()

	pad := ""
	for len(pad) < *docBytes-40 {
		pad += "x"
	}

	fmt.Printf("pre-populating %d documents in %s ...\n", *docPool, *namespace)
	seedCtx, seedCancel := context.WithTimeout(context.Background(), 60*time.Second)
	seedConn, err := client.Connect(seedCtx, *addr, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "seed connect:", err)
		os.Exit(1)
	}
	docIDs := make([]string, *docPool)
	for i := range docIDs {
		id, genErr := codec.RandomUUID()
		if genErr != nil {
			fmt.Fprintln(os.Stderr, "seed uuid:", genErr)
			os.Exit(1)
		}
		docIDs[i] = id.String()
		body := []byte(fmt.Sprintf(`{"id":%q,"seed":true,"pad":%q}`, docIDs[i], pad))
		if _, err := seedConn.Upsert(seedCtx, *namespace, docIDs[i], body); err != nil {
			fmt.Fprintf(os.Stderr, "seed upsert %d/%d: %v\n", i, *docPool, err)
			os.Exit(1)
		}
	}
	seedConn.Close()
	seedCancel()
	fmt.Printf("connecting %d clients to %s ...\n", *concurrency, *addr)

	type sample struct {
		isRead bool
		ns     time.Duration
	}
	results := make(chan sample, 100000)
	var writeOps, readOps, errs int64
	var firstErrMu sync.Mutex
	var firstErr string
	recordErr := func(msg string) {
		firstErrMu.Lock()
		if firstErr == "" {
			firstErr = msg
		}
		firstErrMu.Unlock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), *warmup+*duration+30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			c, err := client.Connect(ctx, *addr, "")
			if err != nil {
				fmt.Fprintf(os.Stderr, "worker %d: connect: %v\n", worker, err)
				atomic.AddInt64(&errs, 1)
				return
			}
			defer c.Close()

			rng := rand.New(rand.NewSource(int64(worker) + time.Now().UnixNano()))
			for {
				select {
				case <-stop:
					return
				default:
				}
				docID := docIDs[rng.Intn(len(docIDs))]
				isRead := rng.Float64() < *readRatio
				start := time.Now()
				if isRead {
					_, _, err = c.GetJSON(ctx, *namespace, docID)
				} else {
					body := []byte(fmt.Sprintf(`{"id":%q,"w":%d,"t":%d,"pad":%q}`, docID, worker, time.Now().UnixNano(), pad))
					_, err = c.Upsert(ctx, *namespace, docID, body)
				}
				elapsed := time.Since(start)
				if err != nil {
					atomic.AddInt64(&errs, 1)
					recordErr(err.Error())
					// The connection is gone (server crashed, closed, or context expired) -
					// every further call on it fails instantly with no I/O, which would
					// otherwise spin this loop at however many million iterations/sec the CPU
					// can manage instead of reporting a clean result.
					if errors.Is(err, client.ErrClosed) || ctx.Err() != nil {
						return
					}
					continue
				}
				results <- sample{isRead: isRead, ns: elapsed}
				if isRead {
					atomic.AddInt64(&readOps, 1)
				} else {
					atomic.AddInt64(&writeOps, 1)
				}
			}
		}(w)
	}

	// Warmup: let connections/GC/JIT-equivalent settle, discard these samples.
	time.Sleep(*warmup)
	atomic.StoreInt64(&writeOps, 0)
	atomic.StoreInt64(&readOps, 0)
	atomic.StoreInt64(&errs, 0)
	firstErrMu.Lock()
	firstErr = ""
	firstErrMu.Unlock()
	drainStart := time.Now()
	var writeLatencies, readLatencies []time.Duration
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case s := <-results:
				if s.isRead {
					readLatencies = append(readLatencies, s.ns)
				} else {
					writeLatencies = append(writeLatencies, s.ns)
				}
			case <-time.After(20 * time.Millisecond):
				if time.Since(drainStart) >= *duration {
					return
				}
			}
		}
	}()
	<-done
	close(stop)
	wg.Wait()

	elapsed := time.Since(drainStart)
	totalOps := atomic.LoadInt64(&writeOps) + atomic.LoadInt64(&readOps)
	fmt.Printf("\n=== Result (measured window: %s, excludes %s warmup) ===\n", elapsed.Round(time.Millisecond), warmup)
	fmt.Printf("concurrency=%d read_ratio=%.2f doc_bytes~=%d doc_pool=%d\n", *concurrency, *readRatio, *docBytes, *docPool)
	fmt.Printf("total ops: %d (writes=%d reads=%d errors=%d)\n", totalOps, atomic.LoadInt64(&writeOps), atomic.LoadInt64(&readOps), atomic.LoadInt64(&errs))
	firstErrMu.Lock()
	reportedErr := firstErr
	firstErrMu.Unlock()
	if reportedErr != "" {
		fmt.Printf("first error seen: %s\n", reportedErr)
	}
	fmt.Printf("throughput: %.1f ops/sec\n", float64(totalOps)/elapsed.Seconds())
	printLatencyStats("write", writeLatencies)
	printLatencyStats("read", readLatencies)
}

func printLatencyStats(label string, samples []time.Duration) {
	if len(samples) == 0 {
		fmt.Printf("%s latency: no samples\n", label)
		return
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	pct := func(p float64) time.Duration {
		idx := int(p * float64(len(samples)-1))
		return samples[idx]
	}
	fmt.Printf("%s latency: p50=%s p95=%s p99=%s max=%s (n=%d)\n",
		label, pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond),
		pct(0.99).Round(time.Microsecond), samples[len(samples)-1].Round(time.Microsecond), len(samples))
}
