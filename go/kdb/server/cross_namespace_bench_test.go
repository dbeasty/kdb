package server

import (
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// Cross-namespace commit benchmarks - see docs/benchmarks/2026-09-20-cross-namespace-transactions.md for the
// recorded numbers and how to read them. Every one is file-backed at the default durability
// (sync), because fsync is the cost the protocol is arranged around; an in-memory run would
// measure nothing but map inserts.
//
// Each sub-benchmark runs b.N operations spread over a fixed number of concurrent writers and
// reports ops/s and latency percentiles. Every operation writes fresh documents, so nothing
// conflicts: these measure the commit path, not retry behaviour.
//
//	go test ./kdb/server/ -run '^$' -bench 'BenchmarkCross' -benchtime 3s
//
// Check machine load (uptime) before every run, and run nothing else heavy alongside.

var benchConcurrency = []int{1, 16, 64}

func benchSet(b *testing.B, namespaces ...string) *NamespaceSet {
	b.Helper()
	opts := embed.FileRuntimeOptions{}
	if os.Getenv("KDB_BENCH_SYNC_FAST") == "1" {
		// The barrier-only flush (F_BARRIERFSYNC on darwin) instead of a full device flush -
		// separates "the protocol waits on disks" from "every disk flush serializes the device".
		opts.Storage.SyncMode = storio.SyncModeFast
	}
	host, err := embed.OpenFileHost(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { host.Close() })
	set := NewNamespaceSet(host.Transactions())
	for _, ns := range namespaces {
		rt, err := host.Namespace(embed.CatalogFromNamespace(ns), ns, schema.None())
		if err != nil {
			b.Fatal(err)
		}
		srv := NewKdbServerRuntime(rt)
		// Deep enough that 64 writers queueing on one namespace are measured, not refused.
		srv.writeGate = newWriteGate(256)
		srv.WriteTimeout = time.Minute
		if err := set.Add(srv); err != nil {
			b.Fatal(err)
		}
	}
	return set
}

// runConcurrent runs b.N calls of op across conc goroutines and reports throughput and latency.
func runConcurrent(b *testing.B, conc int, op func(worker int) error) {
	b.Helper()
	var next atomic.Int64
	latencies := make([][]time.Duration, conc)
	var wg sync.WaitGroup
	var firstErr atomic.Pointer[error]
	b.ResetTimer()
	start := time.Now()
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for next.Add(1) <= int64(b.N) {
				t0 := time.Now()
				if err := op(w); err != nil {
					firstErr.CompareAndSwap(nil, &err)
					return
				}
				latencies[w] = append(latencies[w], time.Since(t0))
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	if p := firstErr.Load(); p != nil {
		b.Fatal(*p)
	}
	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		return float64(all[int(float64(len(all)-1)*p)].Microseconds()) / 1000
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
	b.ReportMetric(pct(0.50), "p50-ms")
	b.ReportMetric(pct(0.99), "p99-ms")
	b.ReportMetric(0, "ns/op") // wall time per op across concurrent writers is not a meaningful number here
}

func benchUUID(b *testing.B) codec.UUID {
	id, err := codec.RandomUUID()
	if err != nil {
		b.Fatal(err)
	}
	return id
}

func singleCommit(b *testing.B, rt *KdbServerRuntime, ns string, docs int) error {
	head, err := rt.Runtime.DAG.Head()
	if err != nil {
		return err
	}
	tx := writeTx(head, benchUUID(b), `{"v":1}`)
	for i := 1; i < docs; i++ {
		tx.Operations = append(tx.Operations, writeTx(head, benchUUID(b), `{"v":1}`).Operations...)
	}
	_, err = rt.Commit(ns, tx, "", auth.Principal{})
	return err
}

func acrossCommit(b *testing.B, set *NamespaceSet, namespaces ...string) error {
	parts := make([]NamespaceTransaction, len(namespaces))
	for i, ns := range namespaces {
		parts[i] = NamespaceTransaction{Namespace: ns, Tx: writeTx(codec.Hash{}, benchUUID(b), `{"v":1}`)}
	}
	_, err := set.CommitAcross(parts, auth.Principal{})
	return err
}

// The baseline: the same two document writes, in one namespace, as one ordinary commit.
func BenchmarkCrossBaselineOneNamespaceTwoDocs(b *testing.B) {
	for _, conc := range benchConcurrency {
		b.Run(fmt.Sprintf("writers-%d", conc), func(b *testing.B) {
			set := benchSet(b, "a")
			rt, _ := set.Get("a")
			runConcurrent(b, conc, func(int) error { return singleCommit(b, rt, "a", 2) })
		})
	}
}

// What an application has to do today: two independent commits, one per namespace, with nothing
// holding them together.
func BenchmarkCrossBaselineTwoCommitsNotAtomic(b *testing.B) {
	for _, conc := range benchConcurrency {
		b.Run(fmt.Sprintf("writers-%d", conc), func(b *testing.B) {
			set := benchSet(b, "a", "b")
			rtA, _ := set.Get("a")
			rtB, _ := set.Get("b")
			runConcurrent(b, conc, func(int) error {
				if err := singleCommit(b, rtA, "a", 1); err != nil {
					return err
				}
				return singleCommit(b, rtB, "b", 1)
			})
		})
	}
}

// The performance protocol: per-namespace gates, pipelined, group-committed decisions.
func BenchmarkCrossCommitAcrossTwoNamespaces(b *testing.B) {
	for _, conc := range benchConcurrency {
		b.Run(fmt.Sprintf("writers-%d", conc), func(b *testing.B) {
			set := benchSet(b, "a", "b")
			runConcurrent(b, conc, func(int) error { return acrossCommit(b, set, "a", "b") })
		})
	}
}

// The simple protocol, for comparison: one lock over the set, held through both fsyncs.
func BenchmarkCrossCommitAcrossSerialized(b *testing.B) {
	for _, conc := range benchConcurrency {
		b.Run(fmt.Sprintf("writers-%d", conc), func(b *testing.B) {
			set := benchSet(b, "a", "b")
			set.SetSerializedForBenchmark(true)
			runConcurrent(b, conc, func(int) error { return acrossCommit(b, set, "a", "b") })
		})
	}
}

// Four namespaces: half the writers commit a+b, half c+d. Groups over disjoint namespaces share
// no gate, so this should run close to twice the two-namespace rate.
func BenchmarkCrossCommitAcrossDisjointPairs(b *testing.B) {
	for _, conc := range benchConcurrency[1:] {
		b.Run(fmt.Sprintf("writers-%d", conc), func(b *testing.B) {
			set := benchSet(b, "a", "b", "c", "d")
			runConcurrent(b, conc, func(w int) error {
				if w%2 == 0 {
					return acrossCommit(b, set, "a", "b")
				}
				return acrossCommit(b, set, "c", "d")
			})
		})
	}
}

// Four namespaces. Wider groups cost one more part each; the decision is still one record.
func BenchmarkCrossCommitAcrossFourNamespaces(b *testing.B) {
	for _, conc := range benchConcurrency {
		b.Run(fmt.Sprintf("writers-%d", conc), func(b *testing.B) {
			set := benchSet(b, "a", "b", "c", "d")
			runConcurrent(b, conc, func(int) error { return acrossCommit(b, set, "a", "b", "c", "d") })
		})
	}
}

// What cross-namespace traffic costs the single-namespace writers beside it. 16 writers commit to
// one namespace alone while 16 others run cross-namespace groups: over two *other* namespaces
// (no shared gate - should cost nothing), or over the same namespace plus another (shared gate,
// and every single commit queued behind a group waits for its decision).
func BenchmarkCrossSingleNamespaceWritersBesideGroups(b *testing.B) {
	// "commits-elsewhere" is the control: the same background load as ordinary single-namespace
	// commits to another namespace, no groups at all. What it costs is what any extra writer on
	// the same disk costs, and is not the protocol's.
	for _, shape := range []string{"alone", "commits-elsewhere", "groups-elsewhere", "groups-on-same-namespace"} {
		b.Run(shape, func(b *testing.B) {
			set := benchSet(b, "a", "b", "c")
			rtA, _ := set.Get("a")
			stop := make(chan struct{})
			var bg sync.WaitGroup
			if shape != "alone" {
				pair := []string{"b", "c"}
				if shape == "groups-on-same-namespace" {
					pair = []string{"a", "b"}
				}
				for i := 0; i < 16; i++ {
					bg.Add(1)
					go func() {
						defer bg.Done()
						for {
							select {
							case <-stop:
								return
							default:
							}
							var err error
							if shape == "commits-elsewhere" {
								rtB, _ := set.Get("b")
								err = singleCommit(b, rtB, "b", 1)
							} else {
								err = acrossCommit(b, set, pair...)
							}
							if err != nil {
								b.Error(err)
								return
							}
						}
					}()
				}
			}
			runConcurrent(b, 16, func(int) error { return singleCommit(b, rtA, "a", 1) })
			close(stop)
			bg.Wait()
		})
	}
}
