# `dag.encodeRecord` at 14% of CPU was a macOS profiler artifact

Date: 2026-10-14. Base: `origin/main` b33c7041. Machine: the development Mac (darwin/arm64, Go 1.26.3), load
average 3.4–5 during the runs. Every timed run went through the shared benchmark lock.

## The report

A CPU profile of the memory-only single-user insert benchmark

    cd go && KDB_GRAPH_FILE=0 KDB_TREE_FILE=0 KDB_BOUNDED_LIVE_BODIES=0 go test ./kdb/server -run '^$' \
      -bench 'BenchmarkWorkloadDurability/memory-only/insert/single-user' -benchtime 100000x -cpuprofile cpu.out

charged `dag.encodeRecord` (`go/kdb/dag/graph_record.go`) with 10–16% of all samples. Every sample landed on one
instruction: the first `STP ZR, ZR` of `clear(r[:graphRecordSize])`. That works out to 2–3 µs per insert, for a
function that costs 120–160 ns per `put` in isolation. Two other entries looked similar:
- `runtime.madvise` at 14–16%, called from `mheap.allocSpan` → `sysUsed`.
- `document.(*trieSlab).place` at 11–14%.

## Ruling out real causes

Temporary counters (not committed) over a 100,000x run:

| suspect | measured |
|---|---|
| `put` called more than once per insert | 100,001 `put` calls (one per insert, plus the b.N=1 warm-up) |
| many node stores | 1 `nodeStore` created |
| chunks dropped and reallocated | 25 chunk allocations (100k × 256 B / 1 MiB), 0 `compactSlab` runs |
| page faults on chunk memory | 16.5k–16.8k page reclaims for the **whole process**; **0.08–0.09 s sys time** in total |

## Clock time against profile samples

`encodeRecord`, timed with `time.Now` around the call in `put`, and the top-level `trieSlab.place` in
`trieSlab.commit`, timed the same way:

| function | real cost (clock) | real total over 100k inserts | profile's total | overstated by |
|---|---|---|---|---|
| `dag.encodeRecord` | 46–49 ns/insert | ~5 ms | 230–370 ms | ~50–75× |
| `trieSlab.place` | 657–681 ns/insert | ~67 ms | 250–330 ms | ~4× |
| `runtime.madvise` | ≤ total sys time | ≤ 90 ms | 310–370 ms | ≥ 4× |

Pre-touching each new chunk (writing one byte per 4 KiB right after `new`) makes `encodeRecord` disappear from
the profile, from 0.30–0.37 s to 0–0.03 s. Its clock cost drops only from 46 to 27 ns, and the benchmark does not
move. Three interleaved rounds, 100,000x each:

| variant | round 1 | round 2 | round 3 |
|---|---|---|---|
| base | 19,628 ns/op | 19,753 ns/op | 19,857 ns/op |
| chunks pre-touched | 19,187 ns/op | 19,869 ns/op | 19,868 ns/op |
| `GODEBUG=madvdontneed=1` | 19,385 ns/op | 19,757 ns/op | 20,016 ns/op |

The samples followed the page fault and left nothing else behind, so they measured the fault and not the work.

## Why

On darwin the Go runtime still profiles with the process-wide `setitimer(ITIMER_PROF)`. On Linux, Go has used
per-thread `timer_create` timers since Go 1.18. A process-wide SIGPROF is delivered when some thread next comes back
from the kernel, and a thread returning from a trap is the likeliest to take it. So samples cluster on instructions
that trap:
- the first store to a fresh page (here, the `clear` at the start of a new record);
- syscalls (`madvise`).

The CPU time those samples stand for was actually spent elsewhere in the process.

## What to do instead

- On the Mac, treat a profile entry whose samples sit on one first-touch store, or on a syscall, as a lead, not a
  measurement. Before optimising it, time it with a clock (`time.Now` around the call, summed over the run) or
  check the process's `user`/`sys` split with `/usr/bin/time -l`.
- Profile on the Linux target when attribution matters; its per-thread timers do not have this bias.
- Nothing in `encodeRecord` or the chunk allocation needed to change. Pre-touching saves ~20 ns of a ~19.6 µs insert
  (0.1%, inside run-to-run noise), so it was not kept.
