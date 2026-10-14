# `dag.encodeRecord` at 14% of CPU was a macOS profiler artifact

Date: 2026-10-14. Base: `origin/main` b33c7041.

Machines:
- macOS: the development Mac, darwin/arm64, Go 1.26.3.
- Linux: Docker on the same Mac, linux/arm64, 16 CPUs, `golang:1.26` image (Go 1.26.9).

Load average was 3.4–6.6 during the runs. Every timed run went through the shared benchmark lock.

## The report

A CPU profile of the memory-only single-user insert benchmark

    cd go && KDB_GRAPH_FILE=0 KDB_TREE_FILE=0 KDB_BOUNDED_LIVE_BODIES=0 go test ./kdb/server -run '^$' \
      -bench 'BenchmarkWorkloadDurability/memory-only/insert/single-user' -benchtime 100000x -cpuprofile cpu.out

charged `dag.encodeRecord` (`go/kdb/dag/graph_record.go`) with 10–16% of all samples on macOS. Every sample landed
on one instruction: the first `STP ZR, ZR` of `clear(r[:graphRecordSize])`. That works out to 2–3 µs per insert, for
a function that costs 120–160 ns per `put` in isolation. Two other entries looked similar:
- `runtime.madvise` at 14–16%, called from `mheap.allocSpan` → `sysUsed`.
- `document.(*trieSlab).place` at 11–14%.

## Ruling out real causes

Temporary counters (not committed) over a 100,000x run, the same on both platforms:

| suspect | measured |
|---|---|
| `put` called more than once per insert | 100,001 `put` calls (one per insert, plus the b.N=1 warm-up) |
| many node stores | 1 `nodeStore` created |
| chunks dropped and reallocated | 25 chunk allocations (100k × 256 B / 1 MiB), 0 `compactSlab` runs |
| page faults on chunk memory | macOS: 16.5k–16.8k page reclaims for the **whole process**; **0.08–0.09 s sys time** in total. Linux: 0.03–0.05 s sys time |

## Clock time against profile samples

Clock cost comes from `time.Now` around the call, summed over the run:
- `encodeRecord`: around the call in `put`.
- `trieSlab.place`: around the top-level call in `trieSlab.commit`.
- `foldLeaf`: around its whole body.

The profile share is from three uninstrumented runs.

| function | macOS clock | macOS profile | Linux clock | Linux profile |
|---|---|---|---|---|
| `dag.encodeRecord` | 46–49 ns/insert (~0.25%) | **10–16%** | 48–52 ns/insert | below the 10 ms cutoff (<0.5%) |
| `trieSlab.place` | 657–681 ns/insert (~3.4%) | 11–14% | 693–1,004 ns/insert | 3.5–4% cum |
| `runtime.madvise` | ≤ 0.09 s sys for the whole run (≤ 4%) | 14–16% | ≤ 0.05 s sys | below the cutoff |
| `document.foldLeaf` | **8,976–8,987 ns/insert (~46%)** | **9–12% cum** | 10,040–10,186 ns/insert (~47%) | 37–44% cum |

On Linux, the profile agrees with the clock. On macOS, it overstates first-touch stores and syscalls by 4–75×, and
it understates the largest real cost by about 4×.

`foldLeaf` is SHA-256: 1.36 calls and 37.3 `internalHashOfOnlyChild` rounds per insert. Each round hashes a 513-byte
buffer, nine SHA-256 blocks. The macOS profile showed `sha256.blockSHA2` at 3.6% flat. That is where the CPU went
that the misattributed samples stood for.

## Pre-touching chunk memory: before and after

Pre-touching each new chunk means writing one byte per 4 KiB right after `new`. On macOS it makes `encodeRecord`
disappear from the profile, from 0.30–0.37 s to 0–0.03 s. Its clock cost drops only from 46 to 27 ns, and the
benchmark does not move. Interleaved rounds, 100,000x each.

macOS:

| variant | round 1 | round 2 | round 3 |
|---|---|---|---|
| base | 19,628 ns/op | 19,753 ns/op | 19,857 ns/op |
| chunks pre-touched | 19,187 ns/op | 19,869 ns/op | 19,868 ns/op |
| `GODEBUG=madvdontneed=1` | 19,385 ns/op | 19,757 ns/op | 20,016 ns/op |

Linux:

| variant | round 1 | round 2 | round 3 |
|---|---|---|---|
| base | 21,591 ns/op | 21,440 ns/op | 22,673 ns/op |
| instrumented | 21,201 ns/op | 22,454 ns/op | 21,340 ns/op |
| instrumented, chunks pre-touched | 21,798 ns/op | 21,388 ns/op | 22,121 ns/op |

The Linux VM is ~10% slower overall. SHA-256 of a 65-byte input takes 87.5 ns there against 50.4 ns on the Mac.

## Why

On darwin the Go runtime still profiles with the process-wide `setitimer(ITIMER_PROF)`. On Linux, Go has used
per-thread `timer_create` timers since Go 1.18. A process-wide SIGPROF is delivered when some thread next comes back
from the kernel, and a thread returning from a trap is the likeliest to take it. So samples cluster on instructions
that trap:
- the first store to a fresh page (here, the `clear` at the start of a new record);
- syscalls (`madvise`).

Pure compute that never traps, like SHA-256 block rounds, loses the samples those sites gain.

## What to do instead

- On the Mac, treat a profile entry whose samples sit on one first-touch store, or on a syscall, as a lead, not a
  measurement. Distrust a compute-bound function's *small* share just as much.
- Before optimising, time the suspect with a clock or check the process's `user`/`sys` split with
  `/usr/bin/time -l`.
- For attribution, profile on Linux. Docker on the Mac is enough:

      docker run --rm -v "$PWD":/src -v "$(go env GOMODCACHE)":/gomod:ro -e GOMODCACHE=/gomod -e GOPROXY=off \
        -e GOFLAGS=-buildvcs=false -w /src/go golang:1.26 go test -c -o /src/server.test ./kdb/server

  Then run the binary in the same image with `-test.cpuprofile`. The host's `go tool pprof` reads the Linux binary.
- Nothing in `encodeRecord` or the chunk allocation needed to change. Pre-touching saves ~20 ns of a ~19.6 µs insert
  (0.1%, inside run-to-run noise), so it was not kept.
- The insert's real hotspot is `foldLeaf`'s single-child spine hashing, about half of every insert. Shrinking it
  without changing the tree hash, which Kotlin and TypeScript compute too, is limited to the SHA-256 work itself.
  One option is a precomputed midstate per nibble for the `0x01` + zero prefix. That skips up to 7 of the 9 blocks
  for high nibbles, and 3.5 on average.
