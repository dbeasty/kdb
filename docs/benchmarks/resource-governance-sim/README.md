# Resource-governance simulation (local, Docker)

End-to-end verification of kdb-spec-layer13's resource-governance work — memory admission,
bounded write queues, typed client errors, orderly abort, and crash-only durability — against
real low-memory **and** low-CPU Docker resource limits, over the real wire protocol
(`go/kdb/client`). Complements `docs/benchmarks/lightsail-sim`, which measures throughput; this
harness checks *behavior under pressure*, not throughput.

**Last verified 2026-08-27** against `e2bbc82` (post-group-commit): all three scenarios pass.
Scenario 1 reported `recovered=false` on that run — expected and deliberately not asserted, see
the comment in `run.sh` for why that ratio is workload- and hardware-dependent.

## What's here

- `Dockerfile` — same build as `lightsail-sim`'s, reused rather than duplicated.
- `run.sh` — three scenarios, each asserting specific pass/fail conditions:
  1. **Low-memory, no-zombie** — a tight `--memory-limit-mb` with no `--abort-after`: proves
     writes are rejected with typed `BUSY` errors under pressure (not dropped connections), and
     that pressure clears on its own once load eases (not a permanent latch —
     kdb-spec-layer13 §2.5).
  2. **Low-memory + orderly abort + restart** — a tight `--memory-limit-mb` with `--abort-after`
     set and `docker run --restart=on-failure`: proves a sustained-pressure abort exits cleanly
     (code 75), the container is restarted by Docker, and a document written *before* the abort
     is still readable afterward with **no repair step** — Component 47's actual point.
  3. **Low-CPU** — a tight `--cpus` limit (well below what the write load needs), no memory
     constraint: proves the server keeps answering requests (slower, but not wedged or
     unresponsive) under CPU starvation.

- `go/cmd/kdb-pressure-test` (lives in the Go module proper) — drives sustained Upsert load,
  classifies every error via `errors.Is`/`errors.As` against `client.ErrBusy` /
  `client.ErrUnavailable` / `client.ErrDeadlineExceeded` rather than lumping all errors together
  (unlike `kdb-loadtest`, which is a throughput tool and doesn't need this distinction), then
  idles and re-attempts writes to check recovery, and optionally verifies one specific
  document's value survived the whole run. Prints one `RESULT ...` line the script asserts
  against.

## Running it

```bash
./docs/benchmarks/resource-governance-sim/run.sh
```

Runs all three scenarios in sequence and reports pass/fail for each. Tunables via environment
variables (defaults shown):

```bash
MEMORY_TIGHT=150m CPUS_LOW=0.25 PORT_BASE=19190 \
  ./docs/benchmarks/resource-governance-sim/run.sh
```

Run a single scenario directly:

```bash
./docs/benchmarks/resource-governance-sim/run.sh memory-no-zombie
./docs/benchmarks/resource-governance-sim/run.sh memory-abort-restart
./docs/benchmarks/resource-governance-sim/run.sh cpu-starved
```

## Same caveats as lightsail-sim

Apple Silicon (arm64) vs. real Lightsail (x86_64), Docker Desktop's macOS VM overhead, loopback
networking — see `docs/benchmarks/lightsail-sim/README.md`'s own caveats section, which applies
identically here. What's being verified is *behavioral shape* (does pressure recover, does abort
leave clean state, does the client see typed errors) not absolute throughput numbers.
