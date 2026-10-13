# Peer sync: zstd-compressed pack pages and pushes

Date: 2026-10-13. Base: `origin/main` 529408e. Harness: `TestSyncScale` (`KDB_SYNC_SCALE=50000,200000`, bodies of
about 200 B). There were two alternating base/new pairs, all under the shared benchmark lock, at load average 3.6–9.3
(other jobs on the machine).

Pages and pushes are compressed only between two nodes that both advertise the `zstd-pack` capability, at zstd
level 1.

| scenario | commits | recv, base | recv, zstd | allocated, base | allocated, zstd |
|---|---|---|---|---|---|
| join | 50,000 | 24.3 MiB | **6.7 MiB** | 1,118–1,139 MiB | **1,018–1,022 MiB** |
| catch-up | 50,000 | 24.4 MiB | **6.7 MiB** | 1,169–1,177 MiB | **1,018–1,037 MiB** |
| join | 200,000 | 97.5 MiB | **26.9 MiB** | 6,647–6,664 MiB | **6,135–6,176 MiB** |
| catch-up | 200,000 | 97.7 MiB | **26.9 MiB** | 7,657–7,683 MiB | **7,141–7,220 MiB** |

Bytes on the wire are **3.6× smaller**, and allocation is 7–11% lower, because there is less base64 and JSON to
build and parse. Wall time on this base is dominated by the receiver's per-commit log writes (fixed separately in
#166) and swung between 6 s and 98 s for identical runs, so it is not reported as a result. A single 200-commit page
measured in `TestPackPageZstdRoundTrip` goes from 104 KB to 25 KB.
