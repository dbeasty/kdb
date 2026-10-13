# Receiving a push: adopt main in chunks

Date: 2026-10-13. Base: `origin/main` 529408e, with this branch's `TestSyncScale` copied in so both sides run the new
`push` scenario. The scenario has node A push its whole history (100k commits, bodies of about 2 KB) into an empty
node C. Settings: `KDB_SYNC_LIVE=1 KDB_SYNC_BODY_BYTES=2000 KDB_SYNC_SCALE=50000`. Peak live heap is measured with a
GC before each sample.

| run | peak live, base | peak live, chunked | wall, base | wall, chunked |
|---|---|---|---|---|
| A/B pair 1 (locked) | 428.3 MiB | **199.8 MiB** | 32.8 s | 34.5 s |
| A/B pair 2 (locked) | (no line, see below) | **196.7 MiB** | – | 23.0 s |
| base reruns ×2 | 314.8 / 318.0 MiB | – | 29.9 / 23.1 s | – |

The receiver's peak live heap falls from **315–428 MiB to about 200 MiB**, at the same speed. In the second locked
pair the base binary's push scenario printed no result line. The A/B script filtered failure output, so the cause
was not captured, and four later reruns of the same base binary (two with `KDB_SYNC_LIVE=1`) all completed normally.

The pull scenarios (join and catch-up, 50k commits) are unchanged between base and branch, as expected: pulls were
already chunked by #164.
