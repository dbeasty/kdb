# Unreleased

Notes for changes on `main` since v0.6.2. Rename this file to `vX.Y.Z.md` (and give it a
proper heading and summary) in the change that bumps `VERSION` — see
`docs/kdb-release-plan.md` §4.

## Fixes

**The embeddable Go source bundle now includes `kdb/server`.** This is the known issue
listed in v0.6.2. A downstream project that vendors the bundle and imports `kdb/server`
directly — to build its own server binary or image around the native engine — got a zip
without it, because the bundle's entry points covered the in-process embedding surface
only. `kdb/server` is now an entry point, and the bundle carries its closure: the wire
protocol, the TCP and WebSocket transports, peer sync, streams, stored procedures
(`kdb/script`) and the full-text and vector index packages it reaches. The bundle grows
from 27 to 40 packages.

**Dependency change for bundle consumers.** The bundle's `go.mod` now requires
`github.com/dop251/goja` (the JavaScript engine behind stored procedures) and
`golang.org/x/sync`, plus their indirects (`dlclark/regexp2/v2`, `go-sourcemap/sourcemap`,
`google/pprof`, `golang.org/x/text`). All are pure Go; the bundle still builds with
`CGO_ENABLED=0` on every release platform. A consumer that imports only the in-process
packages compiles none of this in, but the requirements are in the bundle's module graph.

No on-disk or wire format change.
