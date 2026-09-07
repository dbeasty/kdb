#!/usr/bin/env bash
# Proves the embeddable Go source bundle actually works standalone: unzips it, then builds,
# vets, tests and cross-compiles it with nothing but what's inside the zip - no access to the
# rest of this repo, no GOPATH tricks. This is the bundle's own acceptance gate
# (docs/kdb-release-plan.md §2.5/R2): if a future change to kdb/embed reaches into a package the
# bundle doesn't carry, this is what catches it, not a downstream consumer's build failure.
#
# Usage: scripts/verify-bundle.sh dist/bundle/kdb-go-embed-0.1.0.zip
set -euo pipefail

ZIP="${1:?usage: verify-bundle.sh <path-to-kdb-go-embed-VERSION.zip>}"
ZIP="$(cd "$(dirname "$ZIP")" && pwd)/$(basename "$ZIP")"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

unzip -q "$ZIP" -d "$WORK"
BUNDLE_DIR="$WORK/$(basename "$ZIP" .zip)"
cd "$BUNDLE_DIR"

echo "== go build ==" && go build ./...
echo "== go vet ==" && go vet ./...
echo "== go test ==" && go test ./...
for target in linux/amd64 linux/arm64 darwin/arm64; do
	echo "== cross-compile $target =="
	GOOS="${target%/*}" GOARCH="${target#*/}" CGO_ENABLED=0 go build ./...
done

echo "bundle OK: $ZIP"
