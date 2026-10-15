#!/usr/bin/env bash
# Builds kdb-service from a previous release tag (default v0.6.2) into go/bin/prev/, for the
# mixed-version upgrade e2e tests (kdb-integration/e2e/test_upgrade.py, KDB_SERVICE_BIN_PREV).
#
# The tag's tree is exported with `git archive` into a temporary directory - the same way
# scripts/release-verify.sh rebuilds a tag - so the current checkout, its index and the shared
# worktree list are untouched. It is built the way `make build-go` builds it at that tag (the
# same -ldflags version injection), so the binary reports the tag's version and commit.
#
#   scripts/build-prev-service.sh            # -> go/bin/prev/kdb-service-v0.6.2
#   scripts/build-prev-service.sh v0.6.1     # any tag
#   export KDB_SERVICE_BIN_PREV=$PWD/go/bin/prev/kdb-service-v0.6.2
#
# `make build-prev-service` (optionally PREV_TAG=vX.Y.Z) runs this.
set -euo pipefail

TAG="${1:-${PREV_TAG:-v0.7.1}}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$ROOT/go/bin/prev"
OUT="$OUT_DIR/kdb-service-$TAG"

if ! git -C "$ROOT" rev-parse -q --verify "refs/tags/$TAG^{commit}" >/dev/null; then
  echo "tag $TAG not found locally; fetch tags first (git fetch --tags)" >&2
  exit 1
fi

SRC="$(mktemp -d "${TMPDIR:-/tmp}/kdb-prev-$TAG-XXXXXX")"
trap 'rm -rf "$SRC"' EXIT

git -C "$ROOT" archive --format=tar "$TAG" go | tar -x -C "$SRC"
COMMIT="$(git -C "$ROOT" rev-parse "$TAG^{commit}")"
DATE="$(git -C "$ROOT" log -1 --format=%cI "$COMMIT")"
PKG="github.com/limidus/kdb/go/kdb/version"
mkdir -p "$OUT_DIR"
(cd "$SRC/go" && go build -trimpath \
  -ldflags "-X $PKG.Version=${TAG#v} -X $PKG.Commit=$COMMIT -X $PKG.Dirty=false -X $PKG.BuildDate=$DATE" \
  -o "$OUT" ./cmd/kdb-service)
echo "$OUT"
