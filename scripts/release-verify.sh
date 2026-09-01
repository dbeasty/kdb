#!/usr/bin/env bash
# Proves the Go side of a release is reproducible: builds it twice, from two independent clean
# copies of the same source, and diffs the checksums (docs/kdb-release-plan.md §3/§7).
#
# Two modes:
#   scripts/release-verify.sh          - snapshots the current working tree (uncommitted changes
#                                         included) into two temp copies. Useful while iterating,
#                                         before anything is even committed.
#   scripts/release-verify.sh vX.Y.Z   - uses `git archive` on that tag instead, so it only sees
#                                         exactly what was pushed. This is the real release check.
#
# Only verifies release-go (bundle + binaries + checksums) - the Kotlin side isn't yet asserted
# reproducible (docs/kdb-release-plan.md §5, R5) and jar timestamps/ordering aren't pinned here.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAG="${1:-}"

if [ -n "$TAG" ]; then
	git -C "$REPO_ROOT" rev-parse "$TAG^{commit}" >/dev/null 2>&1 || {
		echo "release-verify: no such tag '$TAG'" >&2
		exit 1
	}
	REF="$TAG"
	REF_DESC="tag $TAG"
else
	REF="HEAD"
	REF_DESC="the current working tree"
fi

COMMIT="$(git -C "$REPO_ROOT" rev-parse "$REF")"
DATE="$(git -C "$REPO_ROOT" log -1 --format=%cI "$REF")"
VERSION="$(cat "$REPO_ROOT/VERSION")"

WORK1="$(mktemp -d)"
WORK2="$(mktemp -d)"
trap 'rm -rf "$WORK1" "$WORK2"' EXIT

snapshot() {
	local dest="$1"
	if [ -n "$TAG" ]; then
		git -C "$REPO_ROOT" archive "$TAG" | tar -x -C "$dest"
	else
		rsync -a \
			--exclude='.git' --exclude='.gradle' --exclude='.claude' --exclude='.kotlin' \
			--exclude='**/build/' --exclude='dist' --exclude='go/bin' --exclude='kotlin-js-store' \
			"$REPO_ROOT"/ "$dest"/
	fi
}

echo "verifying $REF_DESC reproduces byte-for-byte across two independent clean builds..."
snapshot "$WORK1"
snapshot "$WORK2"

for w in "$WORK1" "$WORK2"; do
	(cd "$w" && make release-go GIT_COMMIT="$COMMIT" GIT_DIRTY=false RELEASE_DATE="$DATE" VERSION="$VERSION" >/dev/null)
done

if diff -u "$WORK1/dist/SHA256SUMS" "$WORK2/dist/SHA256SUMS"; then
	echo "REPRODUCIBLE: $REF_DESC — two independent builds match byte-for-byte"
else
	echo "MISMATCH: $REF_DESC did not reproduce — see diff above" >&2
	exit 1
fi
