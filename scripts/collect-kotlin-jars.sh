#!/usr/bin/env bash
# Collects each Gradle module's compiled jar into a dist directory, for `make release-kotlin`
# (docs/kdb-release-plan.md §5, R5). Run after `./gradlew jar` has already produced them.
#
# The module list comes from settings.gradle.kts, not a glob over the repo, so it can't pick up
# stray jars from an unrelated build/ directory (e.g. a .claude worktree checkout). Within each
# module's build/libs/, only the versioned main artifact is taken - Kotlin Multiplatform modules
# also produce per-target *-metadata.jar and *-<platform>-<version>-metadata.jar files, and those
# aren't consumable classpath artifacts, so anything with "-metadata-" in the name is skipped.
#
# Usage: collect-kotlin-jars.sh <version> <dist-dir>
set -euo pipefail

VERSION="${1:?usage: collect-kotlin-jars.sh <version> <dist-dir>}"
DIST="${2:?usage: collect-kotlin-jars.sh <version> <dist-dir>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "$DIST"

modules=$(grep -oE 'include\(":[^"]+"\)' "$REPO_ROOT/settings.gradle.kts" | sed -E 's/include\(":([^"]+)"\)/\1/')

# Two modules can legitimately produce the same jar basename (e.g. an MPP module's jvm target
# and a same-named plain-jvm module both resolve to "<name>-jvm-<version>.jar"). Silently
# overwriting one with the other would drop an artifact from the release without a trace, so a
# collision fails loudly instead - the fix belongs in the colliding module's artifactId, not
# here. (No associative arrays: this needs to run under macOS's stock bash 3.2, not just bash 4+.)
count=0
for m in $modules; do
	shopt -s nullglob
	for jar in "$REPO_ROOT/$m"/build/libs/*-"$VERSION".jar; do
		case "$jar" in
		*-metadata-"$VERSION".jar) continue ;;
		esac
		dest="$DIST/$(basename "$jar")"
		if [ -e "$dest" ]; then
			echo "collect-kotlin-jars.sh: jar name collision on '$(basename "$jar")' - another module already produced this artifact filename:" >&2
			echo "  $jar" >&2
			echo "give one module a distinct artifactId/archiveBaseName in its build.gradle.kts" >&2
			exit 1
		fi
		cp "$jar" "$dest"
		count=$((count + 1))
	done
	shopt -u nullglob
done

if [ "$count" -eq 0 ]; then
	echo "collect-kotlin-jars.sh: found no jars matching version $VERSION under any module's build/libs/ - did './gradlew jar' run first?" >&2
	exit 1
fi
echo "kotlin jars: $count files in $DIST/"
