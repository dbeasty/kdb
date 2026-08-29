// Package version holds the build identity shared by every KDB binary (kdb, kdb-service,
// kdb-inspect): a human-readable version string plus the exact git commit it was built from,
// so a running or shipped binary can always be traced back to its source tree.
//
// Release builds inject all of it at link time:
//
//	go build -ldflags "\
//	  -X github.com/limidus/kdb/go/kdb/version.Version=v1.2.3 \
//	  -X github.com/limidus/kdb/go/kdb/version.Commit=$(git rev-parse HEAD) \
//	  -X github.com/limidus/kdb/go/kdb/version.BuildDate=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
//	  -X github.com/limidus/kdb/go/kdb/version.Dirty=false"
//
// The Makefile's build-go target does that automatically from the repo's VERSION file and the
// working tree's git state; the Dockerfile and the release workflow pass the same values in as
// build args. Nothing has to be injected, though: a plain `go build ./cmd/kdb` still reports the
// right commit, because anything left empty falls back to the VCS stamp the Go toolchain embeds
// in the binary itself. Injection only exists for builds where that stamp is unavailable - most
// notably the Docker build, which copies go/ into the image without the .git directory.
//
// The identity is surfaced by each binary's --version flag, the service's startup banner, and
// the admin endpoint's /healthz response.
package version

import (
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
)

// Injected at link time; see the package comment. Read them through Get() rather than directly,
// so the VCS-stamp fallback applies.
var (
	// Version is the release version, e.g. "0.1.0". "0.0.0-dev" marks a from-source build.
	Version = "0.0.0-dev"
	// Commit is the full 40-hex-character git commit SHA the binary was built from.
	Commit = ""
	// BuildDate is when the binary was built, RFC 3339 in UTC.
	BuildDate = ""
	// Dirty is "true" when the build came from a working tree with uncommitted changes, which
	// means Commit alone does not fully identify the source. Anything else counts as clean.
	Dirty = ""
)

// Unknown is what Get reports for a field that was neither injected nor recoverable from the
// binary's VCS stamp.
const Unknown = "unknown"

// Info is the resolved build identity: injected values where they were provided, the Go
// toolchain's embedded VCS stamp where they were not.
type Info struct {
	Version   string // release version, e.g. "0.1.0"
	Commit    string // full git SHA, or Unknown
	BuildDate string // RFC 3339 UTC, or Unknown
	Dirty     bool   // built from a tree with uncommitted changes
	GoVersion string // toolchain that compiled the binary
	OS        string // GOOS
	Arch      string // GOARCH
}

// ShortCommit is Commit truncated to the usual 7-character display form. Unknown is returned
// whole - truncating it to "unknow" would read like a corrupted SHA rather than an absent one.
func (i Info) ShortCommit() string {
	if i.Commit == Unknown || len(i.Commit) < 7 {
		return i.Commit
	}
	return i.Commit[:7]
}

// String renders the one-line form used by --version and the startup banner:
//
//	0.1.0 (commit 8fe306d, built 2026-08-27T09:41:02Z, go1.26 darwin/arm64)
//
// A dirty tree is called out explicitly, because the commit is then only half the story.
func (i Info) String() string {
	commit := i.ShortCommit()
	if i.Dirty {
		commit += "-dirty"
	}
	return i.Version + " (commit " + commit + ", built " + i.BuildDate +
		", " + i.GoVersion + " " + i.OS + "/" + i.Arch + ")"
}

var (
	resolveOnce sync.Once
	resolved    Info
)

// Get returns the resolved build identity. It is safe for concurrent use and does the (cheap)
// resolution work once.
func Get() Info {
	resolveOnce.Do(resolve)
	return resolved
}

// String is shorthand for Get().String().
func String() string { return Get().String() }

func resolve() {
	resolved = Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		Dirty:     strings.EqualFold(Dirty, "true"),
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}
	// Fall back to the VCS stamp the toolchain embeds in binaries built inside a git work tree.
	// It is absent in test binaries and in builds made from a source copy without .git (the
	// Docker build), which is exactly what the link-time injection above covers.
	if resolved.Commit == "" || resolved.BuildDate == "" || Dirty == "" {
		if bi, ok := debug.ReadBuildInfo(); ok {
			for _, s := range bi.Settings {
				switch s.Key {
				case "vcs.revision":
					if resolved.Commit == "" {
						resolved.Commit = s.Value
					}
				case "vcs.time":
					if resolved.BuildDate == "" {
						resolved.BuildDate = s.Value
					}
				case "vcs.modified":
					if Dirty == "" {
						resolved.Dirty = s.Value == "true"
					}
				}
			}
		}
	}
	if resolved.Version == "" {
		resolved.Version = Unknown
	}
	if resolved.Commit == "" {
		resolved.Commit = Unknown
	}
	if resolved.BuildDate == "" {
		resolved.BuildDate = Unknown
	}
}
