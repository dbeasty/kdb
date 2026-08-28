package version

import (
	"regexp"
	"strings"
	"testing"
)

// The link-time vars are package-level globals and Get() memoizes, so tests exercise the
// resolution logic through resolve()'s inputs directly rather than mutating the globals and
// racing the sync.Once.
func withInjected(t *testing.T, ver, commit, date, dirty string) Info {
	t.Helper()
	oldV, oldC, oldD, oldDirty := Version, Commit, BuildDate, Dirty
	Version, Commit, BuildDate, Dirty = ver, commit, date, dirty
	t.Cleanup(func() { Version, Commit, BuildDate, Dirty = oldV, oldC, oldD, oldDirty })

	saved := resolved
	resolve()
	got := resolved
	resolved = saved
	return got
}

func TestInjectedValuesWin(t *testing.T) {
	const sha = "8fe306d12a0e979a4989649ba52908b757cb1266"
	got := withInjected(t, "1.2.3", sha, "2026-08-27T09:41:02Z", "false")

	if got.Version != "1.2.3" {
		t.Errorf("Version = %q, want 1.2.3", got.Version)
	}
	if got.Commit != sha {
		t.Errorf("Commit = %q, want the full injected SHA", got.Commit)
	}
	if got.ShortCommit() != "8fe306d" {
		t.Errorf("ShortCommit() = %q, want 8fe306d", got.ShortCommit())
	}
	if got.Dirty {
		t.Error("Dirty = true for an explicit false injection")
	}
	if got.BuildDate != "2026-08-27T09:41:02Z" {
		t.Errorf("BuildDate = %q", got.BuildDate)
	}
	if got.GoVersion == "" || got.OS == "" || got.Arch == "" {
		t.Errorf("runtime fields not populated: %+v", got)
	}
}

// A release must never ship a binary whose --version can't be traced to a commit, so a missing
// commit has to read as "unknown" rather than as an empty string that looks like a formatting
// bug. Test binaries carry no VCS stamp, which is what makes this case reachable here.
func TestUninjectedCommitIsUnknownNotEmpty(t *testing.T) {
	got := withInjected(t, "0.0.0-dev", "", "", "")

	if got.Commit == "" {
		t.Error("Commit is empty; want a non-empty value (the VCS stamp or Unknown)")
	}
	if got.BuildDate == "" {
		t.Error("BuildDate is empty; want a non-empty value (the VCS stamp or Unknown)")
	}
	if strings.Contains(got.String(), "()") || strings.Contains(got.String(), ", ,") {
		t.Errorf("String() has empty fields: %q", got.String())
	}
}

func TestDirtyIsCalledOutInString(t *testing.T) {
	got := withInjected(t, "1.2.3", "8fe306d12a0e979a4989649ba52908b757cb1266", "2026-08-27T09:41:02Z", "true")

	if !got.Dirty {
		t.Fatal("Dirty = false for a true injection")
	}
	if !strings.Contains(got.String(), "8fe306d-dirty") {
		t.Errorf("String() = %q, want the commit marked -dirty", got.String())
	}
}

func TestDirtyAcceptsAnyCasing(t *testing.T) {
	// The value arrives from shell plumbing (Makefile, Dockerfile ARG, CI), so casing varies.
	for _, in := range []string{"true", "TRUE", "True"} {
		if got := withInjected(t, "1.2.3", "abc", "", in); !got.Dirty {
			t.Errorf("Dirty(%q) = false, want true", in)
		}
	}
	for _, in := range []string{"false", "", "0", "no"} {
		if got := withInjected(t, "1.2.3", "abc", "", in); got.Dirty {
			t.Errorf("Dirty(%q) = true, want false", in)
		}
	}
}

func TestShortCommitHandlesShortAndUnknown(t *testing.T) {
	if got := (Info{Commit: "abc"}).ShortCommit(); got != "abc" {
		t.Errorf("ShortCommit() on a short value = %q, want abc (no panic, no padding)", got)
	}
	if got := (Info{Commit: Unknown}).ShortCommit(); got != Unknown {
		t.Errorf("ShortCommit() on %q = %q, want it returned whole", Unknown, got)
	}
}

func TestStringIsOneParseableLine(t *testing.T) {
	got := withInjected(t, "1.2.3", "8fe306d12a0e979a4989649ba52908b757cb1266", "2026-08-27T09:41:02Z", "false").String()

	if strings.Contains(got, "\n") {
		t.Errorf("String() spans multiple lines: %q", got)
	}
	want := regexp.MustCompile(`^1\.2\.3 \(commit 8fe306d, built 2026-08-27T09:41:02Z, go\S+ \S+/\S+\)$`)
	if !want.MatchString(got) {
		t.Errorf("String() = %q, does not match %v", got, want)
	}
}

func TestGetIsStableAcrossCalls(t *testing.T) {
	if Get() != Get() {
		t.Error("Get() returned differing values across calls")
	}
}
