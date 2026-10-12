//go:build linux

package server

import (
	"os"
	"path/filepath"
	"testing"
)

func writeCgroupFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCgroupMemoryExcludesInactiveFileCache(t *testing.T) {
	const gib = 1 << 30
	t.Run("v2", func(t *testing.T) {
		root := t.TempDir()
		writeCgroupFile(t, root, "memory.current", "1073741824\n")
		writeCgroupFile(t, root, "memory.stat", "anon 52428800\nfile 1000000000\ninactive_file 1019215872\nactive_file 1000\n")
		got, ok := cgroupMemoryCurrentBytesAt(root)
		if !ok || got != gib-1019215872 {
			t.Fatalf("v2 = %d,%v want %d", got, ok, uint64(gib-1019215872))
		}
	})
	t.Run("v1 prefers hierarchical total", func(t *testing.T) {
		root := t.TempDir()
		writeCgroupFile(t, root, "memory/memory.usage_in_bytes", "1073741824\n")
		writeCgroupFile(t, root, "memory/memory.stat", "inactive_file 5\ntotal_inactive_file 1000000000\n")
		got, ok := cgroupMemoryCurrentBytesAt(root)
		if !ok || got != gib-1000000000 {
			t.Fatalf("v1 = %d,%v", got, ok)
		}
	})
	t.Run("missing stat keeps raw usage", func(t *testing.T) {
		root := t.TempDir()
		writeCgroupFile(t, root, "memory.current", "12345\n")
		if got, ok := cgroupMemoryCurrentBytesAt(root); !ok || got != 12345 {
			t.Fatalf("got %d,%v", got, ok)
		}
	})
	t.Run("inactive above usage clamps to zero", func(t *testing.T) {
		root := t.TempDir()
		writeCgroupFile(t, root, "memory.current", "100\n")
		writeCgroupFile(t, root, "memory.stat", "inactive_file 200\n")
		if got, _ := cgroupMemoryCurrentBytesAt(root); got != 0 {
			t.Fatalf("got %d", got)
		}
	})
	t.Run("no cgroup", func(t *testing.T) {
		if _, ok := cgroupMemoryCurrentBytesAt(t.TempDir()); ok {
			t.Fatal("expected ok=false")
		}
	})
}

// A cgroup whose usage is almost entirely page cache must leave the guard in ZoneNormal, while
// the same raw figure without the cache adjustment lands in ZoneCritical.
func TestPageCacheDoesNotMoveGuardOutOfNormal(t *testing.T) {
	root := t.TempDir()
	const limit = 1 << 30
	writeCgroupFile(t, root, "memory.current", "1060000000\n") // ~99% of the limit
	writeCgroupFile(t, root, "memory.stat", "anon 40000000\ninactive_file 1020000000\n")
	g := NewMemoryGuard(limit, 0.85)
	defer g.Stop()

	if z := g.zoneFor(1060000000, ZoneNormal); z != ZoneCritical {
		t.Fatalf("control: raw usage should be critical, got %v", z)
	}
	used, _ := cgroupMemoryCurrentBytesAt(root)
	if z := g.zoneFor(float64(used), ZoneNormal); z != ZoneNormal {
		t.Fatalf("zone = %v for %d working-set bytes, want ZoneNormal", z, used)
	}
}
