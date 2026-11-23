//go:build linux

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCgroupMemoryLimitBytesAt(t *testing.T) {
	write := func(t *testing.T, root, rel, body string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		name   string
		files  map[string]string
		want   uint64
		wantOK bool
	}{
		{"v2 limit", map[string]string{"memory.max": "3221225472\n"}, 3 << 30, true},
		{"v2 unlimited", map[string]string{"memory.max": "max\n"}, 0, false},
		{"v1 limit", map[string]string{"memory/memory.limit_in_bytes": "1073741824\n"}, 1 << 30, true},
		{"v1 unlimited sentinel", map[string]string{"memory/memory.limit_in_bytes": "9223372036854771712\n"}, 0, false},
		{"no controller", nil, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			for rel, body := range c.files {
				write(t, root, rel, body)
			}
			got, ok := cgroupMemoryLimitBytesAt(root)
			if got != c.want || ok != c.wantOK {
				t.Fatalf("got (%d, %v), want (%d, %v)", got, ok, c.want, c.wantOK)
			}
		})
	}
}
