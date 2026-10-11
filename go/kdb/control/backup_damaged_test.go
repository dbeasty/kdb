package control

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/embed"
)

// A backup of a namespace whose delta log is damaged in place is refused (backup.DamagedSegmentError),
// and whoever asked for it over the control plane must see that: a non-2xx status with the typed
// message and where the damage is, a log line, and no backup listed afterwards.
func TestBackupOfADamagedSegmentIsRefusedOverHTTP(t *testing.T) {
	var logged bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	_, base, rt := fileFixture(t)
	for i := 0; i < 4; i++ {
		if _, err := embed.PutJSONDocument(rt, "demo/users", fmt.Sprintf(`{"id":"d%d"}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	_, first := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", `{}`)
	firstID, _ := first["backupId"].(string)
	if firstID == "" {
		t.Fatalf("the clean backup should succeed: %v", first)
	}

	// Damage the second frame's header in place; the two after it stay intact.
	segs, err := filepath.Glob(filepath.Join(rt.DataRoot, "ns", "demo", "users", "delta", "*.seg"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments under %s: %v", rt.DataRoot, err)
	}
	path := segs[len(segs)-1]
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int
	for off := 0; off+20 <= len(raw) && string(raw[off:off+4]) == "KDBP"; {
		offs = append(offs, off)
		off += 20 + (int(raw[off+8])<<24 | int(raw[off+9])<<16 | int(raw[off+10])<<8 | int(raw[off+11]))
	}
	if len(offs) < 3 {
		t.Fatalf("want at least three frames in %s, got %d", path, len(offs))
	}
	raw[offs[1]] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	for _, body := range []string{`{}`, `{"baseBackupId":"` + firstID + `"}`} {
		res, refused := postJSON(t, base, "/v1/ns/demo%2Fusers/backups", body)
		if res.StatusCode != http.StatusConflict {
			t.Fatalf("%s: want 409 for a damaged segment, got %d (%v)", body, res.StatusCode, refused)
		}
		e, _ := refused["error"].(map[string]any)
		msg, _ := e["message"].(string)
		if e["code"] != "segment_damaged" || !strings.Contains(msg, "kdb-inspect verify") ||
			!strings.Contains(msg, fmt.Sprintf("byte %d", offs[1])) {
			t.Fatalf("%s: the refusal should be typed and say where and what to do: %v", body, refused)
		}
		if refused["offset"] != float64(offs[1]) || refused["nextIntactFrame"] != float64(offs[2]) {
			t.Fatalf("%s: the refusal should carry the offsets: %v", body, refused)
		}
		if (body != `{}`) != (refused["incremental"] == true) {
			t.Fatalf("%s: incremental flag wrong: %v", body, refused)
		}
	}
	if !strings.Contains(logged.String(), "backup of namespace demo/users refused") {
		t.Fatalf("a refused backup should be logged, got %q", logged.String())
	}
	_, listed := get(t, base, "/v1/ns/demo%2Fusers/backups")
	if backups := listed["backups"].([]any); len(backups) != 1 {
		t.Fatalf("a refused backup must not be listed: %v", backups)
	}
}
