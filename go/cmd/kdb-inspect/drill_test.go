package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestBackupRestoreDrill runs scripts/backup-restore-drill.sh against binaries built here, so CI
// exercises the whole operator path - live backup, verify, restore while the service is down,
// restart, compare - with real child processes rather than the in-process shortcuts the other
// backup tests take. Those cannot catch a lock or listener problem that only exists between two
// separate processes.
func TestBackupRestoreDrill(t *testing.T) {
	if testing.Short() {
		t.Skip("builds four binaries and starts two services")
	}
	for _, tool := range []string{"bash", "curl", "python3"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not available", tool)
		}
	}
	bin := t.TempDir()
	build := exec.Command("go", "build", "-o", bin+string(filepath.Separator),
		"github.com/limidus/kdb/go/cmd/kdb-service",
		"github.com/limidus/kdb/go/cmd/kdb-inspect",
		"github.com/limidus/kdb/go/cmd/kdb",
		"github.com/limidus/kdb/go/cmd/kdb-e2e-helper")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	script, err := filepath.Abs(filepath.Join("..", "..", "..", "scripts", "backup-restore-drill.sh"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "25")
	cmd.Env = append(os.Environ(), "KDB_BIN_DIR="+bin, "TMPDIR="+t.TempDir())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("drill failed: %v\n%s", err, out)
	}
}
