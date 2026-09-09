package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEveryLiveSettingCanBePersisted is the invariant that keeps the two halves of §7.4 together: a
// setting the engine can change under load, and that a config file can hold, must have a way to be
// written back. Without it a future live setting would silently be live-only, and the operator
// would find out at the next restart.
func TestEveryLiveSettingCanBePersisted(t *testing.T) {
	// A file with every field set, so inFile answers true for all of them.
	full := &ServiceFile{}
	for _, spec := range serviceSpecs() {
		if spec.mutability != MutabilityLive || spec.setInFile != nil {
			continue
		}
		// A live setting with no writer is only acceptable if a config file cannot hold it at all.
		if spec.inFile(full) {
			t.Errorf("%s is live-changeable and has a config-file field, but no setInFile: "+
				"a change to it could be applied and never written down", spec.key)
		}
	}
}

func TestSetInFileRejectsWhatCannotBeWritten(t *testing.T) {
	f := &ServiceFile{}
	if err := SetInFile(f, "storage.dataDir", "/tmp/x"); err == nil {
		t.Error("storage.dataDir is restart-only and has no writer; setting it should be refused")
	}
	if err := SetInFile(f, "no.such.setting", 1); err == nil {
		t.Error("an unknown key should be refused rather than ignored")
	}
	if err := SetInFile(nil, "log.level", "debug"); err == nil {
		t.Error("a nil file should be refused rather than panicking")
	}
	// The floor is the flag's own bound: a persisted value the next startup would reject is worse
	// than no persistence at all.
	if err := SetInFile(f, "memory.reserveMB", -5); err == nil {
		t.Error("a negative reserve should be refused")
	}
	if err := SetInFile(f, "log.level", "chatty"); err == nil {
		t.Error("an unparseable log level should be refused")
	}
	if err := SetInFile(f, "log.level", 7); err == nil {
		t.Error("a non-string log level should be refused")
	}
}

func TestPersistableInFile(t *testing.T) {
	for _, key := range []string{"log.level", "memory.budgetMB", "memory.reserveMB",
		"governance.scanRowBudget"} {
		if !PersistableInFile(key) {
			t.Errorf("%s should be persistable", key)
		}
	}
	// cache.commitOpsBytes is live-changeable but environment-only: there is no config-file field
	// for it, so its durable home is the variable, and the control plane has to say so.
	for _, key := range []string{"cache.commitOpsBytes", "storage.dataDir", "no.such.setting"} {
		if PersistableInFile(key) {
			t.Errorf("%s should not be persistable", key)
		}
	}
}

// TestWriteServiceFileRoundTrips is the property that matters: what is written is what a restart
// reads.
func TestWriteServiceFileRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kdb.json")
	original := `{"dataDir":"/data","namespace":"demo/users","memoryBudgetMb":512}`
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}

	file, err := LoadServiceFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := SetInFile(file, "memory.budgetMB", 1024); err != nil {
		t.Fatal(err)
	}
	if err := SetInFile(file, "log.level", "debug"); err != nil {
		t.Fatal(err)
	}
	if err := WriteServiceFile(path, file); err != nil {
		t.Fatal(err)
	}

	back, err := LoadServiceFile(path)
	if err != nil {
		t.Fatalf("the file we just wrote does not load: %v", err)
	}
	if back.MemoryBudgetMB == nil || *back.MemoryBudgetMB != 1024 {
		t.Errorf("memoryBudgetMb did not round-trip: %v", back.MemoryBudgetMB)
	}
	if back.LogLevel == nil || *back.LogLevel != "debug" {
		t.Errorf("logLevel did not round-trip: %v", back.LogLevel)
	}
	// Untouched fields survive: persisting one setting must not drop the rest of the file.
	if back.DataDir == nil || *back.DataDir != "/data" {
		t.Errorf("dataDir was lost: %v", back.DataDir)
	}
	if back.Namespace == nil || *back.Namespace != "demo/users" {
		t.Errorf("namespace was lost: %v", back.Namespace)
	}
}

// TestWriteServiceFileWritesOnlyWhatIsSet: every ServiceFile field is a pointer without omitempty,
// so a naive round-trip would turn a three-line config into thirty lines of nulls. That is
// semantically identical and unreadable, and someone has to maintain this file by hand.
func TestWriteServiceFileWritesOnlyWhatIsSet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kdb.json")
	level := "warn"
	if err := WriteServiceFile(path, &ServiceFile{LogLevel: &level}); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "null") {
		t.Errorf("unset fields were written as nulls:\n%s", body)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, body)
	}
	if len(fields) != 1 || fields["logLevel"] != "warn" {
		t.Errorf("want just logLevel, got %v", fields)
	}
	// Indented and newline-terminated, because a human reads this next.
	if !strings.HasSuffix(string(body), "}\n") || !strings.Contains(string(body), "\n  \"logLevel\"") {
		t.Errorf("not formatted for a human:\n%q", body)
	}
}

// A nested block is pruned like everything else, and kept when it has content.
func TestWriteServiceFilePrunesEmptyNestedBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kdb.json")
	if err := WriteServiceFile(path, &ServiceFile{TLS: &ServiceTLSFile{}}); err != nil {
		t.Fatal(err)
	}
	body, _ := os.ReadFile(path)
	if strings.Contains(string(body), "tls") {
		t.Errorf("an all-nil tls block should not be written at all:\n%s", body)
	}

	cert := "/etc/kdb/tls.crt"
	if err := WriteServiceFile(path, &ServiceFile{TLS: &ServiceTLSFile{CertFile: &cert}}); err != nil {
		t.Fatal(err)
	}
	back, err := LoadServiceFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if back.TLS == nil || back.TLS.CertFile == nil || *back.TLS.CertFile != cert {
		t.Fatalf("a tls block with content did not round-trip: %+v", back.TLS)
	}
	if back.TLS.KeyFile != nil {
		t.Error("the unset fields inside the block should not have been written")
	}
}

// TestWriteServiceFileKeepsPermissions: a config file deliberately made unreadable to other users
// must not become world-readable because a setting was persisted.
func TestWriteServiceFileKeepsPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kdb.json")
	if err := os.WriteFile(path, []byte(`{"logLevel":"info"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	level := "debug"
	if err := WriteServiceFile(path, &ServiceFile{LogLevel: &level}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("permissions changed from 0600 to %o", got)
	}
}

// TestWriteServiceFileLeavesNothingBehind: the temporary file is in the same directory so the
// rename is atomic, and it must not survive either outcome.
func TestWriteServiceFileLeavesNothingBehind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "kdb.json")
	level := "info"
	if err := WriteServiceFile(path, &ServiceFile{LogLevel: &level}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "kdb.json" {
			t.Errorf("left a stray file behind: %s", e.Name())
		}
	}
}

// TestWriteServiceFileRefusesAnUnwritableDirectory: the failure has to be reported, because the
// caller has already applied the change live and needs to tell the operator it is not durable.
func TestWriteServiceFileRefusesAnUnwritableDirectory(t *testing.T) {
	level := "info"
	err := WriteServiceFile(filepath.Join(t.TempDir(), "no-such-dir", "kdb.json"),
		&ServiceFile{LogLevel: &level})
	if err == nil {
		t.Fatal("writing into a directory that does not exist should fail")
	}
	if !strings.Contains(err.Error(), "temporary file") {
		t.Errorf("the error should say what it could not do, got: %v", err)
	}
}
