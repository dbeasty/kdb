package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The CLI is the only interface some users ever touch, and its argument parsing and command
// bodies were largely untested. These cover the parse table exhaustively, then drive the real
// commands end to end against a temporary data directory.

func TestParseEveryCommandForm(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want Command
	}{
		{"init", []string{"init", "app/data"}, InitCmd{Namespace: "app/data"}},
		{"put", []string{"put", "app/data", `{"a":1}`}, PutCmd{Namespace: "app/data", Payload: `{"a":1}`}},
		{"get", []string{"get", "app/data", "doc-1"}, GetCmd{Namespace: "app/data", DocID: "doc-1"}},
		{"log", []string{"log", "app/data"}, LogCmd{Namespace: "app/data"}},
		{"status", []string{"status", "app/data"}, StatusCmd{Namespace: "app/data"}},
		{"unlock", []string{"unlock"}, UnlockCmd{}},
		{"branch list", []string{"branch", "list", "app/data"}, BranchListCmd{Namespace: "app/data"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, cmd, err := ParseArgsForTest(tc.args)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if cmd != tc.want {
				t.Fatalf("got %#v, want %#v", cmd, tc.want)
			}
		})
	}
}

// A SQL statement is many argv words; the parser has to rejoin them or every query with a space
// in it loses everything after the first word.
func TestParseQueryRejoinsItsArguments(t *testing.T) {
	_, cmd, err := ParseArgsForTest([]string{"query", "app/data", "SELECT", "*", "FROM", "t"})
	if err != nil {
		t.Fatal(err)
	}
	q, ok := cmd.(QueryCmd)
	if !ok {
		t.Fatalf("got %T, want QueryCmd", cmd)
	}
	if q.SQL != "SELECT * FROM t" {
		t.Fatalf("SQL is %q", q.SQL)
	}
}

func TestParseRejectsIncompleteCommands(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"init with no namespace", []string{"init"}},
		{"put with no payload", []string{"put", "app/data"}},
		{"get with no id", []string{"get", "app/data"}},
		{"query with no sql", []string{"query", "app/data"}},
		{"log with no namespace", []string{"log"}},
		{"status with no namespace", []string{"status"}},
		{"unlock with an extra argument", []string{"unlock", "app/data"}},
		{"branch with no subcommand", []string{"branch"}},
		{"branch list with no namespace", []string{"branch", "list"}},
		{"unknown command", []string{"frobnicate"}},
		{"unknown branch subcommand", []string{"branch", "frobnicate", "app/data"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := ParseArgsForTest(tc.args); err == nil {
				t.Fatalf("accepted %v", tc.args)
			}
		})
	}
}

func TestParseGlobalFlags(t *testing.T) {
	cfg, cmd, err := ParseArgsForTest([]string{"--data-dir", "/tmp/kdb-x", "--quiet", "status", "app/data"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "/tmp/kdb-x" {
		t.Errorf("data dir is %q", cfg.DataDir)
	}
	if !cfg.Quiet {
		t.Error("--quiet was not honoured")
	}
	if cmd != (StatusCmd{Namespace: "app/data"}) {
		t.Errorf("command is %#v", cmd)
	}

	// Flags are positional-independent: they may follow the command as well as precede it.
	cfg, _, err = ParseArgsForTest([]string{"status", "app/data", "--data-dir", "/tmp/kdb-y"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != "/tmp/kdb-y" {
		t.Errorf("trailing --data-dir was not honoured: %q", cfg.DataDir)
	}

	// A --data-dir with nothing after it is an error rather than a silently empty path, which
	// would put the data directory at the process's working directory.
	if _, _, err := ParseArgsForTest([]string{"--data-dir"}); err == nil {
		t.Error("--data-dir with no value was accepted")
	}
}

func TestParseNoArgumentsIsNoCommand(t *testing.T) {
	_, cmd, err := ParseArgsForTest(nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cmd != nil {
		t.Fatalf("got %#v, want no command", cmd)
	}
	// And with only flags, still no command.
	if _, cmd, err = ParseArgsForTest([]string{"--quiet"}); err != nil || cmd != nil {
		t.Fatalf("flags-only gave (%#v, %v)", cmd, err)
	}
}

// ---------------------------------------------------------------- end to end

// runCLI runs the CLI with a temp data dir and captures stdout.
func runCLI(t *testing.T, dir string, args ...string) (int, string) {
	t.Helper()
	full := append([]string{"--data-dir", dir}, args...)

	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	code := Run(full)
	_ = w.Close()
	os.Stdout = orig

	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			sb.Write(buf[:n])
		}
		if readErr != nil {
			break
		}
	}
	_ = r.Close()
	return code, sb.String()
}

// putDocID runs a put and returns the document id from its JSON stdout, which is a small object
// (docId, docIdShort, ...) rather than a bare id.
func putDocID(t *testing.T, dir, namespace, payload string) string {
	t.Helper()
	code, out := runCLI(t, dir, "put", namespace, payload)
	if code != 0 {
		t.Fatalf("put exited %d: %s", code, out)
	}
	var res struct {
		DocID string `json:"docId"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &res); err != nil {
		t.Fatalf("put printed something that is not JSON (%q): %v", out, err)
	}
	if res.DocID == "" {
		t.Fatalf("put printed no docId: %q", out)
	}
	return res.DocID
}

func TestPutThenGetRoundTripsThroughTheCLI(t *testing.T) {
	dir := t.TempDir()

	if code, _ := runCLI(t, dir, "init", "app/data"); code != 0 {
		t.Fatalf("init exited %d", code)
	}

	docID := putDocID(t, dir, "app/data", `{"title":"hello"}`)

	code, out := runCLI(t, dir, "get", "app/data", docID)
	if code != 0 {
		t.Fatalf("get exited %d: %s", code, out)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("get printed something that is not JSON (%q): %v", out, err)
	}
	if got["title"] != "hello" {
		t.Fatalf("round trip gave %v", got)
	}
}

// The data written by one process has to be there for the next one - each runCLI call opens and
// closes its own runtime, so this is a restart in all but name.
func TestDataSurvivesBetweenCLIInvocations(t *testing.T) {
	dir := t.TempDir()
	runCLI(t, dir, "init", "app/data")

	var ids []string
	for _, body := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		ids = append(ids, putDocID(t, dir, "app/data", body))
	}

	for i, id := range ids {
		code, out := runCLI(t, dir, "get", "app/data", id)
		if code != 0 {
			t.Fatalf("get %d exited %d: %s", i, code, out)
		}
		if !strings.Contains(out, "\"n\"") {
			t.Fatalf("document %d reads back as %q", i, out)
		}
	}
}

func TestGetOfAnUnknownDocumentFails(t *testing.T) {
	dir := t.TempDir()
	runCLI(t, dir, "init", "app/data")
	if code, _ := runCLI(t, dir, "get", "app/data", "11111111-2222-4333-8444-555555555555"); code == 0 {
		t.Fatal("getting a document that does not exist exited 0")
	}
}

func TestStatusAndLogRunOnAFreshNamespace(t *testing.T) {
	dir := t.TempDir()
	runCLI(t, dir, "init", "app/data")

	if code, out := runCLI(t, dir, "status", "app/data"); code != 0 {
		t.Fatalf("status exited %d: %s", code, out)
	}
	if code, out := runCLI(t, dir, "log", "app/data"); code != 0 {
		t.Fatalf("log exited %d: %s", code, out)
	}
}

func TestBranchListShowsTheDefaultBranch(t *testing.T) {
	dir := t.TempDir()
	runCLI(t, dir, "init", "app/data")

	code, out := runCLI(t, dir, "branch", "list", "app/data")
	if code != 0 {
		t.Fatalf("branch list exited %d: %s", code, out)
	}
	if strings.TrimSpace(out) == "" {
		t.Fatal("branch list printed nothing - a namespace always has at least its default branch")
	}
}

func TestPutAcceptsAFilePayload(t *testing.T) {
	dir := t.TempDir()
	runCLI(t, dir, "init", "app/data")

	payload := filepath.Join(t.TempDir(), "doc.json")
	if err := os.WriteFile(payload, []byte(`{"from":"file"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	docID := putDocID(t, dir, "app/data", payload)

	code, out := runCLI(t, dir, "get", "app/data", docID)
	if code != 0 {
		t.Fatalf("get exited %d: %s", code, out)
	}
	if !strings.Contains(out, `"from"`) {
		t.Fatalf("the file's contents did not land: %q", out)
	}
}

func TestUnknownCommandExitsNonZero(t *testing.T) {
	dir := t.TempDir()
	if code, _ := runCLI(t, dir, "frobnicate"); code == 0 {
		t.Fatal("an unknown command exited 0")
	}
	if code, _ := runCLI(t, dir); code == 0 {
		t.Fatal("no command at all exited 0")
	}
}
