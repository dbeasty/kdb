package backup

import (
	"bytes"
	"encoding/json"
	"testing"
)

func compactJSON(t *testing.T, raw []byte) string {
	t.Helper()
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// U5: definitions handed to a backup are in its manifest, and a restore that fetched it leaves
// them where the server adopts them; a backup without them leaves nothing.
func TestBackupCarriesDefinitionsToTheRestore(t *testing.T) {
	shim := newTestShim(t)
	commits := chain(t, 2)
	appendSegment(t, shim, 0, commits[0], commits[1])
	store := &DirStore{Root: t.TempDir()}
	defs := json.RawMessage(`[{"id":"00000000-0000-4000-8000-000000000001","body":"{\"kind\":\"index\"}"}]`)
	m, err := CreateWithOptions(shim, ns, store, "", CreateOptions{Definitions: defs})
	if err != nil {
		t.Fatal(err)
	}
	fetched, err := FetchToDir(store, ns, m.BackupID, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if compactJSON(t, fetched.Definitions) != compactJSON(t, defs) {
		t.Fatalf("manifest definitions %s, want %s", fetched.Definitions, defs)
	}
	out := t.TempDir()
	if err := WriteRestoredDefinitions(out, fetched); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadRestoredDefinitions(out, ns)
	if err != nil || !ok || compactJSON(t, got) != compactJSON(t, defs) {
		t.Fatalf("restored definitions %s ok=%v err=%v", got, ok, err)
	}
	if err := RemoveRestoredDefinitions(out, ns); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := ReadRestoredDefinitions(out, ns); ok {
		t.Fatal("definitions file still there after removal")
	}

	plain, err := Create(shim, ns, store, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(plain.Definitions) != 0 {
		t.Fatal("a backup made without definitions carries some")
	}
	empty := t.TempDir()
	if err := WriteRestoredDefinitions(empty, plain); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := ReadRestoredDefinitions(empty, ns); ok {
		t.Fatal("a backup without definitions left a definitions file")
	}
}
