package transaction_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/storage/mem"
	"github.com/limidus/kdb/go/kdb/transaction"
)

// Cross-language fixture for what a merge-patch write commits: a write of {"i":2,"pad":"pp"} and
// then a merge patch {"i":2,"rewrite":2} over it, with fixed ids and timestamps. The commit records
// the merged document (operationsAsStored), so its hash covers that document; the Kotlin engine
// must produce the same two hashes and the same stored op (kdb-jdbc MergePatchGoldenTest).
//
// merge_patch_commits.txt: the first commit's hash, the second's, then the second's stored patch.
func TestExportMergePatchGolden(t *testing.T) {
	lines := mergePatchFixture(t)
	dir := filepath.Join("..", "..", "testdata", "golden", "physical")
	if err := os.WriteFile(filepath.Join(dir, "go", "merge_patch_commits.txt"), []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Kotlin's copy, when its exporter has run; absent is a setup gap, not a parity failure.
	if raw, err := os.ReadFile(filepath.Join(dir, "kotlin", "merge_patch_commits.txt")); err == nil {
		if got := strings.Split(strings.TrimSpace(string(raw)), "\n"); strings.Join(got, "\n") != strings.Join(lines, "\n") {
			t.Fatalf("Kotlin commits a merge patch differently.\nkotlin: %q\n    go: %q", got, lines)
		}
	}
}

func mergePatchFixture(t *testing.T) []string {
	t.Helper()
	ns := "golden/merge"
	d, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	store := mem.NewInMemoryStorageAdapter()
	engine := transaction.NewEngine(transaction.ConflictPolicyStrict, nil)
	docID := codec.UUID{MSB: 0x0102030405060708, LSB: 0x090a0b0c0d0e0f10}
	author := codec.UUID{MSB: 0x2122232425262728, LSB: 0x292a2b2c2d2e2f30}
	commit := func(n int64, patch string) document.Commit {
		base, err := d.Head()
		if err != nil {
			t.Fatal(err)
		}
		tx := document.Transaction{
			ID:           codec.UUID{MSB: 0x1111111111111111, LSB: n},
			BaseVersion:  base,
			Operations:   []document.Op{document.WriteOp{DocID: docID, Patch: patch}},
			Timestamp:    codec.Timestamp{EpochMillis: 1_700_000_000_000 + n, MicroRemainder: 7},
			AuthorNodeID: author,
		}
		res := mustCommit(t, engine, tx, d, store)
		ok, isOK := res.(transaction.ResultSuccess)
		if !isOK {
			t.Fatalf("commit %d: %#v", n, res)
		}
		return ok.Commit
	}
	first := commit(1, `{"i":2,"pad":"pp"}`)
	second := commit(2, `{"i":2,"rewrite":2}`)
	stored := second.Operations[0].(document.WriteOp).Patch
	if stored == `{"i":2,"rewrite":2}` {
		t.Fatalf("the commit records the patch, not the merged document")
	}
	return []string{first.Hash.Hex(), second.Hash.Hex(), stored}
}
