package integration

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/sql"
)

func TestFullStackMemory(t *testing.T) {
	rt, err := embed.OpenMemoryRuntime("demo", "demo/users", schema.None())
	if err != nil {
		t.Fatal(err)
	}
	d, ok := rt.DAG.(*dag.InMemoryCommitDag)
	if !ok {
		t.Fatal("expected InMemoryCommitDag")
	}
	engine := sql.NewEngine(rt.Storage, d)
	ctx := sql.QueryContext{NamespaceID: rt.DefaultNamespace, Schema: schema.None()}
	created, err := engine.Execute("CREATE TABLE users (id VARCHAR NOT NULL, name VARCHAR NOT NULL)", ctx)
	if err != nil {
		t.Fatal(err)
	}
	if created.AppliedSchema == nil {
		t.Fatal("expected schema")
	}
	sch := *created.AppliedSchema
	_, err = engine.ExecuteDML("INSERT INTO users (id, name) VALUES ('1', 'alice')", sql.QueryContext{
		NamespaceID: rt.DefaultNamespace,
		Schema:      sch,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := engine.Execute("SELECT COUNT(*) AS n FROM users", sql.QueryContext{
		NamespaceID: rt.DefaultNamespace,
		Schema:      sch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) == 0 {
		t.Fatal("expected count row")
	}
}

func TestGoCliFilePutThenGetCrossProcess(t *testing.T) {
	root := t.TempDir()
	ns := "app/t"
	kdb := filepath.Join("..", "..", "bin", "kdb")

	build := exec.Command("go", "build", "-o", kdb, "./cmd/kdb")
	build.Dir = filepath.Join("..", "..")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}

	put := exec.Command(kdb, "--data-dir", root, "put", ns, `{"name":"Ada"}`)
	put.Dir = filepath.Join("..", "..")
	out, err := put.CombinedOutput()
	if err != nil {
		t.Fatalf("put: %v\n%s", err, out)
	}
	if len(out) == 0 {
		t.Fatal("expected stdout from put")
	}

	// Give filesystem a moment; delta writer flushes but tests sometimes race on slow disks.
	time.Sleep(10 * time.Millisecond)

	var parsed struct {
		DocID string `json:"docId"`
	}
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("parse put json: %v\n%s", err, out)
	}
	docID := parsed.DocID
	if docID == "" {
		t.Fatalf("could not parse docId from %q", string(out))
	}

	get := exec.Command(kdb, "--data-dir", root, "get", ns, docID)
	get.Dir = filepath.Join("..", "..")
	got, err := get.CombinedOutput()
	if err != nil {
		t.Fatalf("get: %v\n%s", err, got)
	}
	if string(got) == "" {
		t.Fatal("expected json output")
	}
}

func TestKotlinPutThenGoGet_InteropDelta(t *testing.T) {
	root := t.TempDir()
	ns := "app/t"
	docID := "00000000-0000-0000-0000-0000000000ff"
	repo := findRepoRoot(t)

	docPath := filepath.Join(root, "doc.json")
	if err := os.WriteFile(docPath, []byte(`{"id":"`+docID+`","v":1}`), 0o644); err != nil {
		t.Fatal(err)
	}

	// Kotlin write.
	kt := exec.Command(filepath.Join(repo, "gradlew"), ":kdb-cli:runCli", "--args=--data-dir "+root+" put "+ns+" "+docPath, "--quiet")
	kt.Dir = repo
	if out, err := kt.CombinedOutput(); err != nil {
		t.Fatalf("kotlin put: %v\n%s", err, out)
	}

	// Go read (build once per test to keep it self-contained).
	kdb := filepath.Join(repo, "go", "bin", "kdb")
	build := exec.Command("go", "build", "-o", kdb, "./cmd/kdb")
	build.Dir = filepath.Join(repo, "go")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	get := exec.Command(kdb, "--data-dir", root, "get", ns, docID)
	get.Dir = repo
	got, err := get.CombinedOutput()
	if err != nil {
		t.Fatalf("go get: %v\n%s", err, got)
	}
	if string(got) == "" {
		t.Fatal("expected json output")
	}
}

func findRepoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	cur := wd
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(cur, "gradlew")); err == nil {
			return cur
		}
		next := filepath.Dir(cur)
		if next == cur {
			break
		}
		cur = next
	}
	t.Fatalf("could not find repo root from %s", wd)
	return ""
}
