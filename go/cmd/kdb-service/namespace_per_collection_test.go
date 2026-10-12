package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
)

// One namespace per collection through the real service binary: a namespace named by a client is
// opened on demand, gets its own directory (so its own memory and history), and does not see its
// siblings' documents. qtask used to need a `_c` discriminator because it assumed otherwise.
func TestServiceServesOneNamespacePerCollection(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the service binary")
	}
	tmp := t.TempDir()
	bin := filepath.Join(tmp, exeName("kdb-service"))
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	data := filepath.Join(tmp, "data")
	port := freePort(t)
	var out bytes.Buffer
	cmd := exec.Command(bin, "--data-dir", data, "--sql-addr", fmt.Sprintf("tcp://127.0.0.1:%d?bind=true", port))
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("service never listened\n%s", out.String())
		}
	}

	ctx := context.Background()
	c, err := client.Connect(ctx, "tcp://"+addr, "")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const id = "6f9619ff-8b86-d011-b42d-00cf4fc96400"
	collections := []string{"qtask/tasks", "qtask/users", "qtask/projects"}
	for _, ns := range collections {
		if _, err := c.PutJSON(ctx, ns, id, []byte(fmt.Sprintf(`{"collection":%q}`, ns))); err != nil {
			t.Fatalf("write %s: %v", ns, err)
		}
	}
	for _, ns := range collections {
		body, _, err := c.GetJSON(ctx, ns, id)
		if err != nil || !strings.Contains(string(body), ns) {
			t.Fatalf("read %s: %s %v", ns, body, err)
		}
	}
	// Same id in three namespaces holds three different documents; an unwritten one holds none.
	const other = "6f9619ff-8b86-d011-b42d-00cf4fc96401"
	if _, err := c.PutJSON(ctx, "qtask/tasks", other, []byte(`{"only":"tasks"}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.GetJSON(ctx, "qtask/users", other); err == nil {
		t.Fatal("a document written to qtask/tasks is visible in qtask/users")
	}
	// A namespace is a directory of its own under the data root, not a slice of a shared one.
	for _, ns := range collections {
		if _, err := os.Stat(filepath.Join(data, "ns", filepath.FromSlash(ns))); err != nil {
			entries, _ := os.ReadDir(data)
			t.Errorf("no directory for %s under the data root: %v (have %v)", ns, err, entries)
		}
	}
}
