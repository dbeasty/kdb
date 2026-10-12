package main

import (
	"bytes"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Two instances that differ only in their SQL address must both start: the peer-sync and stream
// listeners used to bind 9091/9092 by default, so the second one failed with "address in use".
func TestTwoDefaultInstancesStartSideBySide(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the service binary")
	}
	bin := filepath.Join(t.TempDir(), "kdb-service")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	for i := 0; i < 2; i++ {
		port := freePort(t)
		var out bytes.Buffer
		cmd := exec.Command(bin, "--memory", "--sql-addr", fmt.Sprintf("tcp://127.0.0.1:%d?bind=true", port))
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		// One goroutine owns Wait; cleanup kills the process and waits for that goroutine. Calling
		// cmd.Wait a second time from the deferred cleanup, concurrently with this one, could block
		// for ever on the output pipe with the process still running: it hung this test 6 runs in
		// 8 on a laptop and timed the CI job out at 10 minutes.
		done := make(chan struct{})
		var waitErr error
		go func() { waitErr = cmd.Wait(); close(done) }()
		defer func(c *exec.Cmd) { _ = c.Process.Kill(); <-done }(cmd)
		deadline := time.Now().Add(10 * time.Second)
		for {
			select {
			case <-done:
				t.Fatalf("instance %d exited early: %v\n%s", i, waitErr, out.String())
			default:
			}
			if c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond); err == nil {
				c.Close()
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("instance %d never listened\n%s", i, out.String())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
