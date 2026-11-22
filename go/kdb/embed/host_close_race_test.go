package embed

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/schema"
)

// An open of a namespace must not start while the previous engine over the same files is still
// closing (flushing, sealing, checkpointing) - CloseNamespace unroutes the namespace before it
// tears it down, so without a guard a concurrent Namespace call saw it absent and opened a second
// engine over files the first was still writing. Host.Close likewise must not release the
// directory lock under a close still in flight.
func TestNamespaceOpenWaitsForInFlightClose(t *testing.T) {
	root := t.TempDir()
	h, err := OpenFileHost(root, FileRuntimeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	const ns = "app/race"
	if _, err := h.Namespace("app", ns, schema.None()); err != nil {
		t.Fatal(err)
	}

	// Hold the namespace's close open until released, standing in for a slow seal.
	release := make(chan struct{})
	entered := make(chan struct{})
	h.mu.Lock()
	entry := h.nss[ns]
	realClose := entry.close
	entry.close = func() error {
		close(entered)
		<-release
		return realClose()
	}
	h.mu.Unlock()

	closeDone := make(chan error, 1)
	go func() { closeDone <- h.CloseNamespace(ns) }()
	<-entered

	openDone := make(chan error, 1)
	go func() {
		_, err := h.Namespace("app", ns, schema.None())
		openDone <- err
	}()
	select {
	case err := <-openDone:
		close(release)
		<-closeDone
		t.Fatalf("namespace reopened (err %v) while its previous engine was still closing", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if err := <-closeDone; err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := <-openDone; err != nil {
		t.Fatalf("open after close: %v", err)
	}
}
