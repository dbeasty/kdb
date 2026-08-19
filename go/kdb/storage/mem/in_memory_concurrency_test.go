package mem

import (
	"strconv"
	"sync"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// TestConcurrentWriteBlobAcrossShards exercises blob_shard.go: many
// goroutines writing distinct blobs concurrently, read back for
// correctness. Run with -race to catch any shard-selection or map-access
// bug from the mutex-per-namespace/hash split.
func TestConcurrentWriteBlobAcrossShards(t *testing.T) {
	a := NewInMemoryStorageAdapter()
	const n = 500
	hashes := make([]codec.Hash, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			h, err := a.WriteBlob([]byte{byte(i), byte(i >> 8)})
			if err != nil {
				t.Errorf("WriteBlob(%d): %v", i, err)
				return
			}
			hashes[i] = h
		}()
	}
	wg.Wait()

	for i, h := range hashes {
		got, err := a.ReadBlob(h)
		if err != nil {
			t.Fatalf("ReadBlob(%d): %v", i, err)
		}
		if len(got) != 2 || got[0] != byte(i) || got[1] != byte(i>>8) {
			t.Fatalf("ReadBlob(%d) = %v, want [%d %d]", i, got, byte(i), byte(i>>8))
		}
	}
}

// TestConcurrentPutDocumentAcrossNamespaces exercises pending_shard.go:
// concurrent PutDocument calls across many namespaces, then commits each
// and checks every document landed.
func TestConcurrentPutDocumentAcrossNamespaces(t *testing.T) {
	a := NewInMemoryStorageAdapter()
	const namespaces = 20
	const docsPerNamespace = 25

	nsIDs := make([]string, namespaces)
	for i := range nsIDs {
		nsIDs[i] = "ns" + string(rune('a'+i))
	}

	var wg sync.WaitGroup
	for _, ns := range nsIDs {
		ns := ns
		for j := 0; j < docsPerNamespace; j++ {
			j := j
			wg.Add(1)
			go func() {
				defer wg.Done()
				id, _ := codec.RandomUUID()
				doc, err := document.FromJSONWithID(id, `{"v":`+strconv.Itoa(j)+`}`)
				if err != nil {
					t.Errorf("FromJSONWithID: %v", err)
					return
				}
				if err := a.PutDocument(ns, doc); err != nil {
					t.Errorf("PutDocument(%s): %v", ns, err)
				}
			}()
		}
	}
	wg.Wait()

	empty := document.EmptyDocumentTree()
	for _, ns := range nsIDs {
		tree, err := a.CommitTree(ns, empty.TreeHash)
		if err != nil {
			t.Fatalf("CommitTree(%s): %v", ns, err)
		}
		if tree.Size() != docsPerNamespace {
			t.Fatalf("CommitTree(%s) size=%d, want %d", ns, tree.Size(), docsPerNamespace)
		}
	}
}
