package sql

import (
	"bytes"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

// An index access path's ids, as a keyset page reads them: document id order, only those after the
// key, nothing dropped or repeated.
func TestIDsAfterInOrder(t *testing.T) {
	var ids []codec.UUID
	for i := 0; i < 200; i++ {
		id, _ := codec.RandomUUID()
		ids = append(ids, id)
	}
	all := idsAfterInOrder(ids, nil)
	if len(all) != len(ids) {
		t.Fatalf("no key: %d ids, want %d", len(all), len(ids))
	}
	for i := 1; i < len(all); i++ {
		if bytes.Compare(all[i-1].Bytes(), all[i].Bytes()) >= 0 {
			t.Fatal("not in id order")
		}
	}
	key := all[99]
	rest := idsAfterInOrder(ids, &key)
	if len(rest) != 100 || rest[0] != all[100] || rest[99] != all[199] {
		t.Fatalf("after the 100th id: %d ids", len(rest))
	}
}
