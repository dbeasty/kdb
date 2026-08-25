package wire_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

func TestTransactionCodecRoundTrip(t *testing.T) {
	docID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	deleteID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	authorID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	base := repeatHex(0xab)
	tx := document.Transaction{
		ID:          txID,
		BaseVersion: base,
		Operations: []document.Op{
			document.WriteOp{DocID: docID, Patch: `{"v":"hello"}`},
			document.DeleteOp{DocID: deleteID},
		},
		Timestamp:    codec.TimestampNow(),
		AuthorNodeID: authorID,
	}

	encoded, err := wire.EncodeTransaction(tx)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := wire.DecodeTransaction(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.ID != tx.ID {
		t.Fatalf("id: got %v want %v", decoded.ID, tx.ID)
	}
	if decoded.BaseVersion != tx.BaseVersion {
		t.Fatalf("baseVersion: got %v want %v", decoded.BaseVersion, tx.BaseVersion)
	}
	if decoded.AuthorNodeID != tx.AuthorNodeID {
		t.Fatalf("authorNodeId: got %v want %v", decoded.AuthorNodeID, tx.AuthorNodeID)
	}
	if decoded.Timestamp.EpochMicros() != tx.Timestamp.EpochMicros() {
		t.Fatalf("timestamp: got %v want %v", decoded.Timestamp, tx.Timestamp)
	}
	if len(decoded.Operations) != 2 {
		t.Fatalf("operations: %+v", decoded.Operations)
	}
	write, ok := decoded.Operations[0].(document.WriteOp)
	if !ok || write.DocID != docID || write.Patch != `{"v":"hello"}` {
		t.Fatalf("write op: %+v", decoded.Operations[0])
	}
	del, ok := decoded.Operations[1].(document.DeleteOp)
	if !ok || del.DocID != deleteID {
		t.Fatalf("delete op: %+v", decoded.Operations[1])
	}
}
