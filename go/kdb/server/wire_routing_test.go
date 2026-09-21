package server

import (
	"fmt"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Sessionless write frames - UPSERT with no session, TRANSACTION_REPLAY - resolve their runtime
// from the namespace they name, just as DOCUMENT_GET does. The Go client always sends a session
// with UPSERT, so this is the only place the sessionless route is exercised.
func TestSessionlessWritesRouteByNamespace(t *testing.T) {
	host, set := fileSet(t, t.TempDir(), "bank/accounts", "bank/ledger")
	defer host.Close()
	primary, _ := set.Get("bank/accounts")
	for _, rt := range set.Runtimes() {
		rt.Namespaces = set
	}
	ln, err := ListenSqlWire("tcp://127.0.0.1:0?bind=true", primary)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c := dialRawWireClient(t, fmt.Sprintf("tcp://%s", ln.Addr().String()))
	c.handshake(t, wire.ClientSQL, "bank/accounts")

	up := mustUUID(t)
	if r := c.upsert(t, "bank/ledger", "", up.String(), `{"u":1}`); r.Error != nil {
		t.Fatalf("sessionless upsert: %s", *r.Error)
	}
	if _, found := docAtHead(t, set, "bank/ledger", up); !found {
		t.Error("a sessionless UPSERT into bank/ledger did not land there")
	}
	if _, found := docAtHead(t, set, "bank/accounts", up); found {
		t.Error("a sessionless UPSERT into bank/ledger landed in the listener's namespace")
	}

	replayed := mustUUID(t)
	encoded, err := wire.EncodeTransaction(document.Transaction{
		ID: mustUUID(t), BaseVersion: headOf(t, set, "bank/ledger"), Timestamp: codec.TimestampNow(),
		Operations: []document.Op{document.WriteOp{DocID: replayed, Patch: `{"r":1}`}},
	})
	if err != nil {
		t.Fatal(err)
	}
	reply := c.request(t, wire.TransactionReplayMessage{
		H:                header(c.nextCorrelation(), wire.MsgTransactionReplay),
		Namespace:        "bank/ledger",
		BaseVersion:      headOf(t, set, "bank/ledger"),
		TransactionBytes: encoded,
	})
	if r, ok := reply.(wire.SqlResultMessage); !ok || r.Error != nil {
		t.Fatalf("replay: %#v", reply)
	}
	if _, found := docAtHead(t, set, "bank/ledger", replayed); !found {
		t.Error("TRANSACTION_REPLAY into bank/ledger did not land there")
	}
	if _, found := docAtHead(t, set, "bank/accounts", replayed); found {
		t.Error("TRANSACTION_REPLAY into bank/ledger landed in the listener's namespace")
	}
}
