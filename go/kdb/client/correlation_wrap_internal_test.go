package client

import (
	"math"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

// Every id nextCorrelation hands out must survive the int32 the wire header carries it in, or the
// reply comes back under a different id than the one its request is waiting on.
func TestNextCorrelationStaysWithinTheWireInt32(t *testing.T) {
	c := &Client{correlation: math.MaxInt32 - 1}
	codec := wire.NewCodec(wire.EncodingJSON)
	for i := 0; i < 4; i++ {
		id := c.nextCorrelation()
		frame, err := codec.Encode(wire.TxRollbackMessage{H: wire.Header{MessageType: wire.MsgTxRollback, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: id}})
		if err != nil {
			t.Fatal(err)
		}
		h, err := codec.DecodeHeader(frame)
		if err != nil {
			t.Fatal(err)
		}
		if h.CorrelationID != id || id <= 0 {
			t.Fatalf("id %d decoded back as %d after a wire round trip", id, h.CorrelationID)
		}
	}
}
