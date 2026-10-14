package server

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// A subscriber that offers the binary wire features gets DeltaCommit frames as binary bodies; one
// that offers none gets JSON as before; both decode to the same deltas. Subscribers with the same
// filter but different encodings must not share fan-out frames.
func TestStreamSubscribersGetDeltasInTheEncodingTheyAgreed(t *testing.T) {
	rt := newTestRuntime(t)
	ns := rt.Runtime.DefaultNamespace
	hub := hubFor(t, rt)
	hub.SetAllowAnonymous(true)

	type sub struct {
		name     string
		features []string
		filter   string
		conn     *capturingConn
	}
	subs := []*sub{
		{name: "json", conn: &capturingConn{}},
		{name: "binary", features: wire.SupportedWireFeatures, conn: &capturingConn{}},
		{name: "binary-core-only", features: []string{wire.WireFeatureBinary}, conn: &capturingConn{}},
		{name: "json-eu", filter: `region = 'EU'`, conn: &capturingConn{}},
		{name: "binary-eu", features: wire.SupportedWireFeatures, filter: `region = 'EU'`, conn: &capturingConn{}},
	}
	for _, s := range subs {
		req := wire.HandshakePayload{NodeID: s.name, Namespaces: []string{ns}, ClientMode: wire.ClientStreamReadOnly, WireFeatures: s.features}
		if s.filter != "" {
			f := s.filter
			req.Filter = &f
		}
		ackFrame, begin := hub.handleHandshake(syncCapturingConn{s.conn}, wire.HandshakeMessage{
			H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}, Request: req,
		})
		if begin == nil {
			t.Fatalf("%s: handshake refused", s.name)
		}
		ack, err := wire.NewCodec(wire.EncodingJSON).Decode(ackFrame)
		if err != nil {
			t.Fatal(err)
		}
		// Binary DeltaCommit bodies need binary-frames-2; a subscriber offering only the core
		// feature is acked nothing and stays on JSON.
		want := []string(nil)
		if slices.Contains(s.features, wire.WireFeatureBinary2) {
			want = wire.NegotiateWireFeatures(s.features)
		}
		if got := ack.(wire.HandshakeAckMessage).Response.WireFeatures; !slices.Equal(got, want) {
			t.Fatalf("%s: acked %v, want %v", s.name, got, want)
		}
		begin()
		conn := s.conn
		t.Cleanup(func() { hub.unregister(syncCapturingConn{conn}) })
	}

	for i := 0; i < 6; i++ {
		region := []string{"EU", "US"}[i%2]
		if _, err := rt.Upsert(ns, mustRandomUUID(t), fmt.Sprintf(`{"region":%q,"q":"a \"quoted\" value","i":%d}`, region, i), auth.Principal{}); err != nil {
			t.Fatal(err)
		}
	}
	head, _ := rt.dag.Head()
	deadline := time.Now().Add(10 * time.Second)
	for _, s := range subs {
		for s.conn.at() != head {
			if time.Now().After(deadline) {
				t.Fatalf("%s stuck short of the head", s.name)
			}
			time.Sleep(time.Millisecond)
		}
	}

	decode := func(s *sub) []wire.DeltaCommitPayload {
		var out []wire.DeltaCommitPayload
		for _, f := range s.conn.snapshot() {
			wantTag := byte(1)
			if slices.Contains(s.features, wire.WireFeatureBinary2) {
				wantTag = 2
			}
			if f[wire.FrameHeaderSize] != wantTag {
				t.Fatalf("%s: frame tag %d, want %d", s.name, f[wire.FrameHeaderSize], wantTag)
			}
			msg, err := wire.NewCodec(wire.EncodingJSON).Decode(f)
			if err != nil {
				t.Fatal(err)
			}
			p := msg.(wire.DeltaCommitMessage).Payload
			if p.Operations == nil {
				p.Operations = []document.Op{}
			}
			if len(p.IndexHints) == 0 {
				p.IndexHints = nil
			}
			out = append(out, p)
		}
		return out
	}
	byName := map[string][]wire.DeltaCommitPayload{}
	for _, s := range subs {
		byName[s.name] = decode(s)
	}
	for _, pair := range [][2]string{{"json", "binary"}, {"json", "binary-core-only"}, {"json-eu", "binary-eu"}} {
		if !reflect.DeepEqual(byName[pair[0]], byName[pair[1]]) {
			t.Fatalf("%s and %s received different deltas:\n%+v\n%+v", pair[0], pair[1], byName[pair[0]], byName[pair[1]])
		}
	}
	if len(byName["json"]) != 6 {
		t.Fatalf("json subscriber got %d deltas, want 6", len(byName["json"]))
	}
}
