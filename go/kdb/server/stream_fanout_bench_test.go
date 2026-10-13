package server

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/schema"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// fanoutBenchConn is a subscriber that discards its frames, the way a socket does once written:
// it is a core.SynchronousSender like the TCP and WebSocket connections. Each frame counts down
// the round; the last one in signals the writer.
type fanoutBenchConn struct {
	round *fanoutRound
}

type fanoutRound struct {
	remaining atomic.Int64
	done      chan struct{}
}

func (c *fanoutBenchConn) Send([]byte) error {
	if c.round.remaining.Add(-1) == 0 {
		c.round.done <- struct{}{}
	}
	return nil
}
func (c *fanoutBenchConn) SendCompletesSynchronously() bool { return true }
func (c *fanoutBenchConn) Incoming() <-chan []byte          { return nil }
func (c *fanoutBenchConn) Close() error                     { return nil }
func (c *fanoutBenchConn) TryPoll() []byte                  { return nil }

// BenchmarkStreamFanout is one writer committing one document per op, and every subscriber
// receiving that commit's delta before the next: time and allocations per commit delivered to
// all subscribers (the commit itself included, identical on both sides of an A/B). "filtered"
// gives every subscriber the same filter, which every document matches.
func BenchmarkStreamFanout(b *testing.B) {
	for _, filtered := range []bool{false, true} {
		for _, n := range []int{1, 16, 128} {
			name := fmt.Sprintf("subs=%d", n)
			if filtered {
				name = "filtered/" + name
			}
			b.Run(name, func(b *testing.B) { benchStreamFanout(b, n, filtered) })
		}
	}
}

func benchStreamFanout(b *testing.B, subs int, filtered bool) {
	rt, err := embed.OpenMemoryRuntime("demo", "app/data", schema.None())
	if err != nil {
		b.Fatal(err)
	}
	srt := NewKdbServerRuntime(rt)
	ns := rt.DefaultNamespace
	hub := NewStreamHub(wire.NewCodec(wire.EncodingJSON), ns, srt)
	srt.CommitListener = func(string, document.Commit) { hub.Publish(stream.PublishedCommit{}) }

	round := &fanoutRound{done: make(chan struct{}, 1)}
	conns := make([]*fanoutBenchConn, subs)
	for i := range conns {
		conns[i] = &fanoutBenchConn{round: round}
		req := wire.HandshakePayload{NodeID: fmt.Sprintf("sub-%d", i), Namespaces: []string{ns}, ClientMode: wire.ClientStreamReadOnly}
		if filtered {
			f := `region = 'EU'`
			req.Filter = &f
		}
		_, start := hub.handleHandshake(conns[i], wire.HandshakeMessage{
			H: wire.Header{MessageType: wire.MsgHandshake, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 1}, Request: req,
		})
		if start == nil {
			b.Fatal("handshake refused")
		}
		start()
	}
	defer func() {
		for _, c := range conns {
			hub.unregister(c)
		}
	}()

	body := `{"region":"EU","name":"` + strings.Repeat("x", 300) + `","n":1}`
	ids := make([]codec.UUID, 256)
	for i := range ids {
		if ids[i], err = codec.RandomUUID(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		round.remaining.Store(int64(subs))
		if _, err := srt.Upsert(ns, ids[i%len(ids)], body, auth.Principal{}); err != nil {
			b.Fatal(err)
		}
		<-round.done
	}
	b.StopTimer()
}
