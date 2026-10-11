package stream_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

// TestInMemoryTransportBurstLosesNoFrame sends far more frames than the memory:// link buffers
// while nobody is reading, then drains. Every frame must arrive, in order: a slow reader is caught
// up when it drains, it does not lose what was sent while it was slow.
func TestInMemoryTransportBurstLosesNoFrame(t *testing.T) {
	const total = 500
	name := fmt.Sprintf("burst-%d", time.Now().UnixNano())
	conn, err := stream.NewInMemoryTransport().Connect("memory://" + name)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	hub := stream.HubFor(name)

	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := 0; i < total; i++ {
			hub.ServerSend([]byte(fmt.Sprintf("frame-%d", i)))
		}
	}()
	// Let the sender run well past the buffer before reading anything.
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < total; i++ {
		select {
		case frame, ok := <-conn.Incoming():
			if !ok {
				t.Fatalf("incoming closed after %d of %d frames", i, total)
			}
			if want := fmt.Sprintf("frame-%d", i); string(frame) != want {
				t.Fatalf("frame %d = %q, want %q (frames were dropped)", i, frame, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("received %d of %d frames; the rest were dropped", i, total)
		}
	}
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("sender still blocked after every frame was drained")
	}
}

// TestInMemoryTransportCloseDuringBlockedSend fills a link's buffer so the next send has to wait,
// then closes the link while senders are waiting. The send must actually wait (not drop), Close
// must not deadlock against it, every waiting sender must return, nothing may panic with a send on
// a closed channel, and Incoming must still end closed. Run under -race.
func TestInMemoryTransportCloseDuringBlockedSend(t *testing.T) {
	for round := 0; round < 20; round++ {
		name := fmt.Sprintf("close-blocked-%d-%d", time.Now().UnixNano(), round)
		conn, err := stream.NewInMemoryTransport().Connect("memory://" + name)
		if err != nil {
			t.Fatal(err)
		}
		hub := stream.HubFor(name)

		// Fill the buffer: these all fit, so ServerSend returns for each.
		for i := 0; i < cap(conn.Incoming()); i++ {
			hub.ServerSend([]byte("fill"))
		}
		// Several more senders queue up behind the full buffer.
		var senders sync.WaitGroup
		for i := 0; i < 8; i++ {
			senders.Add(1)
			go func() {
				defer senders.Done()
				hub.ServerSend([]byte("blocked"))
			}()
		}
		returned := make(chan struct{})
		go func() { senders.Wait(); close(returned) }()
		select {
		case <-returned:
			t.Fatalf("round %d: sends past a full buffer returned at once; the frames were dropped", round)
		case <-time.After(100 * time.Millisecond):
		}

		closed := make(chan error, 1)
		go func() { closed <- conn.Close() }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: Close deadlocked against a blocked send", round)
		}
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: blocked senders never returned after Close", round)
		}
		deadline := time.After(2 * time.Second)
	drain:
		for {
			select {
			case _, ok := <-conn.Incoming():
				if !ok {
					break drain
				}
			case <-deadline:
				t.Fatalf("round %d: Incoming never closed", round)
			}
		}
		// A send after Close is a no-op, not a panic.
		hub.ServerSend([]byte("after-close"))
	}
}

// TestFullEventBufferCannotSwallowDesync leaves a subscriber's events undrained, fills them with
// compaction notices, and then sends a commit whose parent is not the subscriber's position. The
// subscriber must not end up out of sync without being told: either the desync error reaches the
// consumer, or the subscription is closed with a terminal event the consumer sees before anything
// is lost.
func TestFullEventBufferCannotSwallowDesync(t *testing.T) {
	w := wire.NewCodec(wire.EncodingKdbBinary)
	for _, notices := range []int{0, 30, 31, 32, 40} {
		t.Run(fmt.Sprintf("notices=%d", notices), func(t *testing.T) {
			ns := fmt.Sprintf("desync-%d-%d", notices, time.Now().UnixNano())
			parent := repeatHex(0x11)
			transport := stream.NewInMemoryTransport()
			coordinator := stream.NewCoordinator(w, transport)
			if err := coordinator.Start(stream.SessionConfig{
				NamespaceID:  ns,
				NodeID:       "coord",
				HeadProvider: func() (codec.Hash, error) { return parent, nil },
			}); err != nil {
				t.Fatal(err)
			}
			defer coordinator.Stop()

			subscriber := stream.NewSubscriber(w, transport, nil)
			if _, err := subscriber.Connect(stream.SubscriberConfig{
				NamespaceID:    ns,
				NodeID:         "sub",
				Mode:           stream.ClientReadOnly,
				CoordinatorURI: "memory://" + ns,
				ResumeFrom:     &parent,
			}); err != nil {
				t.Fatal(err)
			}
			defer subscriber.Disconnect()
			events := subscriber.Events()

			hub := stream.HubFor(ns)
			for i := 0; i < notices; i++ {
				frame, err := w.Encode(wire.CompactionNoticeMessage{
					H:      wire.Header{MessageType: wire.MsgCompactionNotice, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 7000 + i},
					Intent: wire.CompactionIntent{NamespaceID: ns, Boundary: repeatHex(0x33)},
				})
				if err != nil {
					t.Fatal(err)
				}
				hub.ServerSend(frame)
			}
			wrongParent := repeatHex(0x99)
			frame, err := w.Encode(wire.DeltaCommitMessage{
				H: wire.Header{MessageType: wire.MsgDeltaCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: 8000},
				Payload: wire.DeltaCommitPayload{
					Namespace:  ns,
					CommitHash: repeatHex(0x44),
					ParentHash: wrongParent,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			hub.ServerSend(frame)

			// Wait until the reader has handled every frame it is going to (the buffer fills, or
			// the count stops moving), without draining anything.
			want := notices + 1
			if want > cap(events) {
				want = cap(events)
			}
			deadline := time.Now().Add(2 * time.Second)
			for len(events) < want && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond)

			var drained []stream.Event
			for {
				select {
				case ev := <-events:
					drained = append(drained, ev)
					continue
				default:
				}
				break
			}

			sawDesync := false
			for _, ev := range drained {
				var desync *stream.DesyncError
				if ev.Cause != nil && errors.As(ev.Cause, &desync) {
					sawDesync = true
				}
			}
			terminal := len(drained) > 0 &&
				drained[len(drained)-1].Kind == stream.EventDisconnected &&
				drained[len(drained)-1].Cause != nil
			if !sawDesync && !terminal {
				t.Fatalf("%d events drained after %d notices and a desync commit: no desync error and no terminal event - the subscriber is out of sync and was never told", len(drained), notices)
			}
			if notices < cap(events) && !sawDesync {
				t.Fatalf("buffer had room for the desync error (%d notices, capacity %d) but it never arrived", notices, cap(events))
			}
		})
	}
}
