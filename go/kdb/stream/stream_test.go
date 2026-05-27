package stream_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/dag"
	"github.com/limidus/kdb/go/kdb/stream"
	"github.com/limidus/kdb/go/kdb/wire"
)

func repeatHex(b byte) codec.Hash {
	h, _ := codec.HashFromHex(strings.Repeat(fmt.Sprintf("%02x", b), 32))
	return h
}

func TestPublishDeliversDeltaToSubscriber(t *testing.T) {
	ns := "coord-test"
	w := wire.NewCodec(wire.EncodingKdbBinary)
	transport := stream.NewInMemoryTransport()
	dagInst, err := dag.NewInMemoryCommitDag(ns)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := dagInst.Head()
	if err != nil {
		t.Fatal(err)
	}
	child := repeatHex(0x22)

	coordinator := stream.NewCoordinator(w, transport)
	if err := coordinator.Start(stream.SessionConfig{
		NamespaceID: ns,
		NodeID:      "coord",
		HeadProvider: func() (codec.Hash, error) { return parent, nil },
	}); err != nil {
		t.Fatal(err)
	}
	defer coordinator.Stop()

	subscriber := stream.NewSubscriber(w, transport, nil)
	conn, err := subscriber.Connect(stream.SubscriberConfig{
		NamespaceID:    ns,
		NodeID:         "sub",
		Mode:           stream.ClientReadOnly,
		CoordinatorURI: "memory://" + ns,
		ResumeFrom:     &parent,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Disconnect()

	if err := coordinator.Publish(stream.PublishedCommit{
		CommitHash:      child,
		ParentHash:      parent,
		TimestampMicros: 0,
	}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		pos := conn.Position()
		if pos != nil && *pos == child {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected position %s, got %v", child.Hex(), conn.Position())
}
