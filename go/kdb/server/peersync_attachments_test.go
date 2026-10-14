package server

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
	"github.com/limidus/kdb/go/kdb/peersync"
	"github.com/limidus/kdb/go/kdb/transport/core"
	"github.com/limidus/kdb/go/kdb/transport/tcp"
	"github.com/limidus/kdb/go/kdb/wire"
)

// Attachments cross peer sync both ways: the puller fetches the blobs a page attaches (verified
// chunk by chunk) before storing it, the pusher uploads them before its REF_UPDATE, and a merge of
// two diverged sides keeps every attachment either side made.
func TestV2SyncCarriesAttachmentBlobs(t *testing.T) {
	const ns = "app/data"
	a := newNamespaceSetRuntimes(t, ns)[ns]
	b := newNamespaceSetRuntimes(t, ns)[ns]
	x, y := mustRandomUUID(t), mustRandomUUID(t)
	if _, err := a.Upsert(ns, x, `{"on":"a"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("attachment-bytes!"), 200_000)        // 3.4 MB: four chunks
	bigB := bytes.Repeat([]byte("pulled, chunk by chunk "), 400_000) // 9.2 MB: more than one BLOB_FETCH page
	if _, _, err := a.Runtime.PutAttachment(ns, x, bytes.NewReader(big), embed.PutAttachmentOptions{Name: "big.bin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Upsert(ns, y, `{"on":"b"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Runtime.PutAttachment(ns, y, bytes.NewReader(bigB), embed.PutAttachmentOptions{Name: "b.bin"}); err != nil {
		t.Fatal(err)
	}

	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	sync := func() {
		t.Helper()
		if _, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
			NodeID: a.NodeID.String() + "-a", PeerURI: "tcp://" + ln.Addr().String(),
			Namespaces: []string{ns}, Local: a.PeerNamespaces(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	sync()
	ha, _ := a.dag.Head()
	hb, _ := b.dag.Head()
	if ha != hb {
		t.Fatal("heads differ after sync")
	}
	read := func(rt *KdbServerRuntime, doc string, name string) []byte {
		t.Helper()
		r, _, err := rt.Runtime.GetAttachment(ns, mustUUIDString(t, doc), embed.AttachmentByName(name), ha)
		if err != nil {
			t.Fatalf("%s/%s: %v", doc, name, err)
		}
		defer r.Close()
		out, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, rt := range []*KdbServerRuntime{a, b} {
		if !bytes.Equal(read(rt, x.String(), "big.bin"), big) || !bytes.Equal(read(rt, y.String(), "b.bin"), bigB) {
			t.Fatal("an attachment did not arrive whole")
		}
	}

	// Pushing a blob the host already holds sends it once: re-attaching the same bytes elsewhere
	// is answered by Expect. Then both sides change different attachments of x and merge.
	if _, _, err := a.Runtime.PutAttachment(ns, x, bytes.NewReader(big), embed.PutAttachmentOptions{Name: "copy.bin"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Runtime.PutAttachment(ns, x, bytes.NewReader([]byte("b's note")), embed.PutAttachmentOptions{Name: "note.txt"}); err != nil {
		t.Fatal(err)
	}
	sync()
	ha, _ = a.dag.Head()
	hb, _ = b.dag.Head()
	if ha != hb {
		t.Fatal("heads differ after the merge sync")
	}
	for _, rt := range []*KdbServerRuntime{a, b} {
		if !bytes.Equal(read(rt, x.String(), "copy.bin"), big) || string(read(rt, x.String(), "note.txt")) != "b's note" {
			t.Fatal("the merge lost an attachment")
		}
	}
}

func mustUUIDString(t *testing.T, s string) codec.UUID {
	t.Helper()
	id, err := codec.ParseUUID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// A peer of an older build cannot decode a commit that carries attachments: it is refused them with
// a typed error naming the upgrade, in either direction, and nothing is half-synced.
func TestV2RefusesAttachmentsToAPeerWithoutThem(t *testing.T) {
	const ns = "app/data"
	withoutAttachments := func(caps []string) []string {
		var out []string
		for _, c := range caps {
			if c != wire.SyncCapAttachments {
				out = append(out, c)
			}
		}
		return out
	}
	newCaps := peersync.ClientCapabilities
	defer func() { peersync.ClientCapabilities = newCaps }()

	a := newNamespaceSetRuntimes(t, ns)[ns]
	b := newNamespaceSetRuntimes(t, ns)[ns]
	x := mustRandomUUID(t)
	if _, err := b.Upsert(ns, x, `{"on":"b"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Runtime.PutAttachment(ns, x, bytes.NewReader([]byte("new")), embed.PutAttachmentOptions{Name: "n"}); err != nil {
		t.Fatal(err)
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// a pretends to be an older build: it does not offer the capability.
	peersync.ClientCapabilities = withoutAttachments(newCaps)
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: a.NodeID.String() + "-old", PeerURI: "tcp://" + ln.Addr().String(), Namespaces: []string{ns}, Local: a.PeerNamespaces(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Namespaces) != 1 || res.Namespaces[0].Err == nil || !strings.Contains(res.Namespaces[0].Err.Error(), "upgrade") {
		t.Fatalf("an old peer was not refused attachment commits: %+v", res.Namespaces)
	}
	if a.dag.HasAttachments() {
		t.Fatal("the old peer took in attachment commits")
	}
}

// A peer whose stored copy of a blob is damaged refuses it in BLOB_FETCH with a per-entry INTEGRITY
// error - not NOT_FOUND (which a puller skips past) and not an untyped failure - so the puller knows
// the blob exists but must come from another peer. Nothing is half-synced.
func TestV2BlobFetchRefusesCorruptBlobAsIntegrity(t *testing.T) {
	const ns = "app/data"
	a := newNamespaceSetRuntimes(t, ns)[ns]
	b := newNamespaceSetRuntimes(t, ns)[ns]
	y := mustRandomUUID(t)
	if _, err := b.Upsert(ns, y, `{"on":"b"}`, auth.Principal{}); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("damaged on b's disk "), 200_000) // 4 MB: four chunks
	_, info, err := b.Runtime.PutAttachment(ns, y, bytes.NewReader(data), embed.PutAttachmentOptions{Name: "b.bin"})
	if err != nil {
		t.Fatal(err)
	}
	h := info.ContentHash
	if err := b.Runtime.AttachmentStore(ns).CorruptForTest(h, 1<<20+3); err != nil {
		t.Fatal(err)
	}
	ln, err := ListenPeerSync("tcp://127.0.0.1:0?bind=true", b, ns)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	before, _ := a.dag.Head()
	res, err := peersync.SyncV2(wire.NewCodec(wire.EncodingJSON), tcp.NewTransport(core.DefaultConnectOptions()), peersync.V2ClientConfig{
		NodeID: a.NodeID.String() + "-a", PeerURI: "tcp://" + ln.Addr().String(),
		Namespaces: []string{ns}, Local: a.PeerNamespaces(),
	})
	if err == nil && len(res.Namespaces) == 1 {
		err = res.Namespaces[0].Err
	}
	var remote *peersync.RemoteError
	if !errors.As(err, &remote) || remote.Code != wire.ErrorCodeIntegrity || !strings.Contains(remote.Message, h.Hex()) {
		t.Fatalf("pulling a damaged blob: %v", err)
	}
	if after, _ := a.dag.Head(); after != before {
		t.Fatal("the commit landed without its blob")
	}
}
