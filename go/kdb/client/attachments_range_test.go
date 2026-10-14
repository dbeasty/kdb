package client_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/client"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/wire"
)

// rangeBytes is document's range-proof golden attachment: 4 MiB + 5 bytes, byte i = i mod 251.
func rangeBytes() []byte {
	b := make([]byte, 4<<20+5)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func readRange(t *testing.T, c *client.Client, ns, doc string, ref client.AttachmentRef, asOf string, off, length int64) ([]byte, *client.AttachmentReader, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := c.OpenAttachmentRange(ctx, ns, doc, ref, asOf, off, length)
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	return got, r, err
}

func TestAttachmentRangeReads(t *testing.T) {
	addr, _ := startTestServer(t)
	c := connectTestClient(t, addr)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const ns = "app/data"
	const mib = 1 << 20
	id, _ := codec.RandomUUID()
	doc := id.String()
	if _, err := c.PutJSON(ctx, ns, doc, []byte(`{"title":"ranges"}`)); err != nil {
		t.Fatal(err)
	}
	data := rangeBytes()
	size := int64(len(data))
	commit, _, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader(data), client.PutAttachmentOptions{Name: "r.bin"})
	if err != nil {
		t.Fatal(err)
	}
	ref := client.ByAttachmentName("r.bin")

	for _, tc := range []struct {
		name        string
		off, length int64
	}{
		{"start", 0, 10},
		{"mid-chunk", mib + 100, 50},
		{"across a chunk boundary", mib - 10, 20},
		{"across pages", 100, 4*mib + 7 - 100},
		{"whole", 0, -1},
		{"from a chunk boundary to the end", 3 * mib, -1},
		{"into the short last chunk", 4*mib - 3, -1},
		{"last byte", size - 1, 1},
		{"past the end is cut", size - 2, 100},
		{"empty mid-way", 2*mib + 1, 0},
		{"at the end", size, -1},
		{"at the end, with a length", size, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, r, err := readRange(t, c, ns, doc, ref, "", tc.off, tc.length)
			if err != nil {
				t.Fatal(err)
			}
			end := size
			if tc.length >= 0 {
				end = min(size, tc.off+tc.length)
			}
			if !bytes.Equal(got, data[tc.off:end]) {
				t.Fatalf("got %d bytes, want %d", len(got), end-tc.off)
			}
			if r.Superseded() || r.Commit() != commit || r.Info().Size != size {
				t.Fatalf("superseded %v commit %s info %+v", r.Superseded(), r.Commit(), r.Info())
			}
		})
	}

	_, err = c.OpenAttachmentRange(ctx, ns, doc, ref, "", size+1, 1)
	var rangeErr *client.AttachmentRangeError
	if !errors.As(err, &rangeErr) || rangeErr.Size != size || !errors.Is(err, client.ErrAttachmentRange) {
		t.Fatalf("past the end: %v", err)
	}
	if _, err := c.OpenAttachmentRange(ctx, ns, doc, ref, "", -1, 1); !errors.Is(err, client.ErrAttachmentRange) {
		t.Fatalf("negative offset: %v", err)
	}
	// The failed open did not leave a read pinned: the connection's 64 reads are all still free.
	for range 70 {
		if _, err := c.OpenAttachmentRange(ctx, ns, doc, ref, "", size+1, 1); !errors.Is(err, client.ErrAttachmentRange) {
			t.Fatalf("repeated past the end: %v", err)
		}
	}

	// ReadAttachmentAt, io.ReaderAt style.
	p := make([]byte, 10)
	if n, err := c.ReadAttachmentAt(ctx, ns, doc, ref, "", p, 2*mib-5); n != 10 || err != nil || !bytes.Equal(p, data[2*mib-5:2*mib+5]) {
		t.Fatalf("ReadAttachmentAt: %d %v", n, err)
	}
	if n, err := c.ReadAttachmentAt(ctx, ns, doc, ref, "", p, size-3); n != 3 || err != io.EOF || !bytes.Equal(p[:3], data[size-3:]) {
		t.Fatalf("ReadAttachmentAt at the tail: %d %v", n, err)
	}
	if n, err := c.ReadAttachmentAt(ctx, ns, doc, ref, "", p, size); n != 0 || err != io.EOF {
		t.Fatalf("ReadAttachmentAt at the end: %d %v", n, err)
	}
	if _, err := c.ReadAttachmentAt(ctx, ns, doc, ref, "", p, size+10); !errors.Is(err, client.ErrAttachmentRange) {
		t.Fatalf("ReadAttachmentAt past the end: %v", err)
	}

	// A ranged read pins its version and reports a newer one at the end of its range, though that
	// is not the attachment's end.
	r, err := c.OpenAttachmentRange(ctx, ns, doc, ref, "", mib-1, 2*mib)
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 2)
	if _, err := io.ReadFull(r, head); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader([]byte("replaced")), client.PutAttachmentOptions{Name: "r.bin"}); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(append(head, rest...), data[mib-1:3*mib-1]) {
		t.Fatalf("pinned range: %d bytes, %v", len(rest), err)
	}
	if !r.Superseded() {
		t.Fatal("the range read did not report the newer version")
	}
	// The old version stays readable by commit.
	if got, _, err := readRange(t, c, ns, doc, ref, commit, size-5, -1); err != nil || !bytes.Equal(got, data[size-5:]) {
		t.Fatalf("as of the old commit: %v", err)
	}

	// A zero-byte attachment: its one empty chunk is proved.
	if _, _, err := c.PutAttachment(ctx, ns, doc, bytes.NewReader(nil), client.PutAttachmentOptions{Name: "empty"}); err != nil {
		t.Fatal(err)
	}
	for _, length := range []int64{-1, 0, 10} {
		if got, r, err := readRange(t, c, ns, doc, client.ByAttachmentName("empty"), "", 0, length); err != nil || len(got) != 0 || r.Info().Size != 0 {
			t.Fatalf("empty attachment, length %d: %d bytes %v", length, len(got), err)
		}
	}
	if _, err := c.OpenAttachmentRange(ctx, ns, doc, client.ByAttachmentName("empty"), "", 1, 1); !errors.Is(err, client.ErrAttachmentRange) {
		t.Fatalf("empty attachment past the end: %v", err)
	}
}

// fakeProofServer serves one attachment over ATTACHMENT_GET with proofs, flipping a byte of
// chunk tamper (-1: none) after the proofs were computed, and with the server's offered features.
func fakeProofServer(t *testing.T, data []byte, tamper int, features []string) string {
	t.Helper()
	const chunk = document.AttachmentChunkBytes
	var leaves []codec.Hash
	for off := 0; off < len(data) || off == 0; off += chunk {
		leaves = append(leaves, document.ChunkLeafHash(data[off:min(off+chunk, len(data))]))
		if off+chunk >= len(data) {
			break
		}
	}
	tree := document.NewMerkleTree(leaves)
	served := bytes.Clone(data)
	if tamper >= 0 {
		served[tamper*chunk+3] ^= 0xff
	}
	info := wire.AttachmentInfo{ID: codec.UUID{MSB: 1, LSB: 2}, Name: "a", Hash: document.AttachmentContentHash(data), Size: int64(len(data))}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				cd := wire.NewCodec(wire.EncodingJSON)
				for {
					var prefix [4]byte
					if _, err := io.ReadFull(conn, prefix[:]); err != nil {
						return
					}
					frame := make([]byte, binary.LittleEndian.Uint32(prefix[:]))
					copy(frame, prefix[:])
					if _, err := io.ReadFull(conn, frame[4:]); err != nil {
						return
					}
					msg, err := cd.Decode(frame)
					if err != nil {
						return
					}
					var reply wire.Message
					switch m := msg.(type) {
					case wire.HandshakeMessage:
						reply = wire.HandshakeAckMessage{H: m.H, Response: wire.HandshakeAckPayload{Accepted: true, NegotiatedEncoding: wire.EncodingJSON, ProtocolVersion: 1, WireFeatures: features}}
					case wire.AttachmentGetMessage:
						out := wire.AttachmentDataMessage{H: wire.Header{MessageType: wire.MsgAttachmentData, ProtocolVersion: 1, CorrelationID: m.H.CorrelationID},
							ReadID: "r", Info: info, CommitHex: "00", Tree: tree.Root(), Scheme: 1}
						if !m.Close {
							off := m.Offset / chunk * chunk
							end := min(int64(len(data)), max((m.Offset+m.MaxBytes+chunk-1)/chunk*chunk, off+chunk))
							for i := off / chunk; i < int64(len(leaves)) && (i*chunk < end || len(data) == 0); i++ {
								out.Chunks = append(out.Chunks, wire.BlobChunkProof{Leaf: i, Path: tree.Proof(int(i))})
							}
							out.Offset, out.Data, out.EOF = off, served[off:end], end == int64(len(data))
						}
						reply = out
					default:
						return
					}
					b, err := cd.Encode(reply)
					if err != nil {
						return
					}
					if _, err := conn.Write(b); err != nil {
						return
					}
				}
			}()
		}
	}()
	return "tcp://" + ln.Addr().String()
}

func TestAttachmentRangeRejectsATamperedChunk(t *testing.T) {
	data := rangeBytes()[:2<<20+10] // three chunks
	addr := fakeProofServer(t, data, 1, wire.SupportedWireFeatures)
	c := connectTestClient(t, addr)
	doc := "6f1c2c4e-0000-4000-8000-000000000001"
	ref := client.ByAttachmentName("a")

	// Chunk 0 is sound, so a range inside it reads.
	if got, _, err := readRange(t, c, "app/data", doc, ref, "", 5, 100); err != nil || !bytes.Equal(got, data[5:105]) {
		t.Fatalf("a sound chunk: %v", err)
	}
	// A range touching chunk 1 fails before handing out anything, even bytes of chunk 0 or bytes of
	// chunk 1 the flip did not touch.
	for _, rg := range [][2]int64{{1<<20 + 100, 10}, {1<<20 - 10, 20}, {0, -1}} {
		if _, err := c.OpenAttachmentRange(context.Background(), "app/data", doc, ref, "", rg[0], rg[1]); !errors.Is(err, client.ErrAttachmentCorrupt) {
			t.Fatalf("range %v over a tampered chunk: %v", rg, err)
		}
	}
	// A page past the bad chunk, opened later in a read, fails the Read too.
	r, err := c.OpenAttachmentRange(context.Background(), "app/data", doc, ref, "", 0, 4<<20)
	if err == nil {
		_, err = io.ReadAll(r)
	}
	if !errors.Is(err, client.ErrAttachmentCorrupt) {
		t.Fatalf("whole range: %v", err)
	}
}

func TestAttachmentRangeNeedsProofsFeature(t *testing.T) {
	data := []byte("hello")
	addr := fakeProofServer(t, data, -1, []string{wire.WireFeatureBinary, wire.WireFeatureAttachments})
	c := connectTestClient(t, addr)
	var unsupported *client.UnsupportedError
	if _, err := c.OpenAttachmentRange(context.Background(), "app/data", "6f1c2c4e-0000-4000-8000-000000000001", client.ByAttachmentName("a"), "", 0, -1); !errors.As(err, &unsupported) {
		t.Fatalf("without attachment-proofs: %v", err)
	}
}
