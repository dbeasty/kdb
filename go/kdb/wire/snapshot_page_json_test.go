package wire

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// The hand-written snapshot page encoding is encoding/json's, byte for byte, whatever the bodies
// hold - and decodes back to the same page.
func TestSnapshotPageJSONMatchesEncodingJSON(t *testing.T) {
	commit := document.Commit{Hash: codec.Hash{}, Message: "root"}
	if h, err := document.ComputeCommitHash(commit); err == nil {
		commit.Hash = h
	}
	awkward := []string{
		``, `{"a":1}`, `{"html":"<b>&amp;</b>"}`, "ctl\x00\x01\x1f\b\f\n\r\t\x7f", `quote " back \ slash`,
		"bad utf8 \xff\xfe end", "line\u2028para\u2029", "ünïcødé 漢字 🙂", "trailing bad \xc3",
	}
	pages := []SnapshotPageMessage{
		{Namespace: "app/x", Commit: commit, Done: true},
		{Namespace: "app/<x>", Commit: commit, Docs: []SnapshotDoc{}, Total: 0, Done: true},
		{Namespace: "app/x", Commit: commit, Next: "n", Total: 12345, AttachmentTables: []byte{0, 1, 2, 250}},
	}
	var docs []SnapshotDoc
	for i, s := range awkward {
		docs = append(docs, SnapshotDoc{DocID: s, Body: awkward[len(awkward)-1-i]})
	}
	pages = append(pages, SnapshotPageMessage{H: Header{MessageType: MsgSnapshotPage, ProtocolVersion: KdbWireProtocolVersion, CorrelationID: 7}, Namespace: "app/x", Commit: commit, Docs: docs, Next: "\u2028", Total: -1})
	for i, p := range pages {
		env, err := messageToEnvelope(p)
		if err != nil {
			t.Fatal(err)
		}
		var want bytes.Buffer
		if err := json.NewEncoder(&want).Encode(env); err != nil {
			t.Fatal(err)
		}
		got, err := appendSnapshotPageJSON(nil, p)
		if err != nil {
			t.Fatal(err)
		}
		if w := bytes.TrimSuffix(want.Bytes(), []byte("\n")); !bytes.Equal(got, w) {
			t.Fatalf("page %d:\n got %s\nwant %s", i, got, w)
		}
	}
	c := NewCodec(EncodingJSON)
	frame, err := c.Encode(pages[3])
	if err != nil {
		t.Fatal(err)
	}
	back, err := c.Decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	if got := back.(SnapshotPageMessage); len(got.Docs) != len(docs) || got.Docs[2] != docs[2] || got.Next != pages[3].Next {
		t.Fatalf("round trip: %+v", got)
	}
}
