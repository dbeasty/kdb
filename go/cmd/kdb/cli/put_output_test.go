package cli_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/limidus/kdb/go/cmd/kdb/cli"
	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/embed"
)

func TestFormatPutStdout(t *testing.T) {
	docID, err := codec.UUIDFromString("00000000-0000-0000-0000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	commit, err := codec.HashFromHex("ab" + strings.Repeat("c", 62))
	if err != nil {
		t.Fatal(err)
	}
	line, err := cli.FormatPutStdoutForTest(embed.PutResult{DocID: docID, Commit: commit})
	if err != nil {
		t.Fatal(err)
	}
	var parsed struct {
		DocID  string `json:"docId"`
		Short  string `json:"docIdShort"`
		Commit string `json:"commit"`
	}
	if err := json.Unmarshal([]byte(line), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.DocID != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("docId %q", parsed.DocID)
	}
	if parsed.Short != "00000000" {
		t.Fatalf("docIdShort %q", parsed.Short)
	}
	if len(parsed.Commit) != 64 {
		t.Fatalf("commit %q", parsed.Commit)
	}
}
