package server

import (
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
)

func TestPageTokenIsBoundToItsStatement(t *testing.T) {
	commit, _ := codec.HashFromHex("11" + "11111111111111111111111111111111111111111111111111111111111111"[:62])
	params := `["a"]`
	tok := encodePageToken(commit, 80, nil, "SELECT x FROM t", &params)

	gotCommit, offset, after, err := decodePageToken(tok, "SELECT x FROM t", &params)
	if err != nil || gotCommit != commit || offset != 80 || after != nil {
		t.Fatalf("round trip: %v %d %v", gotCommit, offset, err)
	}
	other := `["b"]`
	for name, c := range map[string]struct {
		sql    string
		params *string
	}{
		"another statement": {"SELECT y FROM t", &params},
		"other parameters":  {"SELECT x FROM t", &other},
		"no parameters":     {"SELECT x FROM t", nil},
	} {
		if _, _, _, err := decodePageToken(tok, c.sql, c.params); !errors.Is(err, errBadContinuation) {
			t.Errorf("%s: token accepted (%v)", name, err)
		}
	}
	if _, _, _, err := decodePageToken("not-a-token!", "SELECT x FROM t", &params); !errors.Is(err, errBadContinuation) {
		t.Errorf("garbage accepted: %v", err)
	}
}

// A keyset token carries the document its page ended on, and a token without one - from a build
// before keyset paging - still decodes, as an offset continuation.
func TestPageTokenCarriesItsKey(t *testing.T) {
	commit, _ := codec.HashFromHex("22" + "22222222222222222222222222222222222222222222222222222222222222"[:62])
	key, _ := codec.RandomUUID()
	tok := encodePageToken(commit, 40, &key, "SELECT x FROM t", nil)
	_, offset, after, err := decodePageToken(tok, "SELECT x FROM t", nil)
	if err != nil || offset != 40 || after == nil || *after != key {
		t.Fatalf("round trip: offset=%d after=%v err=%v", offset, after, err)
	}
}
