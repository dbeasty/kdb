package server

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
)

// A paged SELECT's continuation token names the commit the first page read at, how many rows have
// been returned, and a digest of the statement and parameters it belongs to. Pinning the commit is
// what keeps the pages one consistent snapshot while writes continue; the digest stops a token
// from one query being replayed against another. It is not a secret and not signed: a forged
// offset only moves the caller around its own authorized read.
type pageToken struct {
	Commit string `json:"c"`
	Offset int    `json:"o"`
	Digest string `json:"h"`
}

func pageDigest(sqlText string, paramsJSON *string) string {
	h := sha256.New()
	h.Write([]byte(sqlText))
	h.Write([]byte{0})
	if paramsJSON != nil {
		h.Write([]byte(*paramsJSON))
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

func encodePageToken(commit codec.Hash, offset int, sqlText string, paramsJSON *string) string {
	b, _ := json.Marshal(pageToken{Commit: commit.Hex(), Offset: offset, Digest: pageDigest(sqlText, paramsJSON)})
	return base64.RawURLEncoding.EncodeToString(b)
}

var errBadContinuation = errors.New("invalid continuation token")

func decodePageToken(token, sqlText string, paramsJSON *string) (codec.Hash, int, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return codec.Hash{}, 0, errBadContinuation
	}
	var t pageToken
	if err := json.Unmarshal(raw, &t); err != nil || t.Offset < 0 {
		return codec.Hash{}, 0, errBadContinuation
	}
	if t.Digest != pageDigest(sqlText, paramsJSON) {
		return codec.Hash{}, 0, fmt.Errorf("%w: it belongs to a different statement or parameters", errBadContinuation)
	}
	commit, err := codec.HashFromHex(t.Commit)
	if err != nil {
		return codec.Hash{}, 0, errBadContinuation
	}
	return commit, t.Offset, nil
}
