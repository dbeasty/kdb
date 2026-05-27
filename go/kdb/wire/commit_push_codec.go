package wire

import (
	"encoding/binary"
	"fmt"

	"github.com/limidus/kdb/go/kdb/document"
)

// EncodeCommits serializes commits for CommitPush payloads.
func EncodeCommits(commits []document.Commit) ([]byte, error) {
	payloads := make([][]byte, len(commits))
	total := 4
	for i, c := range commits {
		b, err := c.ToPayloadBytes()
		if err != nil {
			return nil, err
		}
		payloads[i] = b
		total += 4 + len(b)
	}
	out := make([]byte, total)
	o := 0
	binary.LittleEndian.PutUint32(out[o:], uint32(len(commits)))
	o += 4
	for _, p := range payloads {
		binary.LittleEndian.PutUint32(out[o:], uint32(len(p)))
		o += 4
		copy(out[o:], p)
		o += len(p)
	}
	return out, nil
}

// DecodeCommits deserializes commits from CommitPush payload bytes.
func DecodeCommits(bytes []byte) ([]document.Commit, error) {
	if len(bytes) < 4 {
		return nil, nil
	}
	o := 0
	count := int(binary.LittleEndian.Uint32(bytes[o:]))
	o += 4
	result := make([]document.Commit, 0, count)
	for i := 0; i < count; i++ {
		if o+4 > len(bytes) {
			return nil, fmt.Errorf("truncated commit push payload")
		}
		ln := int(binary.LittleEndian.Uint32(bytes[o:]))
		o += 4
		if o+ln > len(bytes) {
			return nil, fmt.Errorf("truncated commit bytes")
		}
		c, err := document.FromPayloadBytes(bytes[o : o+ln])
		if err != nil {
			return nil, err
		}
		result = append(result, c)
		o += ln
	}
	return result, nil
}
