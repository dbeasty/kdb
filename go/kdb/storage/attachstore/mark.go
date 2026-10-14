package attachstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

// The mark is a small durable note the store keeps for its owner: the commits whose history delta
// compaction has already gone through (the branch and tag heads of its last pass), so the next pass
// - after a restart too - looks only at what was committed since. It lives in its own segment,
// <ns>/attachment/m-<generation>; a new mark is written under the next generation before the old one
// goes, so a crash leaves one or the other. A torn or unreadable mark reads as none: compaction then
// looks at all of history once more, which is slower, never wrong.

const markMagic = "KDBATM1"

func (s *Store) markName(gen int) string { return fmt.Sprintf("%sm-%06d", s.prefix, gen) }

// readMark parses the mark segment name. Must hold mu.
func (s *Store) readMark(name string) (int, []codec.Hash, bool) {
	gen, err := strconv.Atoi(strings.TrimPrefix(name, s.prefix+"m-"))
	if err != nil {
		return 0, nil, false
	}
	head, err := s.shim.ReadFromSegment(name, 0, len(markMagic)+4)
	if err != nil || len(head) < len(markMagic)+4 || string(head[:len(markMagic)]) != markMagic {
		return 0, nil, false
	}
	n := int(binary.BigEndian.Uint32(head[len(markMagic):]))
	if n < 0 || n > 1<<20 {
		return 0, nil, false
	}
	total := len(markMagic) + 4 + n*32 + 4
	raw, err := s.shim.ReadFromSegment(name, 0, total)
	if err != nil || len(raw) != total {
		return 0, nil, false
	}
	body, crc := raw[:total-4], raw[total-4:]
	if sum := sha256.Sum256(body); !bytes.Equal(sum[:4], crc) {
		return 0, nil, false
	}
	out := make([]codec.Hash, n)
	for i := range out {
		copy(out[i].Bytes[:], body[len(markMagic)+4+i*32:])
	}
	return gen, out, true
}

// Mark returns the commits SetMark last recorded, or none.
func (s *Store) Mark() ([]codec.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	return append([]codec.Hash(nil), s.mark...), nil
}

// SetMark durably records commits as the mark, replacing the previous one.
func (s *Store) SetMark(commits []codec.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	b := make([]byte, 0, len(markMagic)+4+len(commits)*32+4)
	b = append(b, markMagic...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(commits)))
	for _, h := range commits {
		b = append(b, h.Bytes[:]...)
	}
	sum := sha256.Sum256(b)
	b = append(b, sum[:4]...)
	gen := s.markGen + 1
	name := s.markName(gen)
	if err := s.writeSegment(name, b); err != nil {
		_ = s.shim.DeleteSegment(name)
		return err
	}
	if s.markSeg != "" {
		_ = s.shim.DeleteSegment(s.markSeg) // a leftover is retired by Collect
	}
	s.markGen, s.markSeg, s.mark = gen, name, append([]codec.Hash(nil), commits...)
	return nil
}
