package attachstore

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
)

// CorruptForTest flips one stored byte of blob h at offset off, the way a bad disk would, so a test
// outside this package can check what a damaged copy turns into on the wire. Only a blob stored
// whole in its own segment (over SmallMaxBytes, not a delta) can be damaged this way.
func (s *Store) CorruptForTest(h codec.Hash, off int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return err
	}
	p, ok, err := s.getLocked(h)
	if err != nil {
		return err
	}
	if !ok || p.segment == "" || p.delta {
		return fmt.Errorf("attachstore: %s is not stored whole in a segment", h.Hex())
	}
	if off < 0 || off >= p.size {
		return fmt.Errorf("attachstore: offset %d outside %s's %d bytes", off, h.Hex(), p.size)
	}
	raw := make([]byte, p.size)
	if err := (segSource{shim: s.shim, name: p.segment, n: p.size}).readAt(raw, 0); err != nil {
		return err
	}
	raw[off] ^= 0xff
	if err := s.shim.DeleteSegment(p.segment); err != nil {
		return err
	}
	_, err = s.shim.AppendToSegment(p.segment, raw)
	return err
}
