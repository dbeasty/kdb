package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Keyed index snapshots.
//
// Format 1 (still read) was text, one line per event of the whole log ("P|doc|commit|key",
// "D|doc|commit"), which is to say every write the index ever took.
//
// Format 2 is binary and starts with keyedSnapshotMagic and a version byte. Its flags byte says
// which of two bodies follows:
//
//   - complete log (nothing was ever dropped): every event, in log order, so a restore can still
//     answer any historical read exactly as before. Each event is a kind byte (0 put, 1 delete),
//     the 16-byte document id, the 32-byte commit hash and, for a put, the uvarint length and
//     bytes of the key's order-preserving encoding (keyenc.go).
//   - head state (history was dropped): the buckets at head, as uvarint key count, then per
//     key its encoding (uvarint length + bytes), uvarint document count and the 16-byte ids;
//     then the commits of events the head skipped (uvarint count + 32-byte hashes). The state is
//     restored at the DAG head - LoadRegistry only keeps a snapshot whose manifest names the
//     DAG head - and reads at older commits fall back to the documents.
//
// Kotlin has its own index implementation and never reads these files.
var keyedSnapshotMagic = []byte{0x00, 'K', 'I', 'X'}

const (
	keyedSnapshotVersion = 2
	snapFlagComplete     = 1
)

func (l *eventLog) snapshotBytes() ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if head, err := l.dag.Head(); err == nil {
		l.advanceLocked(head)
	}
	if l.broken {
		return nil, ErrHistoryUnavailable
	}
	var b bytes.Buffer
	b.Write(keyedSnapshotMagic)
	b.WriteByte(keyedSnapshotVersion)
	var tmp [binary.MaxVarintLen64]byte
	uvarint := func(v uint64) { b.Write(tmp[:binary.PutUvarint(tmp[:], v)]) }
	uuid := func(id codec.UUID) {
		var u [16]byte
		binary.BigEndian.PutUint64(u[:8], uint64(id.MSB))
		binary.BigEndian.PutUint64(u[8:], uint64(id.LSB))
		b.Write(u[:])
	}
	if !l.dropped {
		b.WriteByte(snapFlagComplete)
		uvarint(uint64(len(l.events)))
		for i := range l.events {
			e := &l.events[i]
			if e.del {
				b.WriteByte(1)
			} else {
				b.WriteByte(0)
			}
			uuid(e.doc)
			b.Write(e.commit.Bytes[:])
			if !e.del {
				enc := l.keys.bytes(e.key)
				uvarint(uint64(len(enc)))
				b.Write(enc)
			}
		}
		return b.Bytes(), nil
	}
	b.WriteByte(0)
	st := l.head.st
	uvarint(uint64(st.live))
	for ord := 0; ord < l.keys.len(); ord++ {
		ids := st.ids(uint32(ord))
		if len(ids) == 0 {
			continue
		}
		enc := l.keys.bytes(uint32(ord))
		uvarint(uint64(len(enc)))
		b.Write(enc)
		uvarint(uint64(len(ids)))
		for _, id := range ids {
			uuid(id)
		}
	}
	pending := l.lin.PendingCommits()
	uvarint(uint64(len(pending)))
	for _, c := range pending {
		b.Write(c.Bytes[:])
	}
	return b.Bytes(), nil
}

var errKeyedSnapshot = errors.New("index snapshot: truncated or corrupt")

func (l *eventLog) restoreSnapshotBytes(data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clearLocked()
	if !bytes.HasPrefix(data, keyedSnapshotMagic) {
		return restoreSnapshotLines(strings.Split(string(data), "\n"), l.appendPutLocked, l.appendDeleteLocked)
	}
	r := data[len(keyedSnapshotMagic):]
	if len(r) < 2 {
		return errKeyedSnapshot
	}
	if r[0] != keyedSnapshotVersion {
		return fmt.Errorf("index snapshot: unsupported format version %d", r[0])
	}
	flags := r[1]
	r = r[2:]
	uvarint := func() (uint64, error) {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			return 0, errKeyedSnapshot
		}
		r = r[n:]
		return v, nil
	}
	take := func(n uint64) ([]byte, error) {
		if uint64(len(r)) < n {
			return nil, errKeyedSnapshot
		}
		out := r[:n]
		r = r[n:]
		return out, nil
	}
	uuid := func() (codec.UUID, error) {
		u, err := take(16)
		if err != nil {
			return codec.UUID{}, err
		}
		return codec.UUID{MSB: int64(binary.BigEndian.Uint64(u)), LSB: int64(binary.BigEndian.Uint64(u[8:]))}, nil
	}
	hash := func() (codec.Hash, error) {
		var h codec.Hash
		raw, err := take(32)
		if err != nil {
			return h, err
		}
		copy(h.Bytes[:], raw)
		return h, nil
	}
	if flags&snapFlagComplete != 0 {
		n, err := uvarint()
		if err != nil {
			return err
		}
		for i := uint64(0); i < n; i++ {
			kind, err := take(1)
			if err != nil {
				return err
			}
			doc, err := uuid()
			if err != nil {
				return err
			}
			commit, err := hash()
			if err != nil {
				return err
			}
			if kind[0] == 1 {
				l.appendDeleteLocked(doc, commit)
				continue
			}
			kl, err := uvarint()
			if err != nil {
				return err
			}
			enc, err := take(kl)
			if err != nil {
				return err
			}
			k, rest, err := decodeKeyEnc(enc)
			if err != nil || len(rest) != 0 {
				return errKeyedSnapshot
			}
			l.appendPutLocked(Entry{DocID: doc, Key: k, CommitHash: commit})
		}
		return nil
	}
	head, err := l.dag.Head()
	if err != nil {
		return err
	}
	st := newPostingState()
	nKeys, err := uvarint()
	if err != nil {
		return err
	}
	for i := uint64(0); i < nKeys; i++ {
		kl, err := uvarint()
		if err != nil {
			return err
		}
		enc, err := take(kl)
		if err != nil {
			return err
		}
		if _, rest, err := decodeKeyEnc(enc); err != nil || len(rest) != 0 {
			return errKeyedSnapshot
		}
		ord := l.keys.intern(enc)
		nIDs, err := uvarint()
		if err != nil {
			return err
		}
		for j := uint64(0); j < nIDs; j++ {
			id, err := uuid()
			if err != nil {
				return err
			}
			st.add(id, ord)
		}
	}
	nPending, err := uvarint()
	if err != nil {
		return err
	}
	for i := uint64(0); i < nPending; i++ {
		c, err := hash()
		if err != nil {
			return err
		}
		l.lin.MarkPending(c)
	}
	l.head = &headState{st: st}
	l.lin.Start(head)
	l.dropped = true
	return nil
}
