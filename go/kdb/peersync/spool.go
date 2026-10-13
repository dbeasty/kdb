package peersync

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/limidus/kdb/go/kdb/wire"
)

// pageSpool stages a paged transfer of documents - a snapshot's pages, a pushed graft's - on disk
// rather than in memory, so a session that has to take in a whole namespace before it can act on
// any of it holds one page at a time, not the namespace.
//
// It is Cassandra's streaming session in miniature: the receiving side of a stream writes what
// arrives to disk as it arrives, and only the step that has to be atomic (here: applying the
// snapshot under the write gate, or verifying a pushed graft's tree) reads it back, in bounded
// batches.
//
// The file is unlinked as soon as it is created where the platform allows it, so a process that
// dies mid-transfer leaves nothing behind to clean up; where it does not (Windows), close removes
// it.
type pageSpool struct {
	f       *os.File
	w       *bufio.Writer
	path    string // non-empty while the file still has a name to remove at close
	size    int64  // bytes written
	docs    int    // records written
	bodyLen int64  // body bytes written
}

// spoolBufferBytes is the write and read buffer of a spool: large enough that a page of small
// documents is a handful of syscalls, small enough to be noise next to the page itself.
const spoolBufferBytes = 256 << 10

// newPageSpool creates an empty spool under dir, or the system temporary directory when dir is
// empty.
func newPageSpool(dir string) (*pageSpool, error) {
	if dir == "" {
		dir = os.TempDir()
	} else if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("peer sync: staging directory: %w", err)
	}
	f, err := os.CreateTemp(dir, "kdb-peersync-*.spool")
	if err != nil {
		return nil, fmt.Errorf("peer sync: staging file: %w", err)
	}
	s := &pageSpool{f: f, w: bufio.NewWriterSize(f, spoolBufferBytes), path: f.Name()}
	if os.Remove(f.Name()) == nil {
		s.path = ""
	}
	return s, nil
}

// add appends one document.
func (s *pageSpool) add(docID, body string) error {
	var hdr [2 * binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(docID)))
	n += binary.PutUvarint(hdr[n:], uint64(len(body)))
	if _, err := s.w.Write(hdr[:n]); err != nil {
		return s.writeErr(err)
	}
	if _, err := s.w.WriteString(docID); err != nil {
		return s.writeErr(err)
	}
	if _, err := s.w.WriteString(body); err != nil {
		return s.writeErr(err)
	}
	s.size += int64(n + len(docID) + len(body))
	s.docs++
	s.bodyLen += int64(len(body))
	return nil
}

func (s *pageSpool) writeErr(err error) error {
	return fmt.Errorf("peer sync: writing the staging file: %w", err)
}

// each reads the spool back in order, in batches of up to maxBytes of bodies (at least one
// document), stopping after limit documents when limit >= 0. A batch is only valid during the
// call that receives it: its slice is reused.
func (s *pageSpool) each(maxBytes, limit int, fn func(docs []wire.SnapshotDoc) error) error {
	if err := s.w.Flush(); err != nil {
		return s.writeErr(err)
	}
	if maxBytes <= 0 {
		maxBytes = DefaultPageBytes
	}
	r := bufio.NewReaderSize(io.NewSectionReader(s.f, 0, s.size), spoolBufferBytes)
	var batch []wire.SnapshotDoc
	size, read := 0, 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := fn(batch)
		clear(batch)
		batch, size = batch[:0], 0
		return err
	}
	for limit < 0 || read < limit {
		idLen, err := binary.ReadUvarint(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return s.readErr(err)
		}
		bodyLen, err := binary.ReadUvarint(r)
		if err != nil {
			return s.readErr(err)
		}
		buf := make([]byte, idLen+bodyLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			return s.readErr(err)
		}
		if len(batch) > 0 && size+int(bodyLen) > maxBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, wire.SnapshotDoc{DocID: string(buf[:idLen]), Body: string(buf[idLen:])})
		size += int(bodyLen)
		read++
	}
	return flush()
}

func (s *pageSpool) readErr(err error) error {
	return fmt.Errorf("peer sync: reading the staging file back: %w", err)
}

// close releases the spool and its file.
func (s *pageSpool) close() {
	if s == nil || s.f == nil {
		return
	}
	_ = s.f.Close()
	if s.path != "" {
		_ = os.Remove(s.path)
	}
	s.f = nil
}
