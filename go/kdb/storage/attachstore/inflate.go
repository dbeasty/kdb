package attachstore

import (
	"errors"
	"io"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Inflating turns a version stored as a delta back into the original file, whole - what
// Config.DisableDeltas asks for of deltas stored before it was set. Like a delta job it runs in
// steps of bounded work (InflateStep), one job per store, holding no pins or locks between steps:
// each step reads the next bytes through the delta (every chunk checked against the version's
// leaves, as any read is) and appends them to a new segment. Once the copy is whole and sealed, a
// higher-generation pointer names it and the delta segment goes (or waits for its readers). A crash
// mid-job leaves an unnamed segment, which Collect retires, and the job starts over.

type inflateJob struct {
	h        codec.Hash
	deltaSeg string // h's delta segment when the job started; another means h changed
	seg      string // the whole copy being written
	pos      int64
}

// Deltas lists the blobs stored as deltas.
func (s *Store) Deltas() ([]codec.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.loadLocked(); err != nil {
		return nil, err
	}
	var out []codec.Hash
	err := s.idx.each(func(h codec.Hash, e indexEntry) error {
		if e.delta {
			out = append(out, h)
		}
		return nil
	})
	return out, err
}

// InflateStep copies about budget bytes (zero means Config.DeltaStepBytes) of delta-stored h into
// a whole segment, and reports whether h is now stored whole and the bytes it used. A blob that is
// not a delta is done at once. done=false means call again with the same h: the job carries over.
func (s *Store) InflateStep(h codec.Hash, budget int64) (bool, int64, error) {
	if budget <= 0 {
		budget = s.cfg.DeltaStepBytes
	}
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	s.mu.Lock()
	if err := s.loadLocked(); err != nil {
		s.mu.Unlock()
		return false, 0, err
	}
	p, ok, err := s.getLocked(h)
	if err != nil {
		s.mu.Unlock()
		return false, 0, err
	}
	if job := s.inflate; job != nil && (job.h != h || !ok || p.segment != job.deltaSeg) {
		s.dropInflateLocked()
	}
	if !ok || !p.delta {
		s.mu.Unlock()
		return true, 0, nil
	}
	job := s.inflate
	if job == nil {
		seg, err := s.newSegmentName()
		if err != nil {
			s.mu.Unlock()
			return false, 0, err
		}
		job = &inflateJob{h: h, deltaSeg: p.segment, seg: seg}
		s.inflate = job
		s.uploads[seg] = struct{}{}
	}
	s.mu.Unlock()

	r, err := s.Open(h)
	if err != nil {
		s.abandonInflate(job)
		return false, 0, err
	}
	defer r.Close()
	buf := make([]byte, chunk)
	used := int64(0)
	for job.pos < r.Size() && used < budget {
		n, err := r.ReadAt(buf[:min(int64(chunk), r.Size()-job.pos)], job.pos)
		if err != nil && !(errors.Is(err, io.EOF) && job.pos+int64(n) == r.Size()) {
			s.abandonInflate(job)
			return false, used, err
		}
		if _, err := s.shim.AppendToSegment(job.seg, buf[:n]); err != nil {
			s.abandonInflate(job)
			return false, used, err
		}
		job.pos += int64(n)
		used += int64(n)
	}
	if job.pos < r.Size() {
		return false, used, nil
	}
	if err := s.shim.FlushSegment(job.seg); err != nil {
		s.abandonInflate(job)
		return false, used, err
	}
	if err := s.shim.SealSegment(job.seg); err != nil {
		s.abandonInflate(job)
		return false, used, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok, err := s.getLocked(h)
	if err != nil {
		s.dropInflateLocked()
		return false, used, err
	}
	if s.inflate != job || !ok || !cur.delta || cur.segment != job.deltaSeg {
		s.dropInflateLocked() // changed under us; leave it be
		return !ok || !cur.delta, used, nil
	}
	np, err := s.writePointerLocked(h, pointer{segment: job.seg, leaves: cur.leaves, size: cur.size})
	if err != nil {
		s.dropInflateLocked()
		return false, used, err
	}
	if err := s.replaceLocked(h, np); err != nil {
		return false, used, err
	}
	delete(s.uploads, job.seg)
	s.inflate = nil
	s.deleteOrDeferLocked(cur.name)
	s.deleteOrDeferLocked(cur.segment)
	return true, used, nil
}

func (s *Store) abandonInflate(job *inflateJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflate == job {
		s.dropInflateLocked()
	}
}

// dropInflateLocked abandons the inflate job, deleting its partial copy. Must hold mu (and
// deltaMu, or know no step runs).
func (s *Store) dropInflateLocked() {
	job := s.inflate
	if job == nil {
		return
	}
	s.inflate = nil
	delete(s.uploads, job.seg)
	_ = s.shim.DeleteSegment(job.seg)
}
