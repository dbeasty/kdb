package attachstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// A delta job re-stores one replaced version as a delta against its successor, in steps of bounded
// work (DeltaStep), so a maintenance pass over a multi-GB version ends in bounded time and the
// next pass goes on where it stopped. Between steps the job holds no pins and no locks - only its
// base index (bounded by Config.DeltaIndexEntries) and its progress; each step opens both versions
// afresh. The phases:
//
//  1. index the base's unmatched chunks (delta.go);
//  2. encode the target into the delta segment, frame by frame, the frame index going to a
//     second segment until the end, when it is copied after the frames with the footer;
//  3. verify: read the whole delta back through the base and check every chunk against the old
//     version's leaves - the old copy is deleted on the strength of this;
//  4. swap: a higher-generation pointer names the delta, then the whole copy goes (or waits for
//     its readers). A crash before the swap leaves the whole copy and some unnamed segments, which
//     Collect retires; the job starts over.
//
// One job runs per store. A step for another pair abandons it; so does the version changing under
// it, or GC removing either version.

// DeltaOutcome is what one DeltaStep did.
type DeltaOutcome int

const (
	// DeltaSkipped: old stays as it is - not eligible, or its delta would not be worth storing.
	DeltaSkipped DeltaOutcome = iota
	// DeltaStored: old is now a delta against base.
	DeltaStored
	// DeltaPending: the budget ran out; call again with the same pair to go on.
	DeltaPending
)

const (
	jobIndex = iota
	jobEncode
	jobVerify
)

type deltaJob struct {
	old, base codec.Hash
	oldSeg    string // old's whole segment when the job started; another means old changed
	seg       string // the delta
	framesSeg string // the frame index, until it is copied into seg
	started   bool
	phase     int
	ixChunk   int64
	vPos      int64
	enc       encoder
}

// StoreAsDelta re-stores old as a delta against base (the version that replaced it) when old is a
// whole standalone blob, the chain stays within DeltaMaxDepth, and the delta is under half old's
// size. It reports whether it did. It runs DeltaStep until the job ends, however long that takes;
// maintenance uses DeltaStep with a budget instead. Readers already open on old keep reading the
// whole copy, which is deleted when the last of them closes.
func (s *Store) StoreAsDelta(old, base codec.Hash) (bool, error) {
	for {
		out, _, err := s.DeltaStep(old, base, 0)
		if err != nil || out != DeltaPending {
			return out == DeltaStored, err
		}
	}
}

// DeltaStep does about budget bytes of work (bytes indexed, encoded or verified; zero means
// Config.DeltaStepBytes) towards re-storing old as a delta against base, and reports the outcome
// and the bytes it used. DeltaPending means call again with the same pair: the job carries over.
func (s *Store) DeltaStep(old, base codec.Hash, budget int64) (DeltaOutcome, int64, error) {
	if budget <= 0 {
		budget = s.cfg.DeltaStepBytes
	}
	if old == base || old == emptyHash || base == emptyHash || s.cfg.DisableDeltas {
		return DeltaSkipped, 0, nil
	}
	s.deltaMu.Lock()
	defer s.deltaMu.Unlock()
	s.mu.Lock()
	if err := s.loadLocked(); err != nil {
		s.mu.Unlock()
		return DeltaSkipped, 0, err
	}
	p, ok, err := s.getLocked(old)
	if err != nil {
		s.mu.Unlock()
		return DeltaSkipped, 0, err
	}
	baseChain, baseLarge, err := s.resolveLocked(base)
	_, declined := s.declined.get([2]codec.Hash{old, base})
	limit := s.cfg.DeltaMaxBytes
	if job := s.job; job != nil && (job.old != old || job.base != base || !ok || p.segment != job.oldSeg || !baseLarge) {
		s.dropJobLocked()
	}
	if err != nil || !ok || !baseLarge || p.delta || p.inline != nil || declined ||
		(limit > 0 && (p.size > limit || baseChain[0].p.size > limit)) || len(baseChain) > s.cfg.DeltaMaxDepth {
		s.mu.Unlock()
		return DeltaSkipped, 0, err
	}
	job := s.job
	s.mu.Unlock()

	oldR, err := s.Open(old)
	if err != nil {
		return DeltaSkipped, 0, err
	}
	defer oldR.Close()
	baseR, err := s.Open(base)
	if err != nil {
		return DeltaSkipped, 0, err
	}
	defer baseR.Close()
	baseLeaves := baseChain[0].p.leaves
	if baseChain[0].p.inline != nil {
		baseLeaves = ""
	}

	if job == nil {
		if job, err = s.startJob(old, base, p); err != nil {
			return DeltaSkipped, 0, err
		}
	}
	used, done, err := s.runJob(job, oldR, baseR, p, baseLeaves, budget)
	if err == nil && !done {
		return DeltaPending, used, nil
	}
	if err == nil {
		did, err := s.finishJob(job)
		if did {
			return DeltaStored, used, err
		}
		return DeltaSkipped, used, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job == job {
		s.dropJobLocked()
	}
	if errors.Is(err, errDeltaTooBig) || errors.Is(err, errDeltaMismatch) {
		if errors.Is(err, errDeltaMismatch) {
			slog.Error("kdb: attachment delta did not reproduce its version; kept whole", "old", old.Hex(), "base", base.Hex(), "err", err)
		}
		s.declined.put([2]codec.Hash{old, base}, struct{}{})
		return DeltaSkipped, used, nil
	}
	return DeltaSkipped, used, err
}

// errDeltaMismatch is a delta that read back different bytes than the version it encodes: a bug,
// caught before the whole copy goes.
var errDeltaMismatch = errors.New("attachstore: delta does not reproduce its version")

func (s *Store) startJob(old, base codec.Hash, p pointer) (*deltaJob, error) {
	seg, err := s.newSegmentName()
	if err != nil {
		return nil, err
	}
	frames, err := s.newSegmentName()
	if err != nil {
		return nil, err
	}
	job := &deltaJob{old: old, base: base, oldSeg: p.segment, seg: seg, framesSeg: frames}
	job.enc = encoder{
		tn:        p.size,
		entries:   s.cfg.DeltaIndexEntries,
		frameOps:  s.cfg.deltaFrameOps,
		pageBytes: s.cfg.deltaPageBytes,
		limit:     p.size / 2,
		contB:     -1,
	}
	s.mu.Lock()
	s.uploads[seg] = struct{}{}
	s.uploads[frames] = struct{}{}
	s.job = job
	s.mu.Unlock()
	return job, nil
}

// dropJobLocked abandons the job, deleting what it wrote. Must hold mu (and deltaMu, or know no
// step runs).
func (s *Store) dropJobLocked() {
	job := s.job
	if job == nil {
		return
	}
	s.job = nil
	for _, name := range []string{job.seg, job.framesSeg} {
		if name == "" {
			continue
		}
		delete(s.uploads, name)
		_ = s.shim.DeleteSegment(name)
	}
}

// runJob advances job by about budget bytes of work and reports whether it is ready to swap.
func (s *Store) runJob(job *deltaJob, oldR, baseR *Reader, p pointer, baseLeaves string, budget int64) (int64, bool, error) {
	e := &job.enc
	e.bn = baseR.Size()
	e.target, e.base = oldR.src, baseR.src
	e.tc, e.bc = newPager(e.target, e.pageBytes), newPager(e.base, e.pageBytes)
	e.tl, e.bl = newLeafSet(s, p.leaves, p.size), newLeafSet(s, baseLeaves, e.bn)
	e.appendSeg = func(b []byte) error { _, err := s.shim.AppendToSegment(job.seg, b); return err }
	e.appendFrame = func(b []byte) error { _, err := s.shim.AppendToSegment(job.framesSeg, b); return err }
	defer func() { e.target, e.base, e.tc, e.bc, e.tl, e.bl = nil, nil, nil, nil, nil, nil }()

	if !job.started {
		job.started = true
		unmatched, err := e.unmatchedBase()
		if err != nil {
			return 0, false, err
		}
		e.block = minBlock
		for unmatched/e.block > int64(e.entries) {
			e.block *= 2
		}
		e.pow = 1
		for i := int64(1); i < e.block; i++ {
			e.pow *= deltaPrime
		}
		if unmatched < e.block || e.tn < e.block {
			job.phase = jobEncode
		} else {
			e.index = make(map[uint64]int64, min(unmatched/e.block, int64(e.entries)))
		}
	}
	used := int64(0)
	for used < budget {
		switch job.phase {
		case jobIndex:
			k, n, err := e.indexChunks(job.ixChunk, budget-used)
			if err != nil {
				return used, false, err
			}
			job.ixChunk, used = k, used+n
			if k >= (e.bn+chunk-1)/chunk {
				if len(e.index) == 0 {
					e.index = nil
				}
				job.phase = jobEncode
			}
		case jobEncode:
			before := e.pos
			done, err := e.step(budget - used)
			if err != nil {
				return used, false, err
			}
			used += e.pos - before
			if done {
				e.index = nil // the encoding is done with it
				if err := s.sealDelta(job); err != nil {
					return used, false, err
				}
				job.phase = jobVerify
			}
		case jobVerify:
			n, done, err := s.verifyDelta(job, budget-used)
			used += n
			if err != nil || done {
				return used, done, err
			}
		}
	}
	return used, false, nil
}

// sealDelta appends the frame index and the footer to the delta and seals it.
func (s *Store) sealDelta(job *deltaJob) error {
	e := &job.enc
	indexOff := e.written
	src := segSource{shim: s.shim, name: job.framesSeg, n: e.frames * frameEntryBytes}
	buf := make([]byte, min(src.n, outPiece))
	for off := int64(0); off < src.n; {
		n := min(int64(len(buf)), src.n-off)
		if err := src.readAt(buf[:n], off); err != nil {
			return err
		}
		if err := e.grow(int(n)); err != nil {
			return err
		}
		if err := e.appendSeg(buf[:n]); err != nil {
			return err
		}
		off += n
	}
	var f [footerBytes]byte
	binary.BigEndian.PutUint64(f[0:], uint64(indexOff))
	binary.BigEndian.PutUint64(f[8:], uint64(e.frames))
	binary.BigEndian.PutUint64(f[16:], uint64(e.tn))
	copy(f[24:], framedMagic)
	if err := e.grow(footerBytes); err != nil {
		return err
	}
	if err := e.appendSeg(f[:]); err != nil {
		return err
	}
	if err := s.shim.FlushSegment(job.seg); err != nil {
		return err
	}
	if err := s.shim.SealSegment(job.seg); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.uploads, job.framesSeg)
	s.mu.Unlock()
	_ = s.shim.DeleteSegment(job.framesSeg)
	job.framesSeg = ""
	return nil
}

// verifyDelta reads the delta back from vPos, a chunk at a time, against old's leaves.
func (s *Store) verifyDelta(job *deltaJob, budget int64) (int64, bool, error) {
	e := &job.enc
	d, err := openFramedDelta(e.base, segSource{shim: s.shim, name: job.seg, n: e.written})
	if err != nil {
		return 0, false, err
	}
	if d.n != e.tn {
		return 0, false, fmt.Errorf("%w: %d bytes, not %d", errDeltaMismatch, d.n, e.tn)
	}
	buf := make([]byte, min(chunk, e.tn))
	used := int64(0)
	for job.vPos < e.tn && used < budget {
		n := min(chunk, e.tn-job.vPos)
		if err := d.readAt(buf[:n], job.vPos); err != nil {
			if errors.Is(err, ErrCorrupt) {
				return used, false, fmt.Errorf("%w: %v", errDeltaMismatch, err)
			}
			return used, false, err
		}
		want, ok, err := e.tl.get(job.vPos / chunk)
		if err != nil {
			return used, false, err
		}
		if !ok || document.ChunkLeafHash(buf[:n]) != want {
			return used, false, fmt.Errorf("%w: chunk %d", errDeltaMismatch, job.vPos/chunk)
		}
		job.vPos += n
		used += n
	}
	return used, job.vPos >= e.tn, nil
}

// finishJob swaps old's pointer to the verified delta, unless old or base changed meanwhile.
func (s *Store) finishJob(job *deltaJob) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job != job {
		return false, nil
	}
	cur, ok, err := s.getLocked(job.old)
	if err != nil {
		s.dropJobLocked()
		return false, err
	}
	chain, baseLarge, err := s.resolveLocked(job.base)
	if !ok || cur.delta || cur.segment != job.oldSeg || err != nil || !baseLarge || len(chain) > s.cfg.DeltaMaxDepth {
		s.dropJobLocked() // changed under us; leave it be
		return false, nil
	}
	np, err := s.writePointerLocked(job.old, pointer{segment: job.seg, leaves: cur.leaves, size: cur.size, delta: true, framed: true, base: job.base})
	if err != nil {
		s.dropJobLocked()
		return false, err
	}
	if err := s.replaceLocked(job.old, np); err != nil {
		return false, err
	}
	delete(s.uploads, job.seg)
	s.job = nil
	s.deleteOrDeferLocked(cur.name)
	s.deleteOrDeferLocked(cur.segment)
	return true, nil
}
