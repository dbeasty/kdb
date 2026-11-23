package io

import (
	"os"
	"path/filepath"
	"strings"
)

// spareSegment is the next preallocated delta segment of a namespace, created ahead of need.
//
// Preallocating a segment writes its full size in zeros and syncs them (preallocateFile): 64 MiB
// by default, tens to hundreds of milliseconds of I/O. Done at the moment a rotation first writes
// the new segment, that is the commit-log writer's time, and every commit acknowledged after a
// sync waits behind it - the largest durable-mode write stalls once segments are preallocated.
// Cassandra keeps its commit log off that path the same way: CommitLogSegmentManager's allocator
// thread always has the next segment created and ready before the active one fills.
//
// The spare is an unnamed file (O_TMPFILE) in the segment's directory, so it is invisible to every
// listing - Go's, Kotlin's and older builds', which refuse a namespace holding any delta file
// they do not recognise - and a crash or close leaves nothing behind. When the writer first
// appends to the next segment, openFor links the spare in under that segment's name: the same
// file, at the same size, holding the same synced zeros as one created on the spot, so nothing
// that reads it can tell the difference. Off Linux, and on a filesystem without O_TMPFILE, there
// is no spare and segments are created as before.
//
// Preparation starts once the active segment is half written (spareStartDivisor), so a namespace
// that writes rarely never holds a second segment's worth of disk.
type spareSegment struct {
	// ready is closed once preparation has finished, with file set (nil if it failed). Both are
	// written under OSByteStore.mu.
	ready chan struct{}
	file  *os.File
	// dropped means nobody will take this spare (the store closed, the namespace released its
	// handles): of the goroutine finishing and the dropper, whichever sees both ready closed and
	// dropped set - they decide under OSByteStore.mu - closes file.
	dropped bool
}

// spareStartDivisor: preparing the next segment starts once the active one holds
// 1/spareStartDivisor of its preallocated size.
const spareStartDivisor = 2

// nextDeltaSegmentName is the segment a writer rotates to after segmentName: the same directory,
// the next sequence number. ok is false for anything that is not a sequenced delta segment.
func nextDeltaSegmentName(segmentName string) (string, bool) {
	i := strings.LastIndex(segmentName, "/")
	if i < 0 {
		return "", false
	}
	seq, ok := ParseDeltaSequencedFileName(segmentName[i+1:])
	if !ok {
		return "", false
	}
	return segmentName[:i+1] + DeltaSequencedFileName(seq+1), true
}

// maybePrepareSpare starts preparing seg's successor once seg is far enough along. Called after
// every append; costs a comparison and, once per segment, a goroutine.
func (s *OSByteStore) maybePrepareSpare(seg *openSegment, newSize int64) {
	if seg.next == "" || newSize < s.preallocateBytes/spareStartDivisor || !seg.spareStarted.CompareAndSwap(false, true) {
		return
	}
	s.prepareSpare(seg.next)
}

// prepareSpare creates and preallocates an unnamed file to become segmentName, on a goroutine.
func (s *OSByteStore) prepareSpare(segmentName string) {
	if s.noSpares.Load() {
		return
	}
	s.mu.Lock()
	if _, exists := s.spares[segmentName]; exists {
		s.mu.Unlock()
		return
	}
	sp := &spareSegment{ready: make(chan struct{})}
	s.spares[segmentName] = sp
	s.mu.Unlock()
	dir := filepath.Dir(s.pathFor(segmentName))
	size := s.preallocateBytes
	go func() {
		f, err := createUnnamedFile(dir)
		if err != nil {
			// No O_TMPFILE here: stop trying, segments are created by name as before.
			s.noSpares.Store(true)
			f = nil
		} else if err := preallocateFileTrickle(f, size, trickleSyncBytes); err != nil {
			_ = f.Close()
			f = nil
		}
		s.mu.Lock()
		sp.file = f
		close(sp.ready)
		dropped := sp.dropped
		s.mu.Unlock()
		if dropped && f != nil {
			_ = f.Close()
		}
	}()
}

// takeSpare removes and returns the spare prepared for segmentName, waiting for it if it is
// still being prepared (which costs the caller no more than creating the segment itself would).
// nil when there is none or preparing it failed; the caller owns the result.
func (s *OSByteStore) takeSpare(segmentName string) *os.File {
	s.mu.Lock()
	sp := s.spares[segmentName]
	delete(s.spares, segmentName)
	s.mu.Unlock()
	if sp == nil {
		return nil
	}
	<-sp.ready
	return sp.file
}

// dropSpares abandons every spare whose segment name has prefix, closing those already prepared;
// the rest are closed by their goroutine when it finishes. It returns the ready channels of the
// dropped spares, for a caller that has to wait until none is left running.
func (s *OSByteStore) dropSpares(prefix string) []chan struct{} {
	s.mu.Lock()
	var waits []chan struct{}
	var closeNow []*os.File
	for name, sp := range s.spares {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		delete(s.spares, name)
		sp.dropped = true
		waits = append(waits, sp.ready)
		select {
		case <-sp.ready:
			if sp.file != nil {
				closeNow = append(closeNow, sp.file)
			}
		default:
		}
	}
	s.mu.Unlock()
	for _, f := range closeNow {
		_ = f.Close()
	}
	return waits
}

// spareCount reports the spares currently held or being prepared (tests).
func (s *OSByteStore) spareCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.spares)
}
