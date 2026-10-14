package delta

import (
	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/bufpool"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// StreamFramePayloads implements storage.DeltaPayloadStreamer: fn sees each frame's decompressed
// payload, in order, with one frame in memory at a time. StreamCommits reads the whole segment
// (64 MiB) and builds a commit per frame - every operation and patch of it - which for a reader
// that wants a few writes per frame is nearly all garbage.
//
// The payload is valid only during fn. The walk stops quietly at the first frame it cannot read
// whole and intact - a torn tail, or damage, which this reader does not try to tell apart: it is
// for readers to whom a missed frame costs speed, not answers (the snapshot copy, whose misses
// are read the ordinary way). Anything that must see every frame uses StreamCommits.
func (r *DefaultReader) StreamFramePayloads(segment storage.DeltaSegmentRef, fn func(payload []byte, frameOffset int64) error) error {
	name := storio.SegmentNameBuilder.DeltaSequenced(segment.NamespaceID, segment.SequenceNumber)
	for off := int64(0); off+frameHeaderSize <= segment.SizeBytes; {
		frame, buf, err := r.readFrame(name, off)
		if err != nil {
			return nil
		}
		end, ok := frameBounds(frame, 0)
		if !ok || verifyFrameCRC(frame[:end], int(off)) != nil {
			bufpool.Put(buf)
			return nil
		}
		payload, pbuf, err := framePayload(frame[:end])
		if err != nil {
			bufpool.Put(buf)
			return nil
		}
		err = fn(payload, off)
		bufpool.Put(pbuf)
		bufpool.Put(buf)
		if err != nil {
			return err
		}
		off += int64(end)
	}
	return nil
}

var _ storage.DeltaPayloadStreamer = (*DefaultReader)(nil)
