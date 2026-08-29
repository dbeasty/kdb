package core

import (
	"bytes"
	"errors"
	"testing"

	"github.com/limidus/kdb/go/kdb/wire"
)

func mustFrame(t *testing.T, msgType wire.MessageType, correlationID int, payload []byte) []byte {
	t.Helper()
	frame, err := wire.EncodeFrameOnly(wire.Header{
		MessageType:     msgType,
		ProtocolVersion: wire.KdbWireProtocolVersion,
		CorrelationID:   correlationID,
	}, payload)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

// The core claim of kdb-spec-layer13 §5.4: the decision is made from the header alone, before the
// body has arrived. Feeding only the header must be enough to get a rejection - if the reader
// waited for the whole frame first, it would have paid exactly the cost shedding exists to avoid.
func TestAdmitterDecidesBeforeBodyArrives(t *testing.T) {
	var sawPayloadLength int
	shed := errors.New("shed")
	r := NewAdmittingFrameStreamReader(wire.DefaultMaxFrameBytes, func(h wire.Header) ([]byte, error) {
		sawPayloadLength = h.PayloadLength
		return []byte("REJECTED"), shed
	})

	body := bytes.Repeat([]byte("x"), 4096)
	frame := mustFrame(t, wire.MsgUpsert, 42, body)

	// Feed only the 12-byte header.
	frames, err := r.Feed(frame[:wire.FrameHeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 0 {
		t.Fatalf("no complete frame has arrived, got %d", len(frames))
	}
	rejections := r.TakeRejections()
	if len(rejections) != 1 || string(rejections[0]) != "REJECTED" {
		t.Fatalf("expected the rejection to be produced from the header alone, got %v", rejections)
	}
	if sawPayloadLength != len(body) {
		t.Errorf("admitter should see the declared payload length %d, got %d", len(body), sawPayloadLength)
	}
}

// A shed frame's body must be consumed and thrown away, and the stream must resynchronize exactly
// at the next frame boundary - shedding a request must not cost the connection.
func TestShedFrameIsDiscardedAndStreamResynchronizes(t *testing.T) {
	shed := errors.New("shed")
	calls := 0
	r := NewAdmittingFrameStreamReader(wire.DefaultMaxFrameBytes, func(h wire.Header) ([]byte, error) {
		calls++
		if h.MessageType == wire.MsgUpsert {
			return []byte("no"), shed
		}
		return nil, nil
	})

	rejected := mustFrame(t, wire.MsgUpsert, 1, bytes.Repeat([]byte("a"), 1000))
	accepted := mustFrame(t, wire.MsgDocumentGet, 2, []byte("keep me"))

	frames, err := r.Feed(append(append([]byte{}, rejected...), accepted...))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("expected exactly the admitted frame through, got %d", len(frames))
	}
	if !bytes.Equal(frames[0], accepted) {
		t.Error("the frame that came through must be the admitted one, byte for byte")
	}
	if got := r.TakeRejections(); len(got) != 1 {
		t.Fatalf("expected one rejection, got %d", len(got))
	}
	if calls != 2 {
		t.Errorf("admitter should be consulted once per frame, got %d calls for 2 frames", calls)
	}
	if r.BufferedBytes() != 0 {
		t.Errorf("nothing should remain buffered, got %d bytes", r.BufferedBytes())
	}
}

// The decision is made once. A body dribbling in over many reads must not re-ask - otherwise
// every metric it drives would over-count, and any future side effect would fire repeatedly.
func TestAdmitterConsultedOncePerFrameAcrossChunkedReads(t *testing.T) {
	calls := 0
	r := NewAdmittingFrameStreamReader(wire.DefaultMaxFrameBytes, func(h wire.Header) ([]byte, error) {
		calls++
		return nil, nil
	})
	frame := mustFrame(t, wire.MsgSqlExec, 7, bytes.Repeat([]byte("y"), 3000))
	for i := 0; i < len(frame); i += 64 {
		end := i + 64
		if end > len(frame) {
			end = len(frame)
		}
		if _, err := r.Feed(frame[i:end]); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Errorf("admitter should be consulted exactly once for one frame, got %d", calls)
	}
}

// Same, for a shed frame whose body arrives in pieces: the discard must span reads without
// re-consulting, and must not eat into the following frame.
func TestShedFrameDiscardSpansChunkedReads(t *testing.T) {
	calls := 0
	shed := errors.New("shed")
	r := NewAdmittingFrameStreamReader(wire.DefaultMaxFrameBytes, func(h wire.Header) ([]byte, error) {
		calls++
		if h.CorrelationID == 1 {
			return []byte("no"), shed
		}
		return nil, nil
	})
	rejected := mustFrame(t, wire.MsgUpsert, 1, bytes.Repeat([]byte("a"), 5000))
	accepted := mustFrame(t, wire.MsgUpsert, 2, []byte("ok"))
	stream := append(append([]byte{}, rejected...), accepted...)

	var got [][]byte
	for i := 0; i < len(stream); i += 97 { // deliberately not a frame-aligned chunk size
		end := i + 97
		if end > len(stream) {
			end = len(stream)
		}
		frames, err := r.Feed(stream[i:end])
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, frames...)
	}
	if len(got) != 1 || !bytes.Equal(got[0], accepted) {
		t.Fatalf("expected only the accepted frame to survive intact, got %d frames", len(got))
	}
	if calls != 2 {
		t.Errorf("expected one admitter call per frame, got %d", calls)
	}
}

// An admitter that returns no rejection frame still sheds the request; there is simply nothing to
// send back.
func TestShedWithNoRejectionFrameStillDiscards(t *testing.T) {
	r := NewAdmittingFrameStreamReader(wire.DefaultMaxFrameBytes, func(h wire.Header) ([]byte, error) {
		return nil, errors.New("shed")
	})
	frame := mustFrame(t, wire.MsgUpsert, 1, []byte("payload"))
	frames, err := r.Feed(frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 0 {
		t.Errorf("frame should have been shed, got %d through", len(frames))
	}
	if got := r.TakeRejections(); got != nil {
		t.Errorf("no rejection frame was offered, so none should be reported, got %v", got)
	}
}

// Without an admitter the reader must behave exactly as it always has - this path is on every
// client connection too, not just servers under pressure.
func TestReaderWithoutAdmitterIsUnchanged(t *testing.T) {
	r := NewFrameStreamReader(wire.DefaultMaxFrameBytes)
	a := mustFrame(t, wire.MsgUpsert, 1, []byte("one"))
	b := mustFrame(t, wire.MsgSqlExec, 2, []byte("two"))
	frames, err := r.Feed(append(append([]byte{}, a...), b...))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || !bytes.Equal(frames[0], a) || !bytes.Equal(frames[1], b) {
		t.Fatal("both frames should pass through untouched")
	}
	if got := r.TakeRejections(); got != nil {
		t.Errorf("a reader with no admitter can never reject, got %v", got)
	}
}

// An oversized length prefix is a framing error, not an admission decision, and must still be
// caught - shedding must not become a way to smuggle a malformed frame past validation.
func TestOversizedFrameStillRejectedWithAdmitter(t *testing.T) {
	r := NewAdmittingFrameStreamReader(64, func(h wire.Header) ([]byte, error) { return nil, nil })
	frame := mustFrame(t, wire.MsgUpsert, 1, bytes.Repeat([]byte("z"), 1000))
	if _, err := r.Feed(frame); err == nil {
		t.Fatal("expected a frame-too-large error")
	}
}

func TestResetClearsAdmissionState(t *testing.T) {
	r := NewAdmittingFrameStreamReader(wire.DefaultMaxFrameBytes, func(h wire.Header) ([]byte, error) {
		return []byte("no"), errors.New("shed")
	})
	frame := mustFrame(t, wire.MsgUpsert, 1, bytes.Repeat([]byte("a"), 2000))
	if _, err := r.Feed(frame[:wire.FrameHeaderSize+10]); err != nil {
		t.Fatal(err)
	}
	r.Reset()
	if r.BufferedBytes() != 0 || r.discardRemaining != 0 || r.headDecided || r.rejections != nil {
		t.Error("Reset must clear pending bytes, the in-progress discard, and any queued rejections")
	}
}

func TestPeekHeaderNeedsOnlyTheHeader(t *testing.T) {
	frame := mustFrame(t, wire.MsgUpsert, 99, bytes.Repeat([]byte("q"), 500))
	if _, ok, err := wire.PeekHeader(frame[:wire.FrameHeaderSize-1], wire.DefaultMaxFrameBytes); ok || err != nil {
		t.Errorf("a short buffer should report not-ready, not an error: ok=%v err=%v", ok, err)
	}
	h, ok, err := wire.PeekHeader(frame[:wire.FrameHeaderSize], wire.DefaultMaxFrameBytes)
	if err != nil || !ok {
		t.Fatalf("header alone should decode: ok=%v err=%v", ok, err)
	}
	if h.MessageType != wire.MsgUpsert || h.CorrelationID != 99 || h.PayloadLength != 500 {
		t.Errorf("unexpected header %+v", h)
	}
}
