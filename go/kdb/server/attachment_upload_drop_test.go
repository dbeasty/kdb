package server

import (
	"testing"
	"time"

	"github.com/limidus/kdb/go/kdb/wire"
)

// A chunk that looked its upload up just before another request dropped it (a failed write on a
// second connection resuming the same upload, or the idle sweep) must get the "expired" answer,
// not write into the dropped upload. It used to call Write on the nil upload, a panic that the
// dispatch backstop turned into an INTERNAL error.
func TestAttachmentChunkAfterConcurrentDropIsExpiredNotInternal(t *testing.T) {
	h := historyHandler(t)
	first := h.dispatchRecovering(wire.AttachmentUploadMessage{
		H:    wire.Header{MessageType: wire.MsgAttachmentUpload, CorrelationID: 1},
		Data: []byte("hello "),
	}).(wire.AttachmentUploadResultMessage)
	if first.Error != "" || first.UploadID == "" {
		t.Fatalf("first chunk: %+v", first)
	}
	rt := h.runtime
	rt.attachUploads.mu.Lock()
	up := rt.attachUploads.m[first.UploadID]
	rt.attachUploads.mu.Unlock()
	if up == nil {
		t.Fatal("upload not registered")
	}

	// Hold the upload as a request on another connection would mid-chunk, let the next chunk look
	// it up and queue behind it, then drop it the way that request's failed write does.
	up.mu.Lock()
	replies := make(chan wire.Message, 1)
	go func() {
		replies <- h.dispatchRecovering(wire.AttachmentUploadMessage{
			H:        wire.Header{MessageType: wire.MsgAttachmentUpload, CorrelationID: 2},
			UploadID: first.UploadID,
			Offset:   first.Received,
			Data:     []byte("world"),
			Final:    true,
		})
	}()
	time.Sleep(100 * time.Millisecond)
	h.dropUpload(rt, first.UploadID, up)
	up.mu.Unlock()

	reply, ok := (<-replies).(wire.AttachmentUploadResultMessage)
	if !ok {
		t.Fatalf("reply is not an upload result")
	}
	if reply.ErrorCode != wire.ErrorCodeNotFound {
		t.Fatalf("chunk after a concurrent drop: code %q error %q, want NOT_FOUND (expired)", reply.ErrorCode, reply.Error)
	}
}
