package delta

import (
	"fmt"
	"sync"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/storage"
	storio "github.com/limidus/kdb/go/kdb/storage/io"
)

// DefaultWriter appends framed commits to a delta segment.
type DefaultWriter struct {
	namespaceID string
	segmentID   codec.UUID
	shim        storage.PlatformIOShim
	config      storage.StorageEngineConfig

	mu           sync.Mutex
	segmentName  string
	sizeBytes    int64
	sealed       bool
	firstCommit  *codec.Hash
	lastCommit   *codec.Hash
	pageCodec    PageCodec
}

// NewDefaultWriter opens a new delta segment writer.
func NewDefaultWriter(namespaceID string, segmentID codec.UUID, shim storage.PlatformIOShim, config storage.StorageEngineConfig) *DefaultWriter {
	return &DefaultWriter{
		namespaceID: namespaceID,
		segmentID:   segmentID,
		shim:        shim,
		config:      config,
		segmentName: storio.SegmentNameBuilder.Delta(namespaceID, segmentID.String()),
	}
}

func (w *DefaultWriter) NamespaceID() string    { return w.namespaceID }
func (w *DefaultWriter) SegmentID() codec.UUID    { return w.segmentID }
func (w *DefaultWriter) CurrentSizeBytes() int64  { return w.sizeBytes }
func (w *DefaultWriter) IsSealed() bool           { return w.sealed }

func (w *DefaultWriter) Append(record storage.DeltaRecord) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return 0, fmt.Errorf("segment sealed")
	}
	if w.firstCommit == nil {
		h := record.CommitHash
		w.firstCommit = &h
	}
	h := record.CommitHash
	w.lastCommit = &h
	frame, err := w.pageCodec.Frame(record.CommitPayload, w.config.CompressionCodec)
	if err != nil {
		return 0, err
	}
	offset := w.sizeBytes
	newSize, err := w.shim.AppendToSegment(w.segmentName, frame)
	if err != nil {
		return 0, err
	}
	w.sizeBytes = newSize
	return offset, nil
}

func (w *DefaultWriter) Flush() error {
	return w.shim.FlushSegment(w.segmentName)
}

func (w *DefaultWriter) Seal() (storage.DeltaSegmentRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return storage.DeltaSegmentRef{}, fmt.Errorf("already sealed")
	}
	w.sealed = true
	if err := w.shim.SealSegment(w.segmentName); err != nil {
		return storage.DeltaSegmentRef{}, err
	}
	zero, _ := codec.HashFromBytes(make([]byte, 32))
	first, last := zero, zero
	if w.firstCommit != nil {
		first = *w.firstCommit
	}
	if w.lastCommit != nil {
		last = *w.lastCommit
	}
	return storage.DeltaSegmentRef{
		SegmentID:       w.segmentID,
		NamespaceID:     w.namespaceID,
		FirstCommitHash: first,
		LastCommitHash:  last,
		SizeBytes:       w.sizeBytes,
		Compression:     w.config.CompressionCodec,
	}, nil
}

// DefaultReader reads delta segments for a namespace.
type DefaultReader struct {
	namespaceID string
	shim        storage.PlatformIOShim
	config      storage.StorageEngineConfig
}

// NewDefaultReader returns a delta segment reader.
func NewDefaultReader(namespaceID string, shim storage.PlatformIOShim, config storage.StorageEngineConfig) *DefaultReader {
	return &DefaultReader{namespaceID: namespaceID, shim: shim, config: config}
}

func (r *DefaultReader) NamespaceID() string { return r.namespaceID }

func (r *DefaultReader) ReadAll(segment storage.DeltaSegmentRef) ([]storage.DeltaRecord, error) {
	segmentName := storio.SegmentNameBuilder.Delta(segment.NamespaceID, segment.SegmentID.String())
	bytes, err := r.readFullSegment(segmentName, segment.SizeBytes)
	if err != nil {
		return nil, err
	}
	scanned, err := ScanSegmentBytes(bytes, segment.Compression)
	if err != nil {
		return nil, err
	}
	out := make([]storage.DeltaRecord, 0, len(scanned))
	for _, s := range scanned {
		payload, err := s.Commit.ToPayloadBytes()
		if err != nil {
			return nil, err
		}
		out = append(out, storage.DeltaRecord{
			CommitHash:    s.CommitHash,
			NamespaceID:   segment.NamespaceID,
			Authorship:    storage.DeltaAuthorshipEnvelope{Principal: "unknown", Timestamp: s.Commit.Timestamp},
			CommitPayload: payload,
		})
	}
	return out, nil
}

func (r *DefaultReader) ReadRange(segment storage.DeltaSegmentRef, sinceCommit, untilCommit codec.Hash) ([]storage.DeltaRecord, error) {
	all, err := r.ReadAll(segment)
	if err != nil {
		return nil, err
	}
	var out []storage.DeltaRecord
	pastSince := false
	for _, rec := range all {
		if rec.CommitHash == sinceCommit {
			pastSince = true
		}
		if pastSince && rec.CommitHash != untilCommit {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (r *DefaultReader) ListSegments() ([]storage.DeltaSegmentRef, error) {
	prefix := storio.SegmentNameBuilder.NamespacePrefix(r.namespaceID) + "delta/"
	names, err := r.shim.ListSegments(r.namespaceID)
	if err != nil {
		return nil, err
	}
	var out []storage.DeltaSegmentRef
	for _, name := range names {
		if len(name) < len(prefix) || name[:len(prefix)] != prefix {
			continue
		}
		ref, err := r.scanSegmentRef(name)
		if err != nil || ref == nil {
			continue
		}
		out = append(out, *ref)
	}
	return out, nil
}

func (r *DefaultReader) readFullSegment(segmentName string, sizeBytes int64) ([]byte, error) {
	if sizeBytes <= 0 {
		return []byte{}, nil
	}
	length := int(sizeBytes)
	if int64(length) != sizeBytes {
		length = int(sizeBytes)
	}
	return r.shim.ReadFromSegment(segmentName, 0, length)
}

func (r *DefaultReader) scanSegmentRef(segmentName string) (*storage.DeltaSegmentRef, error) {
	idStr := segmentName[len(segmentName)-36:]
	if len(idStr) != 36 {
		idStr = segmentName
		if i := len(segmentName) - 1; i >= 0 {
			for j := i; j >= 0; j-- {
				if segmentName[j] == '/' {
					idStr = segmentName[j+1:]
					break
				}
			}
		}
	}
	// Allow optional file extension (e.g. "<uuid>.seg").
	if dot := strings.IndexByte(idStr, '.'); dot >= 0 {
		idStr = idStr[:dot]
	}
	segmentID, err := codec.UUIDFromString(idStr)
	if err != nil {
		return nil, err
	}
	bytes, err := r.shim.ReadFromSegment(segmentName, 0, 1<<28)
	if err != nil {
		return nil, err
	}
	scanned, err := ScanSegmentBytes(bytes, r.config.CompressionCodec)
	if err != nil {
		return nil, err
	}
	zero, _ := codec.HashFromBytes(make([]byte, 32))
	if len(scanned) == 0 {
		return &storage.DeltaSegmentRef{
			SegmentID: segmentID, NamespaceID: r.namespaceID,
			FirstCommitHash: zero, LastCommitHash: zero,
			SizeBytes: int64(len(bytes)), Compression: r.config.CompressionCodec,
		}, nil
	}
	return &storage.DeltaSegmentRef{
		SegmentID: segmentID, NamespaceID: r.namespaceID,
		FirstCommitHash: scanned[0].CommitHash,
		LastCommitHash:  scanned[len(scanned)-1].CommitHash,
		SizeBytes:       int64(len(bytes)),
		Compression:     r.config.CompressionCodec,
	}, nil
}

// Factory opens delta writers and readers.
type Factory struct {
	Config storage.StorageEngineConfig
}

func (f Factory) OpenWriter(namespaceID string) (*DefaultWriter, error) {
	id, err := codec.RandomUUID()
	if err != nil {
		return nil, err
	}
	return NewDefaultWriter(namespaceID, id, f.Config.IOShim, f.Config), nil
}

func (f Factory) OpenReader(namespaceID string) *DefaultReader {
	return NewDefaultReader(namespaceID, f.Config.IOShim, f.Config)
}
