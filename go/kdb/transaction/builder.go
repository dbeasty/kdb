package transaction

import (
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

// Builder accumulates operations for one transaction.
type Builder struct {
	NamespaceID  string
	BaseVersion  codec.Hash
	AuthorNodeID codec.UUID
	Schema       schema.KdbSchema

	mu  sync.Mutex
	ops []document.Op
}

// Write appends a JSON patch write.
func (b *Builder) Write(docID codec.UUID, patchJSON string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, document.WriteOp{DocID: docID, Patch: patchJSON})
	return b
}

// Replace appends a write that makes bodyJSON the whole document: unlike Write, a key it does not
// name is removed, and it is atomic (one operation, one commit).
func (b *Builder) Replace(docID codec.UUID, bodyJSON string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, document.WriteOp{DocID: docID, Patch: bodyJSON, Replace: true})
	return b
}

// WriteDocument writes a full document body.
func (b *Builder) WriteDocument(doc document.Document) *Builder {
	return b.Write(doc.ID, doc.JSON)
}

// Delete appends a document delete.
func (b *Builder) Delete(docID codec.UUID) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, document.DeleteOp{DocID: docID})
	return b
}

// FileWrite references a blob by hash.
func (b *Builder) FileWrite(path string, blobHash codec.Hash) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, document.FileWriteOp{Path: path, BlobHash: blobHash})
	return b
}

// Attach sets docID's attachment to the stored blob contentHash names (size bytes, of type mime),
// in the same commit as the builder's other operations. The blob must already be stored - put
// its bytes first (embed's PutAttachment does both).
func (b *Builder) Attach(docID codec.UUID, contentHash codec.Hash, size int64, mime string) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, document.AttachmentPutOp{DocID: docID, ContentHash: contentHash, Size: size, Mime: mime})
	return b
}

// Detach removes docID's attachment, leaving the document.
func (b *Builder) Detach(docID codec.UUID) *Builder {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, document.AttachmentDeleteOp{DocID: docID})
	return b
}

// SchemaMigration appends a schema migration op.
func (b *Builder) SchemaMigration(migration schema.SchemaMigration) error {
	payload, err := EncodeMigration(migration)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ops = append(b.ops, document.SchemaMigrationOp{
		MigrationID:      migration.MigrationID,
		MigrationPayload: payload,
	})
	return nil
}

// Build materializes a KdbTransaction.
func (b *Builder) Build(timestamp codec.Timestamp) (document.Transaction, error) {
	if timestamp.EpochMicros() == 0 {
		timestamp = codec.TimestampNow()
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return document.Transaction{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ops := append([]document.Op(nil), b.ops...)
	return document.Transaction{
		ID:           id,
		BaseVersion:  b.BaseVersion,
		Operations:   ops,
		Timestamp:    timestamp,
		AuthorNodeID: b.AuthorNodeID,
	}, nil
}
