package storage

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// DocumentNotFoundError is returned when a document is missing at a commit.
type DocumentNotFoundError struct {
	kdberr.VersionNotFoundError
	NamespaceID string
	DocID       codec.UUID
	AtCommit    codec.Hash
}

func NewDocumentNotFoundError(msg, namespaceID string, docID codec.UUID, atCommit codec.Hash) *DocumentNotFoundError {
	return &DocumentNotFoundError{
		VersionNotFoundError: *kdberr.NewVersionNotFoundError(msg, namespaceID, atCommit.Hex()),
		NamespaceID:          namespaceID,
		DocID:                docID,
		AtCommit:             atCommit,
	}
}

func (e *DocumentNotFoundError) Error() string {
	return fmt.Sprintf("%s (doc=%s commit=%s)", e.VersionNotFoundError.Error(), e.DocID.String(), e.AtCommit.Hex())
}
