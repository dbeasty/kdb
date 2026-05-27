package transaction

import (
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// BaseNotFoundError is returned when transaction.baseVersion is missing from the DAG.
type BaseNotFoundError struct {
	*kdberr.VersionNotFoundError
	TransactionID codec.UUID
	MissingHash   codec.Hash
}

func NewBaseNotFoundError(msg string, txID codec.UUID, missing codec.Hash) *BaseNotFoundError {
	return &BaseNotFoundError{
		VersionNotFoundError: kdberr.NewVersionNotFoundError(msg, "", missing.Hex()),
		TransactionID:        txID,
		MissingHash:            missing,
	}
}

// MergeBaseNotFoundError is returned when two branch heads have no common ancestor.
type MergeBaseNotFoundError struct {
	*kdberr.VersionNotFoundError
	PrimaryHead codec.Hash
	MergedHead  codec.Hash
}

func NewMergeBaseNotFoundError(msg string, primary, merged codec.Hash) *MergeBaseNotFoundError {
	return &MergeBaseNotFoundError{
		VersionNotFoundError: kdberr.NewVersionNotFoundError(msg, "", ""),
		PrimaryHead:          primary,
		MergedHead:           merged,
	}
}
