package wire

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

func opToDto(op document.Op) opDto {
	switch o := op.(type) {
	case document.WriteOp:
		id := o.DocID.String()
		return opDto{Kind: "write", DocID: &id, Patch: &o.Patch}
	case document.DeleteOp:
		id := o.DocID.String()
		return opDto{Kind: "delete", DocID: &id}
	case document.FileWriteOp:
		h := o.BlobHash.Hex()
		return opDto{Kind: "fileWrite", Path: &o.Path, BlobHashHex: &h}
	case document.SchemaMigrationOp:
		id := o.MigrationID.String()
		return opDto{Kind: "schemaMigration", MigrationID: &id, MigrationPayload: &o.MigrationPayload}
	default:
		return opDto{Kind: "unknown"}
	}
}

func opFromDto(d opDto) (document.Op, error) {
	switch d.Kind {
	case "write":
		if d.DocID == nil || d.Patch == nil {
			return nil, fmt.Errorf("write missing fields")
		}
		id, err := codec.UUIDFromString(*d.DocID)
		if err != nil {
			return nil, err
		}
		return document.WriteOp{DocID: id, Patch: *d.Patch}, nil
	case "delete":
		if d.DocID == nil {
			return nil, fmt.Errorf("delete missing docId")
		}
		id, err := codec.UUIDFromString(*d.DocID)
		if err != nil {
			return nil, err
		}
		return document.DeleteOp{DocID: id}, nil
	case "fileWrite":
		if d.Path == nil || d.BlobHashHex == nil {
			return nil, fmt.Errorf("fileWrite missing fields")
		}
		h, err := codec.HashFromHex(*d.BlobHashHex)
		if err != nil {
			return nil, err
		}
		return document.FileWriteOp{Path: *d.Path, BlobHash: h}, nil
	case "schemaMigration":
		if d.MigrationID == nil || d.MigrationPayload == nil {
			return nil, fmt.Errorf("schemaMigration missing fields")
		}
		id, err := codec.UUIDFromString(*d.MigrationID)
		if err != nil {
			return nil, err
		}
		return document.SchemaMigrationOp{MigrationID: id, MigrationPayload: *d.MigrationPayload}, nil
	default:
		return nil, fmt.Errorf("unknown op kind: %s", d.Kind)
	}
}

func hintToDto(h IndexHint) indexHintDto {
	return indexHintDto{
		IndexID:       h.IndexID.String(),
		FieldName:     h.FieldName,
		IndexType:     h.IndexType,
		Action:        h.Action,
		DocID:         h.DocID.String(),
		Key:           h.Key,
		CommitHashHex: h.CommitHash.Hex(),
	}
}

func hintFromDto(d indexHintDto) (IndexHint, error) {
	indexID, err := codec.UUIDFromString(d.IndexID)
	if err != nil {
		return IndexHint{}, err
	}
	docID, err := codec.UUIDFromString(d.DocID)
	if err != nil {
		return IndexHint{}, err
	}
	commitHash, err := codec.HashFromHex(d.CommitHashHex)
	if err != nil {
		return IndexHint{}, err
	}
	return IndexHint{
		IndexID:    indexID,
		FieldName:  d.FieldName,
		IndexType:  d.IndexType,
		Action:     d.Action,
		DocID:      docID,
		Key:        d.Key,
		CommitHash: commitHash,
	}, nil
}
