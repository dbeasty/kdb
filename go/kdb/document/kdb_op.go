package document

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// Op is an atomic change within a transaction.
type Op interface {
	isOp()
	toValue() codec.Value
}

type WriteOp struct {
	DocID codec.UUID
	Patch string
}

func (WriteOp) isOp() {}

func (o WriteOp) toValue() codec.Value {
	return codec.UnionValue{
		Branch: 0,
		Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(o.DocID),
			2: codec.StringValue{V: o.Patch},
		}},
	}
}

type DeleteOp struct {
	DocID codec.UUID
}

func (DeleteOp) isOp() {}

func (o DeleteOp) toValue() codec.Value {
	return codec.UnionValue{
		Branch: 1,
		Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(o.DocID),
		}},
	}
}

type FileWriteOp struct {
	Path     string
	BlobHash codec.Hash
}

func (FileWriteOp) isOp() {}

func (o FileWriteOp) toValue() codec.Value {
	return codec.UnionValue{
		Branch: 2,
		Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: o.Path},
			2: hashVal(o.BlobHash),
		}},
	}
}

type SchemaMigrationOp struct {
	MigrationID      codec.UUID
	MigrationPayload string
}

func (SchemaMigrationOp) isOp() {}

func (o SchemaMigrationOp) toValue() codec.Value {
	return codec.UnionValue{
		Branch: 3,
		Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(o.MigrationID),
			2: codec.StringValue{V: o.MigrationPayload},
		}},
	}
}

// OpFromValue decodes a wire KdbOp union value.
func OpFromValue(value codec.Value) (Op, error) {
	uv, ok := value.(codec.UnionValue)
	if !ok {
		return nil, NewCommitDecodeError("KdbOp: expected union", nil)
	}
	rec, ok := uv.Inner.(codec.RecordValue)
	if !ok {
		return nil, NewCommitDecodeError("KdbOp: expected record", nil)
	}
	switch uv.Branch {
	case 0:
		id, err := uuidFromVal(rec.Fields[1])
		if err != nil {
			return nil, NewCommitDecodeError("OpWrite docId", nil)
		}
		patch, ok := rec.Fields[2].(codec.StringValue)
		if !ok {
			return nil, NewCommitDecodeError("OpWrite patch", nil)
		}
		return WriteOp{DocID: id, Patch: patch.V}, nil
	case 1:
		id, err := uuidFromVal(rec.Fields[1])
		if err != nil {
			return nil, NewCommitDecodeError("OpDelete docId", nil)
		}
		return DeleteOp{DocID: id}, nil
	case 2:
		path, ok := rec.Fields[1].(codec.StringValue)
		if !ok {
			return nil, NewCommitDecodeError("OpFileWrite path", nil)
		}
		h, err := hashFromVal(rec.Fields[2])
		if err != nil {
			return nil, err
		}
		return FileWriteOp{Path: path.V, BlobHash: h}, nil
	case 3:
		id, err := uuidFromVal(rec.Fields[1])
		if err != nil {
			return nil, NewCommitDecodeError("OpSchemaMigration migrationId", nil)
		}
		payload, ok := rec.Fields[2].(codec.StringValue)
		if !ok {
			return nil, NewCommitDecodeError("OpSchemaMigration payload", nil)
		}
		return SchemaMigrationOp{MigrationID: id, MigrationPayload: payload.V}, nil
	default:
		return nil, NewCommitDecodeError("unknown KdbOp branch", nil)
	}
}
