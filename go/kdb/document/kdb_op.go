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
	// Replace makes Patch the whole document instead of a merge patch over what is stored, so a
	// key the patch does not name is removed. It is a property of the *request* only: staging
	// resolves either kind to the full document, and a commit records that document
	// (operationsAsStored), so Replace is never part of the stored KdbOp and replay, history and
	// peer sync see no new op kind. That is why it is not in toValue.
	Replace bool
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

// AttachmentPutOp sets the document's one binary attachment: the blob ContentHash names, Size
// bytes long, of type Mime. The document must exist (or be written in the same transaction); the
// JSON body is not touched, and replacing the bytes leaves the body's content hash as it was. The
// blob must be in the blob store before the commit that references it is acknowledged.
type AttachmentPutOp struct {
	DocID       codec.UUID
	ContentHash codec.Hash
	Size        int64
	Mime        string
}

func (AttachmentPutOp) isOp() {}

func (o AttachmentPutOp) toValue() codec.Value {
	return codec.UnionValue{
		Branch: 4,
		Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(o.DocID),
			2: hashVal(o.ContentHash),
			3: codec.Int64Value{V: o.Size},
			4: codec.StringValue{V: o.Mime},
		}},
	}
}

// AttachmentDeleteOp removes the document's attachment, leaving the document. Deleting the
// document (DeleteOp) removes its attachment too, without this op.
type AttachmentDeleteOp struct {
	DocID codec.UUID
}

func (AttachmentDeleteOp) isOp() {}

func (o AttachmentDeleteOp) toValue() codec.Value {
	return codec.UnionValue{
		Branch: 5,
		Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(o.DocID),
		}},
	}
}

// OpDocID names the document an operation changes, if it changes one.
func OpDocID(op Op) (codec.UUID, bool) {
	switch o := op.(type) {
	case WriteOp:
		return o.DocID, true
	case DeleteOp:
		return o.DocID, true
	case AttachmentPutOp:
		return o.DocID, true
	case AttachmentDeleteOp:
		return o.DocID, true
	default:
		return codec.UUID{}, false
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
	case 4:
		id, err := uuidFromVal(rec.Fields[1])
		if err != nil {
			return nil, NewCommitDecodeError("OpAttachmentPut docId", nil)
		}
		h, err := hashFromVal(rec.Fields[2])
		if err != nil {
			return nil, NewCommitDecodeError("OpAttachmentPut contentHash", nil)
		}
		size, ok := rec.Fields[3].(codec.Int64Value)
		if !ok || size.V < 0 {
			return nil, NewCommitDecodeError("OpAttachmentPut size", nil)
		}
		mime, ok := rec.Fields[4].(codec.StringValue)
		if !ok {
			return nil, NewCommitDecodeError("OpAttachmentPut mime", nil)
		}
		return AttachmentPutOp{DocID: id, ContentHash: h, Size: size.V, Mime: mime.V}, nil
	case 5:
		id, err := uuidFromVal(rec.Fields[1])
		if err != nil {
			return nil, NewCommitDecodeError("OpAttachmentDelete docId", nil)
		}
		return AttachmentDeleteOp{DocID: id}, nil
	default:
		return nil, NewCommitDecodeError("unknown KdbOp branch", nil)
	}
}
