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

// AttachmentPutOp sets the document's attachment AttachmentID - creating it, or replacing its bytes
// and name - to the blob ContentHash names, Size bytes long, of type Mime, under Name (unique within
// the document). The document's other attachments are left alone. The document must exist (or be
// written in the same transaction); its JSON is not touched, and its content hash stays as it was.
// The blob must be in the blob store before the commit that references it is acknowledged.
type AttachmentPutOp struct {
	DocID        codec.UUID
	AttachmentID codec.UUID
	Name         string
	// Scheme says how ContentHash was computed (AttachmentHashSchemeChunked1).
	Scheme      byte
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
			2: uuidVal(o.AttachmentID),
			3: codec.StringValue{V: o.Name},
			4: codec.Int32Value{V: int32(o.Scheme)},
			5: hashVal(o.ContentHash),
			6: codec.Int64Value{V: o.Size},
			7: codec.StringValue{V: o.Mime},
		}},
	}
}

// AttachmentDeleteOp removes the document's attachment AttachmentID, leaving the document and its
// other attachments. Deleting the document (DeleteOp) removes all its attachments, without this op.
type AttachmentDeleteOp struct {
	DocID        codec.UUID
	AttachmentID codec.UUID
}

func (AttachmentDeleteOp) isOp() {}

func (o AttachmentDeleteOp) toValue() codec.Value {
	return codec.UnionValue{
		Branch: 5,
		Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: uuidVal(o.DocID),
			2: uuidVal(o.AttachmentID),
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
		attID, err := uuidFromVal(rec.Fields[2])
		if err != nil {
			return nil, NewCommitDecodeError("OpAttachmentPut attachmentId", nil)
		}
		name, ok := rec.Fields[3].(codec.StringValue)
		if !ok {
			return nil, NewCommitDecodeError("OpAttachmentPut name", nil)
		}
		scheme, ok := rec.Fields[4].(codec.Int32Value)
		if !ok || scheme.V < 0 || scheme.V > 255 {
			return nil, NewCommitDecodeError("OpAttachmentPut scheme", nil)
		}
		h, err := hashFromVal(rec.Fields[5])
		if err != nil {
			return nil, NewCommitDecodeError("OpAttachmentPut contentHash", nil)
		}
		size, ok := rec.Fields[6].(codec.Int64Value)
		if !ok || size.V < 0 {
			return nil, NewCommitDecodeError("OpAttachmentPut size", nil)
		}
		mime, ok := rec.Fields[7].(codec.StringValue)
		if !ok {
			return nil, NewCommitDecodeError("OpAttachmentPut mime", nil)
		}
		return AttachmentPutOp{DocID: id, AttachmentID: attID, Name: name.V, Scheme: byte(scheme.V),
			ContentHash: h, Size: size.V, Mime: mime.V}, nil
	case 5:
		id, err := uuidFromVal(rec.Fields[1])
		if err != nil {
			return nil, NewCommitDecodeError("OpAttachmentDelete docId", nil)
		}
		attID, err := uuidFromVal(rec.Fields[2])
		if err != nil {
			return nil, NewCommitDecodeError("OpAttachmentDelete attachmentId", nil)
		}
		return AttachmentDeleteOp{DocID: id, AttachmentID: attID}, nil
	default:
		return nil, NewCommitDecodeError("unknown KdbOp branch", nil)
	}
}
