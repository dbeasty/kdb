package document

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/codec/schema"
)

const wireNS = "dev.kdb.document"

var (
	fqnHash32        = wireNS + ".Hash32"
	fqnDocumentBody  = wireNS + ".DocumentBody"
	fqnDocTreeEntry  = wireNS + ".DocumentTreeEntry"
	fqnCommitPayload = wireNS + ".CommitPayload"
	fqnCommitStub    = wireNS + ".CommitStubWire"
	fqnOpWrite       = wireNS + ".OpWrite"
	fqnOpDelete      = wireNS + ".OpDelete"
	fqnOpFileWrite   = wireNS + ".OpFileWrite"
	fqnOpSchemaMigr  = wireNS + ".OpSchemaMigration"
	fqnOpAttachPut   = wireNS + ".OpAttachmentPut"
	fqnOpAttachDel   = wireNS + ".OpAttachmentDelete"
)

var (
	uuidPrim = schema.Primitive{Physical: schema.PhysicalFixed, Logical: schema.LogicalUUID{}}
	hashRef  = schema.Ref{FullyQualifiedName: fqnHash32}
	tsPrim   = schema.Primitive{Physical: schema.PhysicalInt64, Logical: schema.LogicalTimestampMicros{}}
)

var (
	wireOnce sync.Once
	wireReg  *schema.Registry
)

// WireRegistry returns the builtin document wire registry.
func WireRegistry() *schema.Registry {
	wireOnce.Do(func() {
		r := schema.NewRegistry()
		r.RegisterFixed(&schema.FixedSchema{Name: "Hash32", Namespace: wireNS, Size: 32})

		r.RegisterRecord(&schema.RecordSchema{
			Name: "OpWrite", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "docId", Type: uuidPrim},
				{ID: 2, Name: "patch", Type: schema.Prim(schema.PhysicalString)},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "OpDelete", Namespace: wireNS,
			Fields: []schema.FieldSchema{{ID: 1, Name: "docId", Type: uuidPrim}},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "OpFileWrite", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "path", Type: schema.Prim(schema.PhysicalString)},
				{ID: 2, Name: "blobHash", Type: hashRef},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "OpSchemaMigration", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "migrationId", Type: uuidPrim},
				{ID: 2, Name: "migrationPayload", Type: schema.Prim(schema.PhysicalString)},
			},
		})

		// One attachment per record (docs/kdb-attachments-plan.md). Appended, never inserted: a
		// branch's position is its wire tag.
		r.RegisterRecord(&schema.RecordSchema{
			Name: "OpAttachmentPut", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "docId", Type: uuidPrim},
				{ID: 2, Name: "attachmentId", Type: uuidPrim},
				{ID: 3, Name: "name", Type: schema.Prim(schema.PhysicalString)},
				{ID: 4, Name: "scheme", Type: schema.Prim(schema.PhysicalInt32)},
				{ID: 5, Name: "contentHash", Type: hashRef},
				{ID: 6, Name: "size", Type: schema.Prim(schema.PhysicalInt64)},
				{ID: 7, Name: "mime", Type: schema.Prim(schema.PhysicalString)},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "OpAttachmentDelete", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "docId", Type: uuidPrim},
				{ID: 2, Name: "attachmentId", Type: uuidPrim},
			},
		})

		kdbOpWire := schema.Union{Branches: []schema.Type{
			schema.Ref{FullyQualifiedName: fqnOpWrite},
			schema.Ref{FullyQualifiedName: fqnOpDelete},
			schema.Ref{FullyQualifiedName: fqnOpFileWrite},
			schema.Ref{FullyQualifiedName: fqnOpSchemaMigr},
			schema.Ref{FullyQualifiedName: fqnOpAttachPut},
			schema.Ref{FullyQualifiedName: fqnOpAttachDel},
		}}

		r.RegisterRecord(&schema.RecordSchema{
			Name: "DocumentBody", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "id", Type: uuidPrim},
				{ID: 2, Name: "json", Type: schema.Prim(schema.PhysicalString)},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "DocumentTreeEntry", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "docId", Type: uuidPrim},
				{ID: 2, Name: "contentHash", Type: hashRef},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CommitPayload", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "parentHashes", Type: schema.Array{Element: hashRef}},
				{ID: 2, Name: "namespaceId", Type: schema.Prim(schema.PhysicalString)},
				{ID: 3, Name: "transactionId", Type: uuidPrim},
				{ID: 4, Name: "timestamp", Type: tsPrim},
				{ID: 5, Name: "authorNodeId", Type: uuidPrim},
				{ID: 6, Name: "operations", Type: schema.Array{Element: kdbOpWire}},
				{ID: 7, Name: "documentTreeHash", Type: hashRef},
				{ID: 8, Name: "schemaHash", Type: schema.Nullable{Inner: hashRef}, Default: codec.Null},
				{ID: 9, Name: "message", Type: schema.Prim(schema.PhysicalString)},
				// The root of the commit's attachment table (AttachmentTable.Root). Omitted when the
				// table is empty, so a commit with no attachments hashes exactly as before it existed.
				{ID: 10, Name: "attachmentTreeHash", Type: schema.Nullable{Inner: hashRef}, Default: codec.Null},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "CommitStubWire", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "originalHash", Type: hashRef},
				{ID: 2, Name: "archiveLocation", Type: schema.Prim(schema.PhysicalString)},
				{ID: 3, Name: "stubbedAt", Type: tsPrim},
			},
		})
		r.Freeze()
		wireReg = r
	})
	return wireReg
}

// DocumentBodyType is the wire type for document bodies.
var DocumentBodyType = schema.Ref{FullyQualifiedName: fqnDocumentBody}

// DocumentTreeType is the wire type for document trees.
var DocumentTreeType = schema.Array{Element: schema.Ref{FullyQualifiedName: fqnDocTreeEntry}}

// CommitPayloadType is the wire type for commit payloads.
var CommitPayloadType = schema.Ref{FullyQualifiedName: fqnCommitPayload}

// CommitStubWireType is the wire type for commit stubs.
var CommitStubWireType = schema.Ref{FullyQualifiedName: fqnCommitStub}

func uuidVal(u codec.UUID) codec.UUIDValue {
	return codec.UUIDValue{MSB: u.MSB, LSB: u.LSB}
}

func hashVal(h codec.Hash) codec.FixedValue {
	return codec.FixedValue{V: append([]byte(nil), h.Bytes[:]...)}
}

func timestampVal(t codec.Timestamp) codec.TimestampValue {
	return codec.TimestampValue{EpochMicros: t.EpochMicros()}
}

func uuidFromVal(v codec.Value) (codec.UUID, error) {
	u, ok := v.(codec.UUIDValue)
	if !ok {
		return codec.UUID{}, fmt.Errorf("expected uuid")
	}
	return codec.UUID{MSB: u.MSB, LSB: u.LSB}, nil
}

func hashFromVal(v codec.Value) (codec.Hash, error) {
	f, ok := v.(codec.FixedValue)
	if !ok || len(f.V) != 32 {
		return codec.Hash{}, fmt.Errorf("expected hash32")
	}
	return codec.HashFromBytes(f.V)
}

func timestampFromVal(v codec.Value) (codec.Timestamp, error) {
	ts, ok := v.(codec.TimestampValue)
	if !ok {
		return codec.Timestamp{}, fmt.Errorf("expected timestamp")
	}
	return codec.TimestampFromEpochMicros(ts.EpochMicros), nil
}
