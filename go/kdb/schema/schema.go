package schema

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// KdbSchema is a full schema snapshot for one namespace version.
type KdbSchema struct {
	SchemaHash  codec.Hash
	Fields      []Field
	Version     int
	CreatedAt   codec.Timestamp
	Description string

	fieldsByName map[string]Field
}

var (
	noneOnce sync.Once
	noneVal  KdbSchema
)

// None is the sentinel for schema-less namespaces.
func None() KdbSchema {
	noneOnce.Do(func() {
		ts := codec.Timestamp{EpochMillis: 0, MicroRemainder: 0}
		reg := WireRegistry()
		body := schemaBodyValue(nil, 0, ts, "")
		bytes, err := codec.EncodeBytes(body, BodyWireType(), reg)
		if err != nil {
			panic(err)
		}
		sum := document.SHA256Digest(bytes)
		h, err := codec.HashFromBytes(sum[:])
		if err != nil {
			panic(err)
		}
		noneVal = KdbSchema{
			SchemaHash:  h,
			Fields:      nil,
			Version:     0,
			CreatedAt:   ts,
			Description: "",
			fieldsByName: map[string]Field{},
		}
	})
	return noneVal
}

// IsNone reports whether s is the schema-less sentinel.
func (s KdbSchema) IsNone() bool { return s.SchemaHash == None().SchemaHash }

func (s *KdbSchema) indexFields() {
	if s.fieldsByName != nil {
		return
	}
	s.fieldsByName = make(map[string]Field, len(s.Fields))
	for _, f := range s.Fields {
		s.fieldsByName[f.Name] = f
	}
}

// HasField reports whether a field name exists.
func (s KdbSchema) HasField(name string) bool {
	s.indexFields()
	_, ok := s.fieldsByName[name]
	return ok
}

// Field returns a field by name.
func (s KdbSchema) Field(name string) (Field, bool) {
	s.indexFields()
	f, ok := s.fieldsByName[name]
	return f, ok
}

// Build constructs a schema with a content-addressed hash.
func Build(fields []Field, version int, createdAt codec.Timestamp, description string) (KdbSchema, error) {
	if version < 1 {
		return KdbSchema{}, fmt.Errorf("schema version must be >= 1")
	}
	seen := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		if _, dup := seen[f.Name]; dup {
			return KdbSchema{}, fmt.Errorf("duplicate field names")
		}
		seen[f.Name] = struct{}{}
	}
	reg := WireRegistry()
	body := schemaBodyValue(fields, version, createdAt, description)
	bytes, err := codec.EncodeBytes(body, BodyWireType(), reg)
	if err != nil {
		return KdbSchema{}, err
	}
	sum := document.SHA256Digest(bytes)
	h, err := codec.HashFromBytes(sum[:])
	if err != nil {
		return KdbSchema{}, err
	}
	byName := make(map[string]Field, len(fields))
	for _, f := range fields {
		byName[f.Name] = f
	}
	return KdbSchema{
		SchemaHash:   h,
		Fields:       append([]Field(nil), fields...),
		Version:      version,
		CreatedAt:    createdAt,
		Description:  description,
		fieldsByName: byName,
	}, nil
}

// ToBytes returns canonical typed-binary encoding.
func (s KdbSchema) ToBytes() ([]byte, error) {
	v := schemaBodyValue(s.Fields, s.Version, s.CreatedAt, s.Description)
	return codec.EncodeBytes(v, BodyWireType(), WireRegistry())
}

// FromBytes decodes a schema snapshot.
func FromBytes(b []byte) (KdbSchema, error) {
	reg := WireRegistry()
	v, err := codec.DecodeBytes(b, BodyWireType(), reg)
	if err != nil {
		return KdbSchema{}, newDecodeError("schema body decode failed", err)
	}
	rec, ok := v.(codec.RecordValue)
	if !ok {
		return KdbSchema{}, newDecodeError("schema body: expected record", nil)
	}
	return parseSchemaBodyRecord(rec)
}
