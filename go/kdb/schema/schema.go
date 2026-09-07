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
	// UniqueConstraints are the compound (multi-field) unique constraints declared on the
	// schema (Layer 16, Component 73). Single-field uniqueness is still expressed through
	// Field.Unique; UniqueTuples merges both views. Encoded as wire field 5 of KdbSchemaBody
	// with an empty default, so a schema without compound constraints hashes exactly as it
	// did before the field existed.
	UniqueConstraints []UniqueConstraint

	fieldsByName map[string]Field
}

// UniqueConstraint is one compound unique constraint: the ordered tuple of field names whose
// combined values must be unique across live documents. A document in which any part is
// absent or JSON null claims nothing (sparse semantics).
type UniqueConstraint struct {
	Fields []string
}

// UniqueTuples returns every unique constraint as an ordered field tuple: one 1-tuple per
// Field.Unique declaration (in declaration order) followed by the compound constraints.
func (s KdbSchema) UniqueTuples() [][]string {
	var out [][]string
	for _, f := range s.Fields {
		if f.Unique {
			out = append(out, []string{f.Name})
		}
	}
	for _, c := range s.UniqueConstraints {
		out = append(out, append([]string(nil), c.Fields...))
	}
	return out
}

// HasUniqueConstraints reports whether any single-field or compound unique constraint exists.
func (s KdbSchema) HasUniqueConstraints() bool {
	return s.HasUniqueFields() || len(s.UniqueConstraints) > 0
}

func validateUniqueConstraints(fields []Field, constraints []UniqueConstraint) error {
	names := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		names[f.Name] = struct{}{}
	}
	for _, c := range constraints {
		if len(c.Fields) == 0 {
			return fmt.Errorf("unique constraint must name at least one field")
		}
		seen := make(map[string]struct{}, len(c.Fields))
		for _, name := range c.Fields {
			if _, ok := names[name]; !ok {
				return fmt.Errorf("unique constraint references unknown field: %s", name)
			}
			if _, dup := seen[name]; dup {
				return fmt.Errorf("unique constraint repeats field: %s", name)
			}
			seen[name] = struct{}{}
		}
	}
	return nil
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
		body := schemaBodyValue(nil, nil, 0, ts, "")
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
			SchemaHash:   h,
			Fields:       nil,
			Version:      0,
			CreatedAt:    ts,
			Description:  "",
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
	return BuildWithConstraints(fields, nil, version, createdAt, description)
}

// BuildWithConstraints is Build plus compound unique constraints (Component 73).
func BuildWithConstraints(fields []Field, constraints []UniqueConstraint, version int, createdAt codec.Timestamp, description string) (KdbSchema, error) {
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
	if err := validateUniqueConstraints(fields, constraints); err != nil {
		return KdbSchema{}, err
	}
	reg := WireRegistry()
	body := schemaBodyValue(fields, constraints, version, createdAt, description)
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
		SchemaHash:        h,
		Fields:            append([]Field(nil), fields...),
		Version:           version,
		CreatedAt:         createdAt,
		Description:       description,
		UniqueConstraints: cloneConstraints(constraints),
		fieldsByName:      byName,
	}, nil
}

func cloneConstraints(in []UniqueConstraint) []UniqueConstraint {
	if len(in) == 0 {
		return nil
	}
	out := make([]UniqueConstraint, len(in))
	for i, c := range in {
		out[i] = UniqueConstraint{Fields: append([]string(nil), c.Fields...)}
	}
	return out
}

// ToBytes returns canonical typed-binary encoding.
func (s KdbSchema) ToBytes() ([]byte, error) {
	v := schemaBodyValue(s.Fields, s.UniqueConstraints, s.Version, s.CreatedAt, s.Description)
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
