package index

import (
	"crypto/sha256"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/schema"
)

// InferIndexType mirrors Kotlin's inferIndexType: ordered numeric and timestamp fields get a
// BTREE (so range predicates can use them), everything else a HASH.
func InferIndexType(ft schema.FieldType) IndexType {
	switch ft.(type) {
	case schema.Int32Type, schema.Int64Type, schema.Float64Type, schema.TimestampType:
		return IndexTypeBTree
	default:
		return IndexTypeHash
	}
}

// DerivedIndexID is the stable id of a schema-derived index: the same namespace, field and
// type always yield the same id, so a catalog entry survives restarts and matches the one
// Kotlin derives (uuid8 of sha256("kdb-index|" ns "|" field "|" TYPE)).
func DerivedIndexID(namespaceID, field string, typ IndexType) codec.UUID {
	sum := sha256.Sum256([]byte("kdb-index|" + namespaceID + "|" + field + "|" + typ.String()))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x80
	b[8] = (b[8] & 0x3f) | 0x80
	id, _ := codec.UUIDFromBytes(b)
	return id
}

// DescriptorsFromSchema derives one hash/btree descriptor per indexed schema field. The
// index is named `<field>_<hash|btree>` and carries the field's codec type label in
// Options["field_type"] so key extraction produces typed keys (a TIMESTAMP string becomes a
// TimestampKey, not a StringKey). Descriptors are returned in schema field order.
func DescriptorsFromSchema(namespaceID string, sch schema.KdbSchema) []Descriptor {
	var out []Descriptor
	for _, f := range sch.Fields {
		if !f.Indexed {
			continue
		}
		typ := InferIndexType(f.Type)
		out = append(out, Descriptor{
			IndexID:       DerivedIndexID(namespaceID, f.Name, typ),
			NamespaceID:   namespaceID,
			FieldName:     f.Name,
			Fields:        []string{f.Name},
			Type:          typ,
			Unique:        f.Unique,
			SchemaVersion: sch.Version,
			CreatedAtHash: sch.SchemaHash,
			Options: map[string]string{
				OptionIndexName: f.Name + "_" + strings.ToLower(typ.String()),
				OptionFieldType: f.Type.CodecTypeLabel(),
			},
		})
	}
	return out
}
