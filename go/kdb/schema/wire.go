package schema

import (
	"fmt"
	"sync"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/codec/schema"
	"github.com/limidus/kdb/go/kdb/document"
)

const wireNS = "dev.kdb.schema"

var (
	fqnHash32       = wireNS + ".Hash32"
	fqnSchemaField  = wireNS + ".SchemaFieldWire"
	fqnSchemaBody   = wireNS + ".KdbSchemaBody"
	fqnSchemaMigr   = wireNS + ".SchemaMigrationWire"
	fqnEnumPayload  = wireNS + ".EnumValuesPayload"
	fqnFKString     = wireNS + ".FieldKindString"
	fqnFKInt32      = wireNS + ".FieldKindInt32"
	fqnFKInt64      = wireNS + ".FieldKindInt64"
	fqnFKFloat64    = wireNS + ".FieldKindFloat64"
	fqnFKBool       = wireNS + ".FieldKindBool"
	fqnFKTimestamp  = wireNS + ".FieldKindTimestamp"
	fqnFKUUID       = wireNS + ".FieldKindUuid"
	fqnFKObject     = wireNS + ".FieldKindObject"
	fqnFKArray      = wireNS + ".FieldKindArray"
	fqnMSAddField   = wireNS + ".MigrationAddField"
	fqnMSDropField  = wireNS + ".MigrationDropField"
	fqnMSRename     = wireNS + ".MigrationRenameField"
	fqnMSChangeType = wireNS + ".MigrationChangeType"
	fqnMSAddIndex   = wireNS + ".MigrationAddIndex"
	fqnMSDropIndex  = wireNS + ".MigrationDropIndex"
	fqnMSSetReq     = wireNS + ".MigrationSetRequired"
	fqnMSSetUnique  = wireNS + ".MigrationSetUnique"
	fqnMSWidenEnum  = wireNS + ".MigrationWidenEnum"
	fqnMSNarrowEnum = wireNS + ".MigrationNarrowEnum"
)

var (
	wireOnce sync.Once
	wireReg  *schema.Registry
)

var tsPrim = schema.Primitive{Physical: schema.PhysicalInt64, Logical: schema.LogicalTimestampMicros{}}
var uuidPrim = schema.Primitive{Physical: schema.PhysicalFixed, Logical: schema.LogicalUUID{}}

// WireRegistry returns builtin schemas for dev.kdb.schema wire shapes.
func WireRegistry() *schema.Registry {
	wireOnce.Do(func() {
		r := schema.NewRegistry()
		r.RegisterFixed(&schema.FixedSchema{Name: "Hash32", Namespace: wireNS, Size: 32})
		emptyRecords := []string{
			"FieldKindString", "FieldKindInt32", "FieldKindInt64", "FieldKindFloat64",
			"FieldKindBool", "FieldKindTimestamp", "FieldKindUuid", "FieldKindObject", "FieldKindArray",
		}
		for _, name := range emptyRecords {
			r.RegisterRecord(&schema.RecordSchema{Name: name, Namespace: wireNS})
		}
		r.RegisterRecord(&schema.RecordSchema{
			Name: "EnumValuesPayload", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "values", Type: schema.Array{Element: schema.Prim(schema.PhysicalString)}},
			},
		})
		fieldTypeUnion := schema.Union{Branches: []schema.Type{
			schema.Ref{FullyQualifiedName: fqnFKString},
			schema.Ref{FullyQualifiedName: fqnFKInt32},
			schema.Ref{FullyQualifiedName: fqnFKInt64},
			schema.Ref{FullyQualifiedName: fqnFKFloat64},
			schema.Ref{FullyQualifiedName: fqnFKBool},
			schema.Ref{FullyQualifiedName: fqnFKTimestamp},
			schema.Ref{FullyQualifiedName: fqnFKUUID},
			schema.Ref{FullyQualifiedName: fqnFKObject},
			schema.Ref{FullyQualifiedName: fqnFKArray},
			schema.Ref{FullyQualifiedName: fqnEnumPayload},
		}}
		r.RegisterRecord(&schema.RecordSchema{
			Name: "SchemaFieldWire", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "name", Type: schema.Prim(schema.PhysicalString)},
				{ID: 2, Name: "type", Type: fieldTypeUnion},
				{ID: 3, Name: "required", Type: schema.Prim(schema.PhysicalBool)},
				{ID: 4, Name: "indexed", Type: schema.Prim(schema.PhysicalBool)},
				{ID: 5, Name: "unique", Type: schema.Prim(schema.PhysicalBool)},
			},
		})
		r.RegisterRecord(&schema.RecordSchema{
			Name: "KdbSchemaBody", Namespace: wireNS,
			Fields: []schema.FieldSchema{
				{ID: 1, Name: "fields", Type: schema.Array{Element: schema.Ref{FullyQualifiedName: fqnSchemaField}}},
				{ID: 2, Name: "version", Type: schema.Prim(schema.PhysicalInt32)},
				{ID: 3, Name: "createdAt", Type: tsPrim},
				{ID: 4, Name: "description", Type: schema.Prim(schema.PhysicalString)},
			},
		})
		registerMigrationRecords(r, fieldTypeUnion)
		wireReg = r
		wireReg.Freeze()
	})
	return wireReg
}

func registerMigrationRecords(r *schema.Registry, fieldTypeUnion schema.Union) {
	ms := []struct {
		name   string
		fields []schema.FieldSchema
	}{
		{"MigrationAddField", []schema.FieldSchema{{ID: 1, Name: "field", Type: schema.Ref{FullyQualifiedName: fqnSchemaField}}}},
		{"MigrationDropField", []schema.FieldSchema{{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)}}},
		{"MigrationRenameField", []schema.FieldSchema{
			{ID: 1, Name: "oldName", Type: schema.Prim(schema.PhysicalString)},
			{ID: 2, Name: "newName", Type: schema.Prim(schema.PhysicalString)},
		}},
		{"MigrationChangeType", []schema.FieldSchema{
			{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)},
			{ID: 2, Name: "newType", Type: fieldTypeUnion},
		}},
		{"MigrationAddIndex", []schema.FieldSchema{{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)}}},
		{"MigrationDropIndex", []schema.FieldSchema{{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)}}},
		{"MigrationSetRequired", []schema.FieldSchema{
			{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)},
			{ID: 2, Name: "required", Type: schema.Prim(schema.PhysicalBool)},
		}},
		{"MigrationSetUnique", []schema.FieldSchema{
			{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)},
			{ID: 2, Name: "unique", Type: schema.Prim(schema.PhysicalBool)},
		}},
		{"MigrationWidenEnum", []schema.FieldSchema{
			{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)},
			{ID: 2, Name: "addValues", Type: schema.Array{Element: schema.Prim(schema.PhysicalString)}},
		}},
		{"MigrationNarrowEnum", []schema.FieldSchema{
			{ID: 1, Name: "fieldName", Type: schema.Prim(schema.PhysicalString)},
			{ID: 2, Name: "removeValues", Type: schema.Array{Element: schema.Prim(schema.PhysicalString)}},
		}},
	}
	for _, m := range ms {
		r.RegisterRecord(&schema.RecordSchema{Name: m.name, Namespace: wireNS, Fields: m.fields})
	}
	stepUnion := schema.Union{Branches: []schema.Type{
		schema.Ref{FullyQualifiedName: fqnMSAddField},
		schema.Ref{FullyQualifiedName: fqnMSDropField},
		schema.Ref{FullyQualifiedName: fqnMSRename},
		schema.Ref{FullyQualifiedName: fqnMSChangeType},
		schema.Ref{FullyQualifiedName: fqnMSAddIndex},
		schema.Ref{FullyQualifiedName: fqnMSDropIndex},
		schema.Ref{FullyQualifiedName: fqnMSSetReq},
		schema.Ref{FullyQualifiedName: fqnMSSetUnique},
		schema.Ref{FullyQualifiedName: fqnMSWidenEnum},
		schema.Ref{FullyQualifiedName: fqnMSNarrowEnum},
	}}
	r.RegisterRecord(&schema.RecordSchema{
		Name: "SchemaMigrationWire", Namespace: wireNS,
		Fields: []schema.FieldSchema{
			{ID: 1, Name: "migrationId", Type: uuidPrim},
			{ID: 2, Name: "fromVersion", Type: schema.Prim(schema.PhysicalInt32)},
			{ID: 3, Name: "toVersion", Type: schema.Prim(schema.PhysicalInt32)},
			{ID: 4, Name: "steps", Type: schema.Array{Element: stepUnion}},
			{ID: 5, Name: "description", Type: schema.Prim(schema.PhysicalString)},
		},
	})
}

// BodyWireType is the root schema snapshot wire type.
func BodyWireType() schema.Ref { return schema.Ref{FullyQualifiedName: fqnSchemaBody} }

// MigrationWireType is the wire type for SchemaMigration.
func MigrationWireType() schema.Ref { return schema.Ref{FullyQualifiedName: fqnSchemaMigr} }

func schemaBodyValue(fields []Field, version int, createdAt codec.Timestamp, description string) codec.RecordValue {
	els := make([]codec.Value, len(fields))
	for i, f := range fields {
		els[i] = schemaFieldWireRecord(f)
	}
	return codec.RecordValue{Fields: map[int]codec.Value{
		1: codec.ArrayValue{Elements: els},
		2: codec.Int32Value{V: int32(version)},
		3: codec.TimestampValue{EpochMicros: createdAt.EpochMicros()},
		4: codec.StringValue{V: description},
	}}
}

func schemaFieldWireRecord(f Field) codec.RecordValue {
	return codec.RecordValue{Fields: map[int]codec.Value{
		1: codec.StringValue{V: f.Name},
		2: fieldTypeToWireUnion(f.Type),
		3: codec.BoolValue{V: f.Required},
		4: codec.BoolValue{V: f.Indexed},
		5: codec.BoolValue{V: f.Unique},
	}}
}

func fieldTypeToWireUnion(t FieldType) codec.UnionValue {
	switch t := t.(type) {
	case StringType:
		return codec.UnionValue{Branch: 0, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case Int32Type:
		return codec.UnionValue{Branch: 1, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case Int64Type:
		return codec.UnionValue{Branch: 2, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case Float64Type:
		return codec.UnionValue{Branch: 3, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case BoolType:
		return codec.UnionValue{Branch: 4, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case TimestampType:
		return codec.UnionValue{Branch: 5, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case UUIDType:
		return codec.UnionValue{Branch: 6, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case ObjectType:
		return codec.UnionValue{Branch: 7, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case ArrayType:
		return codec.UnionValue{Branch: 8, Inner: codec.RecordValue{Fields: map[int]codec.Value{}}}
	case EnumType:
		vals := sortedEnumValues(t.Values)
		syms := make([]codec.Value, len(vals))
		for i, v := range vals {
			syms[i] = codec.StringValue{V: v}
		}
		return codec.UnionValue{Branch: 9, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.ArrayValue{Elements: syms},
		}}}
	default:
		panic("unknown field type")
	}
}

func parseFieldTypeWire(uv codec.UnionValue) (FieldType, error) {
	switch uv.Branch {
	case 0:
		return StringType{}, nil
	case 1:
		return Int32Type{}, nil
	case 2:
		return Int64Type{}, nil
	case 3:
		return Float64Type{}, nil
	case 4:
		return BoolType{}, nil
	case 5:
		return TimestampType{}, nil
	case 6:
		return UUIDType{}, nil
	case 7:
		return ObjectType{}, nil
	case 8:
		return ArrayType{}, nil
	case 9:
		rec, ok := uv.Inner.(codec.RecordValue)
		if !ok {
			return nil, newDecodeError("EnumValuesPayload expected record", nil)
		}
		arr, ok := rec.Fields[1].(codec.ArrayValue)
		if !ok {
			return nil, newDecodeError("enum values array", nil)
		}
		vals := make([]string, 0, len(arr.Elements))
		for _, el := range arr.Elements {
			s, ok := el.(codec.StringValue)
			if !ok {
				return nil, newDecodeError("enum symbol", nil)
			}
			vals = append(vals, s.V)
		}
		if len(vals) == 0 {
			return nil, newDecodeError("enum requires symbols", nil)
		}
		return NewEnumType(vals...), nil
	default:
		return nil, newDecodeError("unknown field type union branch", nil)
	}
}

func parseSchemaFieldWire(rec codec.RecordValue) (Field, error) {
	name, _ := rec.Fields[1].(codec.StringValue)
	uv, ok := rec.Fields[2].(codec.UnionValue)
	if !ok {
		return Field{}, newDecodeError("schema field type", nil)
	}
	typ, err := parseFieldTypeWire(uv)
	if err != nil {
		return Field{}, err
	}
	req, _ := rec.Fields[3].(codec.BoolValue)
	ix, _ := rec.Fields[4].(codec.BoolValue)
	uq, _ := rec.Fields[5].(codec.BoolValue)
	return NewField(name.V, typ, req.V, ix.V, uq.V)
}

func parseSchemaBodyRecord(rec codec.RecordValue) (KdbSchema, error) {
	arr, ok := rec.Fields[1].(codec.ArrayValue)
	if !ok {
		return KdbSchema{}, newDecodeError("schema fields array", nil)
	}
	fields := make([]Field, 0, len(arr.Elements))
	seen := make(map[string]struct{})
	for _, el := range arr.Elements {
		r, ok := el.(codec.RecordValue)
		if !ok {
			return KdbSchema{}, newDecodeError("schema field record", nil)
		}
		f, err := parseSchemaFieldWire(r)
		if err != nil {
			return KdbSchema{}, err
		}
		if _, dup := seen[f.Name]; dup {
			return KdbSchema{}, fmt.Errorf("duplicate schema field names")
		}
		seen[f.Name] = struct{}{}
		fields = append(fields, f)
	}
	ver, _ := rec.Fields[2].(codec.Int32Value)
	ts, ok := rec.Fields[3].(codec.TimestampValue)
	if !ok {
		return KdbSchema{}, newDecodeError("createdAt", nil)
	}
	desc, _ := rec.Fields[4].(codec.StringValue)
	createdAt := codec.TimestampFromEpochMicros(ts.EpochMicros)
	body := schemaBodyValue(fields, int(ver.V), createdAt, desc.V)
	bytes, err := codec.EncodeBytes(body, BodyWireType(), WireRegistry())
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
		Fields:       fields,
		Version:      int(ver.V),
		CreatedAt:    createdAt,
		Description:  desc.V,
		fieldsByName: byName,
	}, nil
}

func migrationStepToWire(step MigrationStep) codec.UnionValue {
	switch s := step.(type) {
	case AddFieldStep:
		return codec.UnionValue{Branch: 0, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: schemaFieldWireRecord(s.Field),
		}}}
	case DropFieldStep:
		return codec.UnionValue{Branch: 1, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
		}}}
	case RenameFieldStep:
		return codec.UnionValue{Branch: 2, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.OldName},
			2: codec.StringValue{V: s.NewName},
		}}}
	case ChangeTypeStep:
		return codec.UnionValue{Branch: 3, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
			2: fieldTypeToWireUnion(s.NewType),
		}}}
	case AddIndexStep:
		return codec.UnionValue{Branch: 4, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
		}}}
	case DropIndexStep:
		return codec.UnionValue{Branch: 5, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
		}}}
	case SetRequiredStep:
		return codec.UnionValue{Branch: 6, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
			2: codec.BoolValue{V: s.Required},
		}}}
	case SetUniqueStep:
		return codec.UnionValue{Branch: 7, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
			2: codec.BoolValue{V: s.Unique},
		}}}
	case WidenEnumStep:
		return codec.UnionValue{Branch: 8, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
			2: codec.ArrayValue{Elements: stringsToWireValues(sortedStrings(s.AddValues))},
		}}}
	case NarrowEnumStep:
		return codec.UnionValue{Branch: 9, Inner: codec.RecordValue{Fields: map[int]codec.Value{
			1: codec.StringValue{V: s.FieldName},
			2: codec.ArrayValue{Elements: stringsToWireValues(sortedStrings(s.RemoveValues))},
		}}}
	default:
		panic("unknown migration step")
	}
}

func stringsToWireValues(ss []string) []codec.Value {
	out := make([]codec.Value, len(ss))
	for i, s := range ss {
		out[i] = codec.StringValue{V: s}
	}
	return out
}

func sortedStrings(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for s := range m {
		out = append(out, s)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func parseMigrationStepWire(uv codec.UnionValue) (MigrationStep, error) {
	rec, ok := uv.Inner.(codec.RecordValue)
	if !ok {
		return nil, newDecodeError("migration step payload", nil)
	}
	switch uv.Branch {
	case 0:
		frec, _ := rec.Fields[1].(codec.RecordValue)
		f, err := parseSchemaFieldWire(frec)
		if err != nil {
			return nil, err
		}
		return AddFieldStep{Field: f}, nil
	case 1:
		n, _ := rec.Fields[1].(codec.StringValue)
		return DropFieldStep{FieldName: n.V}, nil
	case 2:
		o, _ := rec.Fields[1].(codec.StringValue)
		nn, _ := rec.Fields[2].(codec.StringValue)
		return RenameFieldStep{OldName: o.V, NewName: nn.V}, nil
	case 3:
		n, _ := rec.Fields[1].(codec.StringValue)
		tuv, _ := rec.Fields[2].(codec.UnionValue)
		nt, err := parseFieldTypeWire(tuv)
		if err != nil {
			return nil, err
		}
		return ChangeTypeStep{FieldName: n.V, NewType: nt}, nil
	case 4:
		n, _ := rec.Fields[1].(codec.StringValue)
		return AddIndexStep{FieldName: n.V}, nil
	case 5:
		n, _ := rec.Fields[1].(codec.StringValue)
		return DropIndexStep{FieldName: n.V}, nil
	case 6:
		n, _ := rec.Fields[1].(codec.StringValue)
		r, _ := rec.Fields[2].(codec.BoolValue)
		return SetRequiredStep{FieldName: n.V, Required: r.V}, nil
	case 7:
		n, _ := rec.Fields[1].(codec.StringValue)
		u, _ := rec.Fields[2].(codec.BoolValue)
		return SetUniqueStep{FieldName: n.V, Unique: u.V}, nil
	case 8:
		n, _ := rec.Fields[1].(codec.StringValue)
		add, err := readStringArray(rec.Fields[2])
		if err != nil {
			return nil, err
		}
		m := make(map[string]struct{}, len(add))
		for _, v := range add {
			m[v] = struct{}{}
		}
		return WidenEnumStep{FieldName: n.V, AddValues: m}, nil
	case 9:
		n, _ := rec.Fields[1].(codec.StringValue)
		rem, err := readStringArray(rec.Fields[2])
		if err != nil {
			return nil, err
		}
		m := make(map[string]struct{}, len(rem))
		for _, v := range rem {
			m[v] = struct{}{}
		}
		return NarrowEnumStep{FieldName: n.V, RemoveValues: m}, nil
	default:
		return nil, newDecodeError("unknown migration branch", nil)
	}
}

func readStringArray(v codec.Value) ([]string, error) {
	arr, ok := v.(codec.ArrayValue)
	if !ok {
		return nil, newDecodeError("expected string array", nil)
	}
	out := make([]string, len(arr.Elements))
	for i, el := range arr.Elements {
		s, ok := el.(codec.StringValue)
		if !ok {
			return nil, newDecodeError("string array element", nil)
		}
		out[i] = s.V
	}
	return out, nil
}

func schemaMigrationRecord(m SchemaMigration) codec.RecordValue {
	steps := make([]codec.Value, len(m.Steps))
	for i, s := range m.Steps {
		steps[i] = migrationStepToWire(s)
	}
	return codec.RecordValue{Fields: map[int]codec.Value{
		1: codec.UUIDValue{MSB: m.MigrationID.MSB, LSB: m.MigrationID.LSB},
		2: codec.Int32Value{V: int32(m.FromVersion)},
		3: codec.Int32Value{V: int32(m.ToVersion)},
		4: codec.ArrayValue{Elements: steps},
		5: codec.StringValue{V: m.Description},
	}}
}

func parseSchemaMigrationRecord(rec codec.RecordValue) (SchemaMigration, error) {
	u, ok := rec.Fields[1].(codec.UUIDValue)
	if !ok {
		return SchemaMigration{}, newDecodeError("migration id", nil)
	}
	from, _ := rec.Fields[2].(codec.Int32Value)
	to, _ := rec.Fields[3].(codec.Int32Value)
	arr, ok := rec.Fields[4].(codec.ArrayValue)
	if !ok {
		return SchemaMigration{}, newDecodeError("migration steps", nil)
	}
	steps := make([]MigrationStep, len(arr.Elements))
	for i, el := range arr.Elements {
		uv, ok := el.(codec.UnionValue)
		if !ok {
			return SchemaMigration{}, newDecodeError("step union", nil)
		}
		s, err := parseMigrationStepWire(uv)
		if err != nil {
			return SchemaMigration{}, err
		}
		steps[i] = s
	}
	desc, _ := rec.Fields[5].(codec.StringValue)
	return SchemaMigration{
		MigrationID: codec.UUID{MSB: u.MSB, LSB: u.LSB},
		FromVersion: int(from.V),
		ToVersion:   int(to.V),
		Steps:       steps,
		Description: desc.V,
	}, nil
}
