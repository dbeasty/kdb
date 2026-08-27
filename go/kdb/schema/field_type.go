package schema

// FieldType is the declared SQL / codec projection for one schema field.
type FieldType interface {
	isFieldType()
	SQLTypeName() string
	CodecTypeLabel() string
}

type (
	StringType    struct{}
	Int32Type     struct{}
	Int64Type     struct{}
	Float64Type   struct{}
	BoolType      struct{}
	TimestampType struct{}
	UUIDType      struct{}
	ObjectType    struct{}
	ArrayType     struct{}
)

func (StringType) isFieldType()    {}
func (Int32Type) isFieldType()     {}
func (Int64Type) isFieldType()     {}
func (Float64Type) isFieldType()   {}
func (BoolType) isFieldType()      {}
func (TimestampType) isFieldType() {}
func (UUIDType) isFieldType()      {}
func (ObjectType) isFieldType()    {}
func (ArrayType) isFieldType()     {}

func (StringType) SQLTypeName() string    { return "TEXT" }
func (Int32Type) SQLTypeName() string     { return "INTEGER" }
func (Int64Type) SQLTypeName() string     { return "BIGINT" }
func (Float64Type) SQLTypeName() string   { return "REAL" }
func (BoolType) SQLTypeName() string      { return "BOOLEAN" }
func (TimestampType) SQLTypeName() string { return "TIMESTAMP" }
func (UUIDType) SQLTypeName() string      { return "TEXT" }
func (ObjectType) SQLTypeName() string    { return "JSON" }
func (ArrayType) SQLTypeName() string     { return "JSON" }

func (StringType) CodecTypeLabel() string    { return "STRING" }
func (Int32Type) CodecTypeLabel() string     { return "INT32" }
func (Int64Type) CodecTypeLabel() string     { return "INT64" }
func (Float64Type) CodecTypeLabel() string   { return "FLOAT64" }
func (BoolType) CodecTypeLabel() string      { return "BOOLEAN" }
func (TimestampType) CodecTypeLabel() string { return "TIMESTAMP" }
func (UUIDType) CodecTypeLabel() string      { return "UUID" }
func (ObjectType) CodecTypeLabel() string    { return "JSON_OBJECT" }
func (ArrayType) CodecTypeLabel() string     { return "JSON_ARRAY" }

// EnumType is a string enum field type.
type EnumType struct {
	Values map[string]struct{}
}

func (EnumType) isFieldType()             {}
func (e EnumType) SQLTypeName() string    { return "TEXT" }
func (e EnumType) CodecTypeLabel() string { return "ENUM_AS_STRING" }

// NewEnumType builds an enum with at least one value.
func NewEnumType(values ...string) EnumType {
	if len(values) == 0 {
		panic("EnumType must have at least one value")
	}
	m := make(map[string]struct{}, len(values))
	for _, v := range values {
		m[v] = struct{}{}
	}
	return EnumType{Values: m}
}

func sortedEnumValues(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for v := range m {
		out = append(out, v)
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
