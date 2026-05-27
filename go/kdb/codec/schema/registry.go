package schema

import (
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// FieldSchema is one record field.
type FieldSchema struct {
	ID      int
	Name    string
	Type    Type
	Default any // optional default Kdb value; nil means none
}

// RecordSchema is a named record type.
type RecordSchema struct {
	Name      string
	Namespace string
	Fields    []FieldSchema

	fieldsByID map[int]FieldSchema
}

func (r *RecordSchema) FQName() string { return r.Namespace + "." + r.Name }

func (r *RecordSchema) indexFields() {
	if r.fieldsByID != nil {
		return
	}
	r.fieldsByID = make(map[int]FieldSchema, len(r.Fields))
	for _, f := range r.Fields {
		r.fieldsByID[f.ID] = f
	}
}

func (r *RecordSchema) FieldByID(id int) (FieldSchema, bool) {
	r.indexFields()
	f, ok := r.fieldsByID[id]
	return f, ok
}

// EnumSchema is a named enum.
type EnumSchema struct {
	Name, Namespace string
	Symbols         []string
}

func (e *EnumSchema) FQName() string { return e.Namespace + "." + e.Name }

// FixedSchema is a fixed-size byte blob type.
type FixedSchema struct {
	Name, Namespace string
	Size            int
	Logical         LogicalAnnotation
}

func (f *FixedSchema) FQName() string { return f.Namespace + "." + f.Name }

// NamedSchema is any registered named type.
type NamedSchema interface {
	FQName() string
}

// Value is a placeholder for default values in field schema (codec package defines real Value).
type Value = any

// Registry holds named schemas.
type Registry struct {
	records map[string]*RecordSchema
	enums   map[string]*EnumSchema
	fixed   map[string]*FixedSchema
	frozen  bool
}

func NewRegistry() *Registry {
	return &Registry{
		records: make(map[string]*RecordSchema),
		enums:   make(map[string]*EnumSchema),
		fixed:   make(map[string]*FixedSchema),
	}
}

func BuiltinRegistry() *Registry {
	r := NewRegistry()
	r.Freeze()
	return r
}

func (r *Registry) RegisterRecord(rec *RecordSchema) {
	if r.frozen {
		panic("registry frozen")
	}
	rec.indexFields()
	r.records[rec.FQName()] = rec
}

func (r *Registry) RegisterEnum(e *EnumSchema) {
	if r.frozen {
		panic("registry frozen")
	}
	r.enums[e.FQName()] = e
}

func (r *Registry) RegisterFixed(f *FixedSchema) {
	if r.frozen {
		panic("registry frozen")
	}
	r.fixed[f.FQName()] = f
}

func (r *Registry) Freeze() { r.frozen = true }

func (r *Registry) Resolve(fq string) (NamedSchema, error) {
	if rec, ok := r.records[fq]; ok {
		return rec, nil
	}
	if e, ok := r.enums[fq]; ok {
		return e, nil
	}
	if f, ok := r.fixed[fq]; ok {
		return f, nil
	}
	return nil, kdberr.NewSchemaError("unknown type "+fq, nil)
}
