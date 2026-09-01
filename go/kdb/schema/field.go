package schema

import (
	"fmt"
	"regexp"
)

var fieldNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Field is one schema column declaration.
type Field struct {
	Name     string
	Type     FieldType
	Required bool
	Indexed  bool
	Unique   bool
}

// NewField validates invariants for a schema field.
func NewField(name string, typ FieldType, required, indexed, unique bool) (Field, error) {
	if !fieldNameRe.MatchString(name) {
		return Field{}, fmt.Errorf("field name must be a valid identifier: %s", name)
	}
	if unique && !indexed {
		return Field{}, fmt.Errorf("unique=true requires indexed=true: %s", name)
	}
	return Field{Name: name, Type: typ, Required: required, Indexed: indexed, Unique: unique}, nil
}

// MustField panics on invalid field declarations (tests only).
func MustField(name string, typ FieldType, required, indexed, unique bool) Field {
	f, err := NewField(name, typ, required, indexed, unique)
	if err != nil {
		panic(err)
	}
	return f
}

// UniqueFields returns the fields declared unique, in declaration order. Empty for the None
// schema, which by construction has no fields at all.
func (s KdbSchema) UniqueFields() []Field {
	var out []Field
	for _, f := range s.Fields {
		if f.Unique {
			out = append(out, f)
		}
	}
	return out
}

// HasUniqueFields is UniqueFields' allocation-free predicate, for the hot path that only needs
// to know whether unique enforcement has anything to do at all.
func (s KdbSchema) HasUniqueFields() bool {
	for _, f := range s.Fields {
		if f.Unique {
			return true
		}
	}
	return false
}
