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
