package schema

// Type is an immutable type expression.
type Type interface {
	isType()
}

type Primitive struct {
	Physical PhysicalKind
	Logical  LogicalAnnotation
}

func (Primitive) isType() {}

type Ref struct{ FullyQualifiedName string }

func (Ref) isType() {}

type Nullable struct{ Inner Type }

func (Nullable) isType() {}

type Array struct{ Element Type }

func (Array) isType() {}

type Map struct{ Key, Value Type }

func (Map) isType() {}

type Union struct {
	Branches []Type
}

func (Union) isType() {}

// Prim builds a primitive without logical annotation.
func Prim(p PhysicalKind) Primitive { return Primitive{Physical: p} }
