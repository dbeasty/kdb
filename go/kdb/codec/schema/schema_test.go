package schema

import (
	"strings"
	"testing"
)

// TestPhysicalTagRoundTrip round-trips every PhysicalKind through its wire tag: Tag() must map
// back to the same kind via PhysicalFromTag, covering the full Layer 0 spec §4.1 table.
func TestPhysicalTagRoundTrip(t *testing.T) {
	kinds := []PhysicalKind{
		PhysicalNull, PhysicalBool, PhysicalInt8, PhysicalInt16,
		PhysicalInt32, PhysicalInt64, PhysicalFloat32, PhysicalFloat64,
		PhysicalBytes, PhysicalString, PhysicalArray, PhysicalMap,
		PhysicalRecord, PhysicalEnum, PhysicalUnion, PhysicalFixed,
	}
	if len(kinds) != 16 {
		t.Fatalf("expected 16 physical kinds in the spec table, listed %d", len(kinds))
	}
	seen := make(map[byte]bool)
	for _, k := range kinds {
		tag := k.Tag()
		if seen[tag] {
			t.Fatalf("duplicate tag 0x%02X", tag)
		}
		seen[tag] = true
		got, ok := PhysicalFromTag(tag)
		if !ok {
			t.Fatalf("PhysicalFromTag(0x%02X): unexpectedly unknown", tag)
		}
		if got != k {
			t.Fatalf("PhysicalFromTag(0x%02X): got %v, want %v", tag, got, k)
		}
	}
}

// TestPhysicalFromTagUnknown confirms tags outside the spec table are reported cleanly via
// ok=false - decoding corrupt input must not panic or silently map to a valid kind.
func TestPhysicalFromTagUnknown(t *testing.T) {
	for _, tag := range []byte{0x10, 0x42, 0xFF} {
		if k, ok := PhysicalFromTag(tag); ok {
			t.Fatalf("PhysicalFromTag(0x%02X): expected unknown, got %v", tag, k)
		}
	}
}

// TestRegistryRegisterAndResolve registers one of each named schema kind and resolves each back
// by fully-qualified name.
func TestRegistryRegisterAndResolve(t *testing.T) {
	r := NewRegistry()
	rec := &RecordSchema{
		Name:      "User",
		Namespace: "app",
		Fields: []FieldSchema{
			{ID: 1, Name: "id", Type: Prim(PhysicalInt64)},
			{ID: 2, Name: "name", Type: Nullable{Inner: Prim(PhysicalString)}},
			{ID: 3, Name: "tags", Type: Array{Element: Prim(PhysicalString)}},
		},
	}
	enum := &EnumSchema{Name: "Color", Namespace: "app", Symbols: []string{"RED", "GREEN"}}
	fixed := &FixedSchema{Name: "Md5", Namespace: "app", Size: 16}

	r.RegisterRecord(rec)
	r.RegisterEnum(enum)
	r.RegisterFixed(fixed)

	got, err := r.Resolve("app.User")
	if err != nil {
		t.Fatalf("Resolve(app.User): %v", err)
	}
	if got != NamedSchema(rec) {
		t.Fatalf("Resolve(app.User): got %v, want the registered record", got)
	}
	if got, err := r.Resolve("app.Color"); err != nil || got != NamedSchema(enum) {
		t.Fatalf("Resolve(app.Color): got %v, %v", got, err)
	}
	if got, err := r.Resolve("app.Md5"); err != nil || got != NamedSchema(fixed) {
		t.Fatalf("Resolve(app.Md5): got %v, %v", got, err)
	}
}

// TestRegistryResolveUnknownReturnsError confirms an unknown fully-qualified name resolves to a
// descriptive error - not a panic, and not a nil schema with a nil error.
func TestRegistryResolveUnknownReturnsError(t *testing.T) {
	r := NewRegistry()
	got, err := r.Resolve("nowhere.Missing")
	if err == nil {
		t.Fatal("expected an error for an unknown type")
	}
	if got != nil {
		t.Fatalf("expected nil schema with the error, got %v", got)
	}
	if !strings.Contains(err.Error(), "nowhere.Missing") {
		t.Fatalf("error should name the unresolved type, got: %v", err)
	}
}

// TestRecordSchemaFieldByID covers field lookup by ID, including a miss, and the empty record
// schema (zero fields) as the degenerate case.
func TestRecordSchemaFieldByID(t *testing.T) {
	rec := &RecordSchema{
		Name:      "Point",
		Namespace: "geo",
		Fields: []FieldSchema{
			{ID: 1, Name: "x", Type: Prim(PhysicalFloat64)},
			{ID: 2, Name: "y", Type: Prim(PhysicalFloat64)},
		},
	}
	f, ok := rec.FieldByID(2)
	if !ok || f.Name != "y" {
		t.Fatalf("FieldByID(2): got %+v, ok=%v; want field y", f, ok)
	}
	if _, ok := rec.FieldByID(99); ok {
		t.Fatal("FieldByID(99): expected a miss for an unknown field ID")
	}

	empty := &RecordSchema{Name: "Empty", Namespace: "app"}
	if _, ok := empty.FieldByID(1); ok {
		t.Fatal("empty record schema: FieldByID must miss for every ID")
	}
	if got := empty.FQName(); got != "app.Empty" {
		t.Fatalf("empty record schema FQName: got %q, want %q", got, "app.Empty")
	}
}

// TestEmptyRecordSchemaRegisters confirms an empty record (no fields) registers and resolves
// like any other - an empty schema is valid, not an error.
func TestEmptyRecordSchemaRegisters(t *testing.T) {
	r := NewRegistry()
	empty := &RecordSchema{Name: "Empty", Namespace: "app"}
	r.RegisterRecord(empty)
	got, err := r.Resolve("app.Empty")
	if err != nil {
		t.Fatalf("Resolve(app.Empty): %v", err)
	}
	if got != NamedSchema(empty) {
		t.Fatalf("Resolve(app.Empty): got %v, want the registered empty record", got)
	}
}

// TestFrozenRegistryPanicsOnRegister documents Freeze's contract: all three Register methods
// panic once the registry is frozen, while Resolve keeps working.
func TestFrozenRegistryPanicsOnRegister(t *testing.T) {
	r := NewRegistry()
	rec := &RecordSchema{Name: "Before", Namespace: "app"}
	r.RegisterRecord(rec)
	r.Freeze()

	mustPanic := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s on a frozen registry: expected panic", name)
			}
		}()
		fn()
	}
	mustPanic("RegisterRecord", func() { r.RegisterRecord(&RecordSchema{Name: "R", Namespace: "app"}) })
	mustPanic("RegisterEnum", func() { r.RegisterEnum(&EnumSchema{Name: "E", Namespace: "app"}) })
	mustPanic("RegisterFixed", func() { r.RegisterFixed(&FixedSchema{Name: "F", Namespace: "app", Size: 4}) })

	// Pre-freeze registrations stay resolvable.
	if _, err := r.Resolve("app.Before"); err != nil {
		t.Fatalf("Resolve after Freeze: %v", err)
	}
}

// TestBuiltinRegistryIsFrozenAndEmpty pins BuiltinRegistry's current contract: it starts empty
// and comes back frozen.
func TestBuiltinRegistryIsFrozenAndEmpty(t *testing.T) {
	r := BuiltinRegistry()
	if _, err := r.Resolve("app.Anything"); err == nil {
		t.Fatal("BuiltinRegistry: expected empty registry to resolve nothing")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("BuiltinRegistry: expected RegisterRecord to panic on the frozen registry")
		}
	}()
	r.RegisterRecord(&RecordSchema{Name: "X", Namespace: "app"})
}

// TestTypeComposition builds a representative nested type expression and confirms the structure
// survives composition - the closest thing to a round trip for this package, whose Type values
// are plain immutable structs.
func TestTypeComposition(t *testing.T) {
	tz := "UTC"
	typ := Map{
		Key: Prim(PhysicalString),
		Value: Nullable{
			Inner: Array{
				Element: Union{Branches: []Type{
					Primitive{Physical: PhysicalInt64, Logical: LogicalTimestampMicros{Timezone: &tz}},
					Ref{FullyQualifiedName: "app.User"},
				}},
			},
		},
	}

	if p, ok := typ.Key.(Primitive); !ok || p.Physical != PhysicalString {
		t.Fatalf("Map key: got %#v, want string primitive", typ.Key)
	}
	arr, ok := typ.Value.(Nullable).Inner.(Array)
	if !ok {
		t.Fatalf("Map value: expected Nullable(Array(...)), got %#v", typ.Value)
	}
	union, ok := arr.Element.(Union)
	if !ok || len(union.Branches) != 2 {
		t.Fatalf("array element: expected a 2-branch union, got %#v", arr.Element)
	}
	prim, ok := union.Branches[0].(Primitive)
	if !ok || prim.Physical != PhysicalInt64 {
		t.Fatalf("union branch 0: got %#v, want int64 primitive", union.Branches[0])
	}
	if ts, ok := prim.Logical.(LogicalTimestampMicros); !ok || ts.Timezone == nil || *ts.Timezone != "UTC" {
		t.Fatalf("union branch 0 logical: got %#v, want timestamp-micros with UTC timezone", prim.Logical)
	}
	if ref, ok := union.Branches[1].(Ref); !ok || ref.FullyQualifiedName != "app.User" {
		t.Fatalf("union branch 1: got %#v, want Ref(app.User)", union.Branches[1])
	}
}
