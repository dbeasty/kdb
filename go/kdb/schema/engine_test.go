package schema

import (
	"math"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// DiffSchemas, IsBreaking, ApplyMigration, IsBackwardCompatible and CheckFieldValue had no
// tests. They are what decides whether a schema change is allowed to land and whether a
// document is allowed to be written, so a wrong answer here is accepted bad data.

func mustField(t *testing.T, name string, typ FieldType, required, indexed, unique bool) Field {
	t.Helper()
	f, err := NewField(name, typ, required, indexed, unique)
	if err != nil {
		t.Fatalf("NewField(%s): %v", name, err)
	}
	return f
}

func mustSchema(t *testing.T, version int, fields ...Field) KdbSchema {
	t.Helper()
	s, err := Build(fields, version, codec.TimestampFromEpochMicros(1_700_000_000_000_000), "test")
	if err != nil {
		t.Fatalf("Build(v%d): %v", version, err)
	}
	return s
}

func enumOf(values ...string) EnumType {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return EnumType{Values: set}
}

// ---------------------------------------------------------------- CheckFieldValue

func TestCheckFieldValueAcceptsMatchingTypes(t *testing.T) {
	cases := []struct {
		name  string
		typ   FieldType
		value any
	}{
		{"string", StringType{}, "hello"},
		{"int32", Int32Type{}, float64(42)},
		{"int32 at max", Int32Type{}, float64(math.MaxInt32)},
		{"int32 at min", Int32Type{}, float64(math.MinInt32)},
		{"int64", Int64Type{}, float64(1 << 40)},
		{"float64", Float64Type{}, 1.5},
		{"float64 integral", Float64Type{}, float64(3)},
		{"bool", BoolType{}, true},
		{"timestamp", TimestampType{}, "2026-08-27T12:34:56Z"},
		{"timestamp with micros", TimestampType{}, "2026-08-27T12:34:56.123456Z"},
		{"uuid", UUIDType{}, "11111111-2222-4333-8444-555555555555"},
		{"object", ObjectType{}, map[string]any{"k": "v"}},
		{"array", ArrayType{}, []any{1.0, 2.0}},
		{"enum", enumOf("red", "green"), "red"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := Field{Name: "f", Type: tc.typ}
			if v := CheckFieldValue(f, tc.value); v != nil {
				t.Fatalf("rejected a valid value: %+v", *v)
			}
		})
	}
}

func TestCheckFieldValueRejectsMismatchedTypes(t *testing.T) {
	cases := []struct {
		name  string
		typ   FieldType
		value any
	}{
		{"string given number", StringType{}, 1.0},
		{"int32 given string", Int32Type{}, "1"},
		{"int32 given fraction", Int32Type{}, 1.5},
		{"int32 above range", Int32Type{}, float64(math.MaxInt32) + 1},
		{"int32 below range", Int32Type{}, float64(math.MinInt32) - 1},
		{"int64 given fraction", Int64Type{}, 1.5},
		{"float64 given bool", Float64Type{}, true},
		{"bool given string", BoolType{}, "true"},
		{"timestamp given number", TimestampType{}, 1.0},
		{"timestamp not iso8601", TimestampType{}, "27 August 2026"},
		{"uuid given number", UUIDType{}, 1.0},
		{"uuid malformed", UUIDType{}, "not-a-uuid"},
		{"object given array", ObjectType{}, []any{}},
		{"array given object", ArrayType{}, map[string]any{}},
		{"enum given number", enumOf("red"), 1.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := Field{Name: "f", Type: tc.typ}
			v := CheckFieldValue(f, tc.value)
			if v == nil {
				t.Fatalf("accepted an invalid value %#v", tc.value)
			}
			if v.FieldName != "f" {
				t.Errorf("violation names field %q", v.FieldName)
			}
		})
	}
}

// Int64Type used to accept any integral float64, so a JSON number far outside int64's range
// passed validation and then could not be stored as the int64 the field declares - while
// Int32Type had always bounds-checked. The two must behave the same way at their own limits.
func TestCheckFieldValueBoundsInt64LikeInt32(t *testing.T) {
	// Stated here rather than reused from the implementation, so this test compiles - and
	// fails - against a build without the bound.
	const twoToThe63Local = float64(1 << 63) // one past math.MaxInt64, which float64 cannot hold
	f := Field{Name: "n", Type: Int64Type{}}
	for _, v := range []float64{1e30, -1e30, 9.3e18, -9.3e18, twoToThe63Local} {
		if got := CheckFieldValue(f, v); got == nil {
			t.Errorf("int64 field accepted %g, which is outside int64", v)
		}
	}
	// The largest and smallest values float64 can represent that do fit are still accepted.
	for _, v := range []float64{math.MinInt64, twoToThe63Local - 1024, 0, -1, 1 << 53} {
		if got := CheckFieldValue(f, v); got != nil {
			t.Errorf("int64 field rejected %g, which fits: %+v", v, *got)
		}
	}
}

func TestCheckFieldValueAllowsNullForAnyType(t *testing.T) {
	// Required-ness is enforced by Validate, not here; a null value is "absent", and every
	// type has to say so rather than reporting a type mismatch.
	for _, typ := range []FieldType{
		StringType{}, Int32Type{}, Int64Type{}, Float64Type{}, BoolType{},
		TimestampType{}, UUIDType{}, ObjectType{}, ArrayType{}, enumOf("a"),
	} {
		f := Field{Name: "f", Type: typ}
		if v := CheckFieldValue(f, nil); v != nil {
			t.Errorf("%T rejected a null value: %+v", typ, *v)
		}
	}
}

func TestCheckFieldValueEnumReportsUndeclaredValueDistinctly(t *testing.T) {
	f := Field{Name: "colour", Type: enumOf("red", "green")}
	v := CheckFieldValue(f, "blue")
	if v == nil {
		t.Fatal("an undeclared enum value was accepted")
	}
	// A value of the right shape but not in the enum is a different problem from a value of the
	// wrong type, and the violation says which - a caller deciding whether to widen the enum
	// needs to tell them apart.
	if v.ViolationType != kdberr.EnumValueNotDeclared {
		t.Fatalf("violation type is %v, want EnumValueNotDeclared", v.ViolationType)
	}
	if wrongType := CheckFieldValue(f, 1.0); wrongType == nil ||
		wrongType.ViolationType != kdberr.TypeMismatch {
		t.Fatalf("a non-string enum value should be a TypeMismatch, got %+v", wrongType)
	}
}

// ---------------------------------------------------------------- DiffSchemas

func TestDiffSchemasDetectsAddRemoveModify(t *testing.T) {
	from := mustSchema(t, 1,
		mustField(t, "keep", StringType{}, false, false, false),
		mustField(t, "drop", StringType{}, false, false, false),
		mustField(t, "change", StringType{}, false, false, false),
	)
	to := mustSchema(t, 2,
		mustField(t, "keep", StringType{}, false, false, false),
		mustField(t, "change", Int32Type{}, false, false, false),
		mustField(t, "new", StringType{}, false, false, false),
	)

	d := DiffSchemas(from, to)
	if d.IsEmpty() {
		t.Fatal("diff reported no changes")
	}
	if len(d.AddedFields) != 1 || d.AddedFields[0].Name != "new" {
		t.Errorf("added: %+v", d.AddedFields)
	}
	if len(d.RemovedFields) != 1 || d.RemovedFields[0].Name != "drop" {
		t.Errorf("removed: %+v", d.RemovedFields)
	}
	if len(d.ModifiedFields) != 1 || d.ModifiedFields[0].FieldName != "change" {
		t.Errorf("modified: %+v", d.ModifiedFields)
	}
	if d.FromVersion != 1 || d.ToVersion != 2 {
		t.Errorf("versions: %d -> %d", d.FromVersion, d.ToVersion)
	}
}

func TestDiffSchemasOfIdenticalSchemasIsEmpty(t *testing.T) {
	s := mustSchema(t, 1, mustField(t, "a", StringType{}, false, false, false))
	if d := DiffSchemas(s, s); !d.IsEmpty() {
		t.Fatalf("a schema differs from itself: %+v", d)
	}
}

// The three lists are built by ranging maps, so they are sorted before returning. A Diff gets
// rendered for a human, serialized, and compared against another Diff - all of which a
// shuffling order breaks.
func TestDiffSchemasIsDeterministicallyOrdered(t *testing.T) {
	from := mustSchema(t, 1,
		mustField(t, "r1", StringType{}, false, false, false),
		mustField(t, "r2", StringType{}, false, false, false),
		mustField(t, "r3", StringType{}, false, false, false),
		mustField(t, "m1", StringType{}, false, false, false),
		mustField(t, "m2", StringType{}, false, false, false),
	)
	to := mustSchema(t, 2,
		mustField(t, "a1", StringType{}, false, false, false),
		mustField(t, "a2", StringType{}, false, false, false),
		mustField(t, "a3", StringType{}, false, false, false),
		mustField(t, "m1", Int32Type{}, false, false, false),
		mustField(t, "m2", Int32Type{}, false, false, false),
	)

	first := DiffSchemas(from, to)
	for i := 0; i < 25; i++ {
		again := DiffSchemas(from, to)
		if len(again.AddedFields) != len(first.AddedFields) ||
			len(again.RemovedFields) != len(first.RemovedFields) ||
			len(again.ModifiedFields) != len(first.ModifiedFields) {
			t.Fatalf("diff sizes changed between calls")
		}
		for j := range first.AddedFields {
			if first.AddedFields[j].Name != again.AddedFields[j].Name {
				t.Fatalf("added order changed at %d: %s then %s",
					j, first.AddedFields[j].Name, again.AddedFields[j].Name)
			}
		}
		for j := range first.RemovedFields {
			if first.RemovedFields[j].Name != again.RemovedFields[j].Name {
				t.Fatalf("removed order changed at %d", j)
			}
		}
		for j := range first.ModifiedFields {
			if first.ModifiedFields[j].FieldName != again.ModifiedFields[j].FieldName {
				t.Fatalf("modified order changed at %d", j)
			}
		}
	}
	for i := 1; i < len(first.AddedFields); i++ {
		if first.AddedFields[i-1].Name >= first.AddedFields[i].Name {
			t.Fatalf("added is not sorted by name: %+v", first.AddedFields)
		}
	}
	for i := 1; i < len(first.RemovedFields); i++ {
		if first.RemovedFields[i-1].Name >= first.RemovedFields[i].Name {
			t.Fatalf("removed is not sorted by name: %+v", first.RemovedFields)
		}
	}
	for i := 1; i < len(first.ModifiedFields); i++ {
		if first.ModifiedFields[i-1].FieldName >= first.ModifiedFields[i].FieldName {
			t.Fatalf("modified is not sorted by name: %+v", first.ModifiedFields)
		}
	}
}

// ---------------------------------------------------------------- IsBreaking

func TestDiffIsBreaking(t *testing.T) {
	optional := mustField(t, "f", StringType{}, false, false, false)
	required := mustField(t, "g", StringType{}, true, false, false)

	cases := []struct {
		name string
		diff Diff
		want bool
	}{
		{"empty", Diff{}, false},
		{"added optional field", Diff{AddedFields: []Field{optional}}, false},
		{"added required field", Diff{AddedFields: []Field{required}}, true},
		{"removed field", Diff{RemovedFields: []Field{optional}}, true},
		{"type changed", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{TypeChanged{From: StringType{}, To: Int32Type{}}}}}}, true},
		{"became required", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{RequiredChanged{From: false, To: true}}}}}, true},
		{"stopped being required", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{RequiredChanged{From: true, To: false}}}}}, false},
		{"became unique", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{UniqueChanged{From: false, To: true}}}}}, true},
		{"stopped being unique", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{UniqueChanged{From: true, To: false}}}}}, false},
		{"indexing changed either way", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{IndexedChanged{From: false, To: true}}}}}, false},
		{"enum widened", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{EnumValuesChanged{Added: map[string]struct{}{"blue": {}}}}}}}, false},
		{"enum narrowed", Diff{ModifiedFields: []FieldDiff{{FieldName: "f",
			Changes: []FieldChange{EnumValuesChanged{Removed: map[string]struct{}{"red": {}}}}}}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.diff.IsBreaking(); got != tc.want {
				t.Fatalf("IsBreaking() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMigrationStepIsBreaking(t *testing.T) {
	cases := []struct {
		name string
		step MigrationStep
		want bool
	}{
		{"add optional field", AddFieldStep{Field: Field{Name: "f", Type: StringType{}}}, false},
		{"add required field", AddFieldStep{Field: Field{Name: "f", Type: StringType{}, Required: true}}, true},
		{"drop field", DropFieldStep{FieldName: "f"}, true},
		{"rename field", RenameFieldStep{OldName: "f", NewName: "g"}, true},
		{"change type", ChangeTypeStep{FieldName: "f", NewType: Int32Type{}}, true},
		{"add index", AddIndexStep{FieldName: "f"}, false},
		{"drop index", DropIndexStep{FieldName: "f"}, false},
		{"set required", SetRequiredStep{FieldName: "f", Required: true}, true},
		{"clear required", SetRequiredStep{FieldName: "f", Required: false}, false},
		{"set unique", SetUniqueStep{FieldName: "f", Unique: true}, true},
		{"clear unique", SetUniqueStep{FieldName: "f", Unique: false}, false},
		{"widen enum", WidenEnumStep{FieldName: "f", AddValues: map[string]struct{}{"b": {}}}, false},
		{"narrow enum", NarrowEnumStep{FieldName: "f", RemoveValues: map[string]struct{}{"a": {}}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsBreaking(tc.step); got != tc.want {
				t.Fatalf("IsBreaking(%T) = %v, want %v", tc.step, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------- ApplyMigration

func TestApplyMigrationAddsAField(t *testing.T) {
	current := mustSchema(t, 1, mustField(t, "a", StringType{}, false, false, false))
	id, err := codec.RandomUUID()
	if err != nil {
		t.Fatal(err)
	}
	m := SchemaMigration{
		MigrationID: id,
		FromVersion: 1,
		ToVersion:   2,
		Steps:       []MigrationStep{AddFieldStep{Field: mustField(t, "b", Int32Type{}, false, false, false)}},
		Description: "add b",
	}
	res := ApplyMigration(current, m)
	next, ok := res.Value()
	if !ok {
		t.Fatalf("migration failed: %v", res.Exception())
	}
	if next.Version != 2 {
		t.Fatalf("version is %d, want 2", next.Version)
	}
	if len(next.Fields) != 2 {
		t.Fatalf("field count is %d, want 2", len(next.Fields))
	}
	if next.Description != "add b" {
		t.Fatalf("description is %q", next.Description)
	}
	// The input schema must be untouched - callers hold onto the old version.
	if current.Version != 1 || len(current.Fields) != 1 {
		t.Fatal("ApplyMigration mutated the schema it was given")
	}
}

func TestApplyMigrationIsIdempotentOnceApplied(t *testing.T) {
	// A migration whose toVersion is already the current version is a replay, not an error -
	// this is what makes migration application safe to retry after a crash mid-apply.
	current := mustSchema(t, 2, mustField(t, "a", StringType{}, false, false, false))
	id, _ := codec.RandomUUID()
	m := SchemaMigration{MigrationID: id, FromVersion: 1, ToVersion: 2,
		Steps: []MigrationStep{AddFieldStep{Field: mustField(t, "b", Int32Type{}, false, false, false)}}}

	res := ApplyMigration(current, m)
	got, ok := res.Value()
	if !ok {
		t.Fatalf("replaying an applied migration failed: %v", res.Exception())
	}
	if got.Version != 2 || len(got.Fields) != 1 {
		t.Fatalf("a replay changed the schema: v%d with %d fields", got.Version, len(got.Fields))
	}
}

func TestApplyMigrationRejectsVersionMismatch(t *testing.T) {
	current := mustSchema(t, 3, mustField(t, "a", StringType{}, false, false, false))
	id, _ := codec.RandomUUID()

	// fromVersion must match the schema being migrated - applying v1->v2 to a v3 schema would
	// replay steps that have already been applied.
	if ApplyMigration(current, SchemaMigration{
		MigrationID: id, FromVersion: 1, ToVersion: 4,
		Steps: []MigrationStep{AddIndexStep{FieldName: "a"}},
	}).IsSuccess() {
		t.Error("a migration from the wrong version was accepted")
	}

	// Versions advance one at a time, so a v3->v5 jump has a step nobody has seen.
	if ApplyMigration(current, SchemaMigration{
		MigrationID: id, FromVersion: 3, ToVersion: 5,
		Steps: []MigrationStep{AddIndexStep{FieldName: "a"}},
	}).IsSuccess() {
		t.Error("a migration skipping a version was accepted")
	}
}

func TestApplyMigrationRejectsAStepItCannotReplay(t *testing.T) {
	current := mustSchema(t, 1, mustField(t, "a", StringType{}, false, false, false))
	id, _ := codec.RandomUUID()
	// Dropping a field the schema does not have cannot be replayed onto it.
	if ApplyMigration(current, SchemaMigration{
		MigrationID: id, FromVersion: 1, ToVersion: 2,
		Steps: []MigrationStep{DropFieldStep{FieldName: "nonexistent"}},
	}).IsSuccess() {
		t.Fatal("dropping an absent field was accepted")
	}
}

func TestIsBackwardCompatible(t *testing.T) {
	current := mustSchema(t, 1, mustField(t, "a", StringType{}, false, false, false))
	id, _ := codec.RandomUUID()

	safe := SchemaMigration{MigrationID: id, FromVersion: 1, ToVersion: 2,
		Steps: []MigrationStep{
			AddFieldStep{Field: mustField(t, "b", Int32Type{}, false, false, false)},
			AddIndexStep{FieldName: "a"},
		}}
	if !IsBackwardCompatible(current, safe) {
		t.Error("adding an optional field and an index should be backward compatible")
	}

	// One breaking step among otherwise safe ones makes the whole migration breaking.
	mixed := SchemaMigration{MigrationID: id, FromVersion: 1, ToVersion: 2,
		Steps: []MigrationStep{
			AddFieldStep{Field: mustField(t, "b", Int32Type{}, false, false, false)},
			DropFieldStep{FieldName: "a"},
		}}
	if IsBackwardCompatible(current, mixed) {
		t.Error("a migration containing a field drop is not backward compatible")
	}

	// A migration that does not even start from this version cannot be judged compatible.
	wrongBase := SchemaMigration{MigrationID: id, FromVersion: 7, ToVersion: 8,
		Steps: []MigrationStep{AddIndexStep{FieldName: "a"}}}
	if IsBackwardCompatible(current, wrongBase) {
		t.Error("a migration from another version should not be reported compatible")
	}
}
