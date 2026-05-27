package schema_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
	"github.com/limidus/kdb/go/kdb/schema"
)

func TestValidDocumentPasses(t *testing.T) {
	sch, err := schema.Build([]schema.Field{
		schema.MustField("userId", schema.StringType{}, true, true, false),
		schema.MustField("email", schema.StringType{}, true, true, false),
		schema.MustField("status", schema.NewEnumType("active", "inactive"), true, true, false),
		schema.MustField("createdAt", schema.TimestampType{}, true, false, false),
	}, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSON(`{"userId":"abc","email":"a@b.com","status":"active","createdAt":"2024-01-01T00:00:00Z"}`)
	if err != nil {
		t.Fatal(err)
	}
	r := schema.Validate(doc, sch)
	if !r.IsSuccess() {
		t.Fatalf("expected success: %v", r.Exception())
	}
}

func TestMissingRequiredFails(t *testing.T) {
	sch, err := schema.Build([]schema.Field{
		schema.MustField("userId", schema.StringType{}, true, true, false),
		schema.MustField("email", schema.StringType{}, true, true, false),
		schema.MustField("status", schema.StringType{}, true, true, false),
	}, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSON(`{"userId":"abc","status":"active"}`)
	if err != nil {
		t.Fatal(err)
	}
	r := schema.Validate(doc, sch)
	if r.IsSuccess() {
		t.Fatal("expected failure")
	}
	sv, ok := r.Exception().(*kdberr.SchemaViolationError)
	if !ok {
		t.Fatalf("expected SchemaViolationError, got %T", r.Exception())
	}
	if len(sv.Violations) != 1 || sv.Violations[0].FieldName != "email" {
		t.Fatalf("violations: %+v", sv.Violations)
	}
	if sv.Violations[0].ViolationType != kdberr.RequiredFieldMissing {
		t.Fatalf("type: %v", sv.Violations[0].ViolationType)
	}
}

func TestNoneSchemaAlwaysPasses(t *testing.T) {
	doc, err := document.FromJSON(`{"x":1}`)
	if err != nil {
		t.Fatal(err)
	}
	r := schema.Validate(doc, schema.None())
	if !r.IsSuccess() {
		t.Fatal(r.Exception())
	}
}

func TestMigrationRoundTripWire(t *testing.T) {
	base, err := schema.Build([]schema.Field{
		schema.MustField("s", schema.NewEnumType("a", "b"), false, true, false),
	}, 1, codec.TimestampNow(), "")
	if err != nil {
		t.Fatal(err)
	}
	m, err := schema.Migrate(base, func(b *schema.MigrationBuilder) {
		b.WidenEnum("s", "c").NarrowEnum("s", "b").Description("zigzag")
	})
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := m.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	back, err := schema.MigrationFromBytes(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if back.MigrationID != m.MigrationID || len(back.Steps) != len(m.Steps) {
		t.Fatalf("roundtrip mismatch: %+v vs %+v", back, m)
	}
}

func TestSchemaHashDeterministic(t *testing.T) {
	fields := []schema.Field{
		schema.MustField("userId", schema.StringType{}, true, true, false),
	}
	ts := codec.TimestampFromEpochMicros(1)
	a, err := schema.Build(fields, 1, ts, "")
	if err != nil {
		t.Fatal(err)
	}
	b, err := schema.Build(fields, 1, ts, "")
	if err != nil {
		t.Fatal(err)
	}
	if a.SchemaHash != b.SchemaHash {
		t.Fatalf("hashes differ: %s vs %s", a.SchemaHash.Hex(), b.SchemaHash.Hex())
	}
	h, err := schema.ComputeSchemaHash(a)
	if err != nil {
		t.Fatal(err)
	}
	if h != a.SchemaHash {
		t.Fatalf("compute mismatch: %s vs %s", h.Hex(), a.SchemaHash.Hex())
	}
}
