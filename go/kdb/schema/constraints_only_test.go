package schema_test

import (
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	"github.com/limidus/kdb/go/kdb/schema"
)

// A constraints-only schema carries unique constraints over JSON paths and declares no fields.
// The namespace stays schemaless for queries and validation; the constraint is a real schema
// fact that must be built, hashed, and validated like any other.

func TestConstraintsOnlySchemaIsSchemalessButNotTheSentinel(t *testing.T) {
	got, err := schema.None().WithUniqueTuple([]string{"email"}, codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	if got.IsNone() {
		t.Fatal("it is a real schema that must be recorded and replicated, so it is not the sentinel")
	}
	if !got.IsSchemaless() {
		t.Fatal("it declares no fields, so the namespace must stay schemaless")
	}
	if !got.HasUniqueConstraints() || len(got.UniqueTuples()) != 1 || got.UniqueTuples()[0][0] != "email" {
		t.Fatalf("tuples = %v", got.UniqueTuples())
	}
	if !schema.None().IsSchemaless() {
		t.Fatal("the sentinel is schemaless too")
	}
}

func TestWithUniqueTupleIsAdditiveAndIdempotent(t *testing.T) {
	a, err := schema.None().WithUniqueTuple([]string{"email"}, codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	b, err := a.WithUniqueTuple([]string{"entityType", "entityId"}, codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(b.UniqueTuples()) != 2 {
		t.Fatalf("want both constraints, got %v", b.UniqueTuples())
	}
	again, err := b.WithUniqueTuple([]string{"email"}, codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	if again.SchemaHash != b.SchemaHash {
		t.Fatal("adding a tuple the schema already enforces must change nothing")
	}
	if len(a.UniqueTuples()) != 1 {
		t.Fatal("WithUniqueTuple must not mutate its receiver")
	}
}

func TestDeclaredSchemaStillRejectsAConstraintOnAnUnknownField(t *testing.T) {
	_, err := schema.BuildWithConstraints(
		[]schema.Field{schema.MustField("name", schema.StringType{}, false, false, false)},
		[]schema.UniqueConstraint{{Fields: []string{"nope"}}}, 1, codec.TimestampNow(), "")
	if err == nil {
		t.Fatal("with declared fields a constraint naming an undeclared one is still an error")
	}
}

func TestConstraintsOnlySchemaDoesNotValidateDocuments(t *testing.T) {
	sch, err := schema.None().WithUniqueTuple([]string{"email"}, codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	doc, err := document.FromJSON(`{"anything":"goes","nested":{"x":1}}`)
	if err != nil {
		t.Fatal(err)
	}
	if r := schema.Validate(doc, sch); !r.IsSuccess() {
		t.Fatalf("free-form documents must pass: %v", r.Exception())
	}
}

// Persistence and replication move a schema as bytes; a constraints-only schema has to come back
// from them intact, or a restart would silently drop the constraint.
func TestConstraintsOnlySchemaSurvivesTheWire(t *testing.T) {
	sch, err := schema.None().WithUniqueTuple([]string{"entityType", "entityId"}, codec.TimestampNow())
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := sch.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	back, err := schema.FromBytes(bytes)
	if err != nil {
		t.Fatalf("decoding a constraints-only schema: %v", err)
	}
	if back.SchemaHash != sch.SchemaHash || !back.IsSchemaless() || back.IsNone() {
		t.Fatalf("round trip changed the schema: %+v", back)
	}
	tuples := back.UniqueTuples()
	if len(tuples) != 1 || len(tuples[0]) != 2 || tuples[0][0] != "entityType" || tuples[0][1] != "entityId" {
		t.Fatalf("the constraint did not survive: %v", tuples)
	}
}
