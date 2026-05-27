package schema

import "github.com/limidus/kdb/go/kdb/codec"

// MigrationBuilder is a DSL for SchemaMigration.
type MigrationBuilder struct {
	base        KdbSchema
	steps       []MigrationStep
	description string
}

// NewMigrationBuilder starts a migration from baseSchema.
func NewMigrationBuilder(base KdbSchema) *MigrationBuilder {
	return &MigrationBuilder{base: base}
}

func (b *MigrationBuilder) AddField(name string, typ FieldType, required, indexed, unique bool) *MigrationBuilder {
	f := MustField(name, typ, required, indexed, unique)
	b.steps = append(b.steps, AddFieldStep{Field: f})
	return b
}

func (b *MigrationBuilder) DropField(name string) *MigrationBuilder {
	b.steps = append(b.steps, DropFieldStep{FieldName: name})
	return b
}

func (b *MigrationBuilder) WidenEnum(fieldName string, addValues ...string) *MigrationBuilder {
	m := make(map[string]struct{}, len(addValues))
	for _, v := range addValues {
		m[v] = struct{}{}
	}
	b.steps = append(b.steps, WidenEnumStep{FieldName: fieldName, AddValues: m})
	return b
}

func (b *MigrationBuilder) NarrowEnum(fieldName string, removeValues ...string) *MigrationBuilder {
	m := make(map[string]struct{}, len(removeValues))
	for _, v := range removeValues {
		m[v] = struct{}{}
	}
	b.steps = append(b.steps, NarrowEnumStep{FieldName: fieldName, RemoveValues: m})
	return b
}

func (b *MigrationBuilder) Description(text string) *MigrationBuilder {
	b.description = text
	return b
}

// Build validates the step sequence and returns a migration.
func (b *MigrationBuilder) Build(migrationID codec.UUID) (SchemaMigration, error) {
	if _, err := replayMigrationSteps(b.base.Fields, b.steps); err != nil {
		return SchemaMigration{}, err
	}
	if migrationID == (codec.UUID{}) {
		var err error
		migrationID, err = codec.RandomUUID()
		if err != nil {
			return SchemaMigration{}, err
		}
	}
	return SchemaMigration{
		MigrationID: migrationID,
		FromVersion: b.base.Version,
		ToVersion:   b.base.Version + 1,
		Steps:       append([]MigrationStep(nil), b.steps...),
		Description: b.description,
	}, nil
}

// Migrate runs a builder block against a schema.
func Migrate(base KdbSchema, fn func(*MigrationBuilder)) (SchemaMigration, error) {
	b := NewMigrationBuilder(base)
	fn(b)
	return b.Build(codec.UUID{})
}
