package schema

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// MigrationStep is one ordered schema evolution operation.
type MigrationStep interface {
	isMigrationStep()
}

type (
	AddFieldStep    struct{ Field Field }
	DropFieldStep   struct{ FieldName string }
	RenameFieldStep struct{ OldName, NewName string }
	ChangeTypeStep  struct {
		FieldName string
		NewType   FieldType
	}
	AddIndexStep    struct{ FieldName string }
	DropIndexStep   struct{ FieldName string }
	SetRequiredStep struct {
		FieldName string
		Required  bool
	}
	SetUniqueStep struct {
		FieldName string
		Unique    bool
	}
	WidenEnumStep struct {
		FieldName string
		AddValues map[string]struct{}
	}
	NarrowEnumStep struct {
		FieldName    string
		RemoveValues map[string]struct{}
	}
)

func (AddFieldStep) isMigrationStep()    {}
func (DropFieldStep) isMigrationStep()   {}
func (RenameFieldStep) isMigrationStep() {}
func (ChangeTypeStep) isMigrationStep()  {}
func (AddIndexStep) isMigrationStep()    {}
func (DropIndexStep) isMigrationStep()   {}
func (SetRequiredStep) isMigrationStep() {}
func (SetUniqueStep) isMigrationStep()   {}
func (WidenEnumStep) isMigrationStep()   {}
func (NarrowEnumStep) isMigrationStep()  {}

// IsBreaking reports whether a step breaks backward compatibility.
func IsBreaking(step MigrationStep) bool {
	switch s := step.(type) {
	case AddFieldStep:
		return s.Field.Required
	case DropFieldStep, RenameFieldStep, ChangeTypeStep:
		return true
	case AddIndexStep, DropIndexStep:
		return false
	case SetRequiredStep:
		return s.Required
	case SetUniqueStep:
		return s.Unique
	case WidenEnumStep:
		return false
	case NarrowEnumStep:
		return true
	default:
		return true
	}
}

// SchemaMigration describes a schema transition.
type SchemaMigration struct {
	MigrationID codec.UUID
	FromVersion int
	ToVersion   int
	Steps       []MigrationStep
	Description string
}

// ToBytes returns canonical wire bytes.
func (m SchemaMigration) ToBytes() ([]byte, error) {
	return codec.EncodeBytes(schemaMigrationRecord(m), MigrationWireType(), WireRegistry())
}

// MigrationFromBytes decodes a migration.
func MigrationFromBytes(b []byte) (SchemaMigration, error) {
	reg := WireRegistry()
	v, err := codec.DecodeBytes(b, MigrationWireType(), reg)
	if err != nil {
		return SchemaMigration{}, newDecodeError("migration decode failed", err)
	}
	rec, ok := v.(codec.RecordValue)
	if !ok {
		return SchemaMigration{}, newDecodeError("migration: expected record", nil)
	}
	return parseSchemaMigrationRecord(rec)
}
