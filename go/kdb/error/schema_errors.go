package error

// SchemaViolationError is returned when document JSON fails schema validation.
type SchemaViolationError struct {
	*base
	Violations []FieldViolation
}

func NewSchemaViolationError(msg string, violations []FieldViolation) *SchemaViolationError {
	return &SchemaViolationError{
		base:       &base{code: SchemaViolation, msg: msg},
		Violations: violations,
	}
}

// SchemaMigrationError is returned when a migration cannot be applied.
type SchemaMigrationError struct {
	*base
	FieldName string
	Step      string
}

func NewSchemaMigrationError(msg, field, step string, cause error) *SchemaMigrationError {
	return &SchemaMigrationError{
		base:      &base{code: SchemaMigrationFailed, msg: msg, cause: cause},
		FieldName: field,
		Step:      step,
	}
}
