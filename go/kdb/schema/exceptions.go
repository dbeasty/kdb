package schema

// DecodeError indicates schema wire decode failure.
type DecodeError struct {
	Msg   string
	Cause error
}

func (e *DecodeError) Error() string {
	if e.Cause != nil {
		return e.Msg + ": " + e.Cause.Error()
	}
	return e.Msg
}

func newDecodeError(msg string, cause error) *DecodeError {
	return &DecodeError{Msg: msg, Cause: cause}
}

// MigrationConflictError indicates an invalid migration step.
type MigrationConflictError struct {
	Msg  string
	Step MigrationStep
}

func (e *MigrationConflictError) Error() string { return e.Msg }

func newMigrationConflict(msg string, step MigrationStep) *MigrationConflictError {
	return &MigrationConflictError{Msg: msg, Step: step}
}
