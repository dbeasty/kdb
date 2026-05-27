package schema

// LogicalAnnotation marks logical types over physical kinds.
type LogicalAnnotation interface {
	isLogical()
}

type LogicalDate struct{}
func (LogicalDate) isLogical() {}

type LogicalTimeMicros struct{}
func (LogicalTimeMicros) isLogical() {}

type LogicalTimestampMicros struct{ Timezone *string }
func (LogicalTimestampMicros) isLogical() {}

type LogicalTimestampMillis struct{ Timezone *string }
func (LogicalTimestampMillis) isLogical() {}

type LogicalUUID struct{}
func (LogicalUUID) isLogical() {}

type LogicalDuration struct{}
func (LogicalDuration) isLogical() {}

type LogicalCustom struct{ ID string }
func (LogicalCustom) isLogical() {}
