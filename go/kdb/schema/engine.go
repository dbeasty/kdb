package schema

import (
	"fmt"
	"math"
	"sort"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// Validate checks document JSON against a schema.
func Validate(doc document.Document, sch KdbSchema) kdberr.Result[document.Document] {
	if sch.IsNone() {
		return kdberr.Ok(doc)
	}
	var violations []kdberr.FieldViolation
	for _, field := range sch.Fields {
		path := "$." + field.Name
		raw, err := jsonGet(doc.JSON, path)
		if err != nil {
			violations = append(violations, kdberr.FieldViolation{
				FieldName:     field.Name,
				ViolationType: kdberr.TypeMismatch,
				Detail:        "invalid JSON path access for " + field.Name,
			})
			continue
		}
		if field.Required && isJSONNull(raw) {
			violations = append(violations, kdberr.FieldViolation{
				FieldName:     field.Name,
				ViolationType: kdberr.RequiredFieldMissing,
				Detail:        "required field missing or null",
			})
			continue
		}
		if !field.Required && isJSONNull(raw) {
			continue
		}
		if v := checkFieldValue(field, raw); v != nil {
			violations = append(violations, *v)
		}
	}
	if len(violations) > 0 {
		return kdberr.Fail[document.Document](kdberr.NewSchemaViolationError("schema validation failed", violations))
	}
	return kdberr.Ok(doc)
}

// ApplyMigration applies one version step to a schema.
func ApplyMigration(current KdbSchema, migration SchemaMigration) kdberr.Result[KdbSchema] {
	if current.Version == migration.ToVersion {
		return kdberr.Ok(current)
	}
	if migration.FromVersion != current.Version {
		return migrationFailure("migration.fromVersion (" + itoa(migration.FromVersion) + ") != current.version (" + itoa(current.Version) + ")")
	}
	if migration.ToVersion != current.Version+1 {
		return migrationFailure("migration.toVersion (" + itoa(migration.ToVersion) + ") must equal current.version + 1")
	}
	nextFields, err := replayMigrationSteps(current.Fields, migration.Steps)
	if err != nil {
		var mc *MigrationConflictError
		if e, ok := err.(*MigrationConflictError); ok {
			mc = e
		} else {
			return migrationFailure(err.Error())
		}
		return kdberr.Fail[KdbSchema](kdberr.NewSchemaMigrationError(
			mc.Error(), "", describeStep(mc.Step), mc))
	}
	desc := migration.Description
	if desc == "" {
		desc = current.Description
	}
	built, err := Build(nextFields, migration.ToVersion, codec.TimestampNow(), desc)
	if err != nil {
		return kdberr.Fail[KdbSchema](kdberr.NewSchemaMigrationError(err.Error(), "", "", err))
	}
	return kdberr.Ok(built)
}

// ComputeSchemaHash recomputes the schema content hash.
func ComputeSchemaHash(sch KdbSchema) (codec.Hash, error) {
	body := schemaBodyValue(sch.Fields, sch.UniqueConstraints, sch.Version, sch.CreatedAt, sch.Description)
	bytes, err := codec.EncodeBytes(body, BodyWireType(), WireRegistry())
	if err != nil {
		return codec.Hash{}, err
	}
	sum := document.SHA256Digest(bytes)
	return codec.HashFromBytes(sum[:])
}

// IsBackwardCompatible reports whether migration steps are all non-breaking.
func IsBackwardCompatible(current KdbSchema, migration SchemaMigration) bool {
	if migration.FromVersion != current.Version {
		return false
	}
	for _, step := range migration.Steps {
		if IsBreaking(step) {
			return false
		}
	}
	return true
}

// DiffSchemas compares two schema versions.
func DiffSchemas(from, to KdbSchema) Diff {
	fa := map[string]Field{}
	for _, f := range from.Fields {
		fa[f.Name] = f
	}
	fb := map[string]Field{}
	for _, f := range to.Fields {
		fb[f.Name] = f
	}
	// Each of the three lists is built by ranging a map, so each is sorted by field name before
	// returning. Without that the order varied run to run, and a Diff is exactly the kind of
	// thing that gets rendered for a human, serialized, or compared against another Diff to
	// decide whether two schema versions agree - all of which a shuffling order breaks.
	var added, removed []Field
	for name, f := range fb {
		if _, ok := fa[name]; !ok {
			added = append(added, f)
		}
	}
	for name, f := range fa {
		if _, ok := fb[name]; !ok {
			removed = append(removed, f)
		}
	}
	var modified []FieldDiff
	for name, a := range fa {
		b, ok := fb[name]
		if !ok {
			continue
		}
		if changes := diffSingleField(a, b); len(changes) > 0 {
			modified = append(modified, FieldDiff{FieldName: name, Changes: changes})
		}
	}
	sort.Slice(added, func(i, j int) bool { return added[i].Name < added[j].Name })
	sort.Slice(removed, func(i, j int) bool { return removed[i].Name < removed[j].Name })
	sort.Slice(modified, func(i, j int) bool { return modified[i].FieldName < modified[j].FieldName })
	return Diff{
		AddedFields: added, RemovedFields: removed, ModifiedFields: modified,
		FromVersion: from.Version, ToVersion: to.Version,
	}
}

// CheckFieldValue validates one JSON value against a field declaration.
func CheckFieldValue(field Field, value any) *kdberr.FieldViolation {
	return checkFieldValue(field, value)
}

func checkFieldValue(field Field, value any) *kdberr.FieldViolation {
	if isJSONNull(value) {
		return nil
	}
	switch t := field.Type.(type) {
	case StringType:
		if _, ok := value.(string); ok {
			return nil
		}
		return typeMismatch(field.Name, "expected string")
	case Int32Type:
		if v, ok := value.(float64); ok && isIntegralFloat(v) && v >= math.MinInt32 && v <= math.MaxInt32 {
			return nil
		}
		return typeMismatch(field.Name, "expected int32")
	case Int64Type:
		// Bounded like Int32Type above, which it was not: an integral float64 outside int64's
		// range (1e30, say) passed validation and then could not be stored as the int64 the
		// field declares. float64 cannot represent math.MaxInt64 exactly - the nearest value is
		// 2^63, one past it - so the upper bound is a strict "less than 2^63" rather than a
		// "<= MaxInt64" that would silently admit 2^63 itself.
		if v, ok := value.(float64); ok && isIntegralFloat(v) && v >= math.MinInt64 && v < twoToThe63 {
			return nil
		}
		return typeMismatch(field.Name, "expected int64")
	case Float64Type:
		if _, ok := value.(float64); ok {
			return nil
		}
		return typeMismatch(field.Name, "expected number")
	case BoolType:
		if _, ok := value.(bool); ok {
			return nil
		}
		return typeMismatch(field.Name, "expected boolean")
	case TimestampType:
		s, ok := value.(string)
		if !ok {
			return typeMismatch(field.Name, "expected timestamp string")
		}
		if _, err := codec.TimestampFromISO8601(s); err != nil {
			return typeMismatch(field.Name, "invalid ISO-8601 timestamp")
		}
		return nil
	case UUIDType:
		s, ok := value.(string)
		if !ok {
			return typeMismatch(field.Name, "expected UUID string")
		}
		if _, err := codec.ParseUUID(s); err != nil {
			return typeMismatch(field.Name, "invalid UUID string")
		}
		return nil
	case ObjectType:
		if _, ok := value.(map[string]any); ok {
			return nil
		}
		return typeMismatch(field.Name, "expected JSON object")
	case ArrayType:
		if _, ok := value.([]any); ok {
			return nil
		}
		return typeMismatch(field.Name, "expected JSON array")
	case EnumType:
		s, ok := value.(string)
		if !ok {
			return typeMismatch(field.Name, "expected string enum")
		}
		if _, ok := t.Values[s]; !ok {
			return &kdberr.FieldViolation{
				FieldName:     field.Name,
				ViolationType: kdberr.EnumValueNotDeclared,
				Detail:        "value not in enum",
			}
		}
		return nil
	default:
		return typeMismatch(field.Name, "unknown field type")
	}
}

func typeMismatch(field, detail string) *kdberr.FieldViolation {
	return &kdberr.FieldViolation{FieldName: field, ViolationType: kdberr.TypeMismatch, Detail: detail}
}

// twoToThe63 is one past math.MaxInt64. math.MaxInt64 itself has no exact float64
// representation, so comparing against it would round up to this value and admit it.
const twoToThe63 = float64(1 << 63)

func isIntegralFloat(d float64) bool {
	return math.Abs(d-math.Round(d)) < 1e-9
}

func diffSingleField(a, b Field) []FieldChange {
	var changes []FieldChange
	ea, aIsEnum := a.Type.(EnumType)
	eb, bIsEnum := b.Type.(EnumType)
	if aIsEnum && bIsEnum {
		added, removed := enumDiff(ea.Values, eb.Values)
		if len(added) > 0 || len(removed) > 0 {
			changes = append(changes, EnumValuesChanged{Added: added, Removed: removed})
		}
	} else if fieldTypeEqual(a.Type, b.Type) == false {
		changes = append(changes, TypeChanged{From: a.Type, To: b.Type})
	}
	if a.Required != b.Required {
		changes = append(changes, RequiredChanged{From: a.Required, To: b.Required})
	}
	if a.Indexed != b.Indexed {
		changes = append(changes, IndexedChanged{From: a.Indexed, To: b.Indexed})
	}
	if a.Unique != b.Unique {
		changes = append(changes, UniqueChanged{From: a.Unique, To: b.Unique})
	}
	return changes
}

func enumDiff(a, b map[string]struct{}) (added, removed map[string]struct{}) {
	added = map[string]struct{}{}
	removed = map[string]struct{}{}
	for k := range b {
		if _, ok := a[k]; !ok {
			added[k] = struct{}{}
		}
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			removed[k] = struct{}{}
		}
	}
	return added, removed
}

func fieldTypeEqual(a, b FieldType) bool {
	switch at := a.(type) {
	case EnumType:
		bt, ok := b.(EnumType)
		if !ok || len(at.Values) != len(bt.Values) {
			return false
		}
		for k := range at.Values {
			if _, ok := bt.Values[k]; !ok {
				return false
			}
		}
		return true
	default:
		return fmt.Sprintf("%T", a) == fmt.Sprintf("%T", b)
	}
}

func migrationFailure(msg string) kdberr.Result[KdbSchema] {
	return kdberr.Fail[KdbSchema](kdberr.NewSchemaMigrationError(msg, "", "", nil))
}

func describeStep(step MigrationStep) string {
	switch s := step.(type) {
	case AddFieldStep:
		return "AddField(" + s.Field.Name + ")"
	case DropFieldStep:
		return "DropField(" + s.FieldName + ")"
	case RenameFieldStep:
		return "RenameField(" + s.OldName + "->" + s.NewName + ")"
	case ChangeTypeStep:
		return "ChangeType(" + s.FieldName + ")"
	case AddIndexStep:
		return "AddIndex(" + s.FieldName + ")"
	case DropIndexStep:
		return "DropIndex(" + s.FieldName + ")"
	case SetRequiredStep:
		return "SetRequired(" + s.FieldName + ")"
	case SetUniqueStep:
		return "SetUnique(" + s.FieldName + ")"
	case WidenEnumStep:
		return "WidenEnum(" + s.FieldName + ")"
	case NarrowEnumStep:
		return "NarrowEnum(" + s.FieldName + ")"
	default:
		return "unknown"
	}
}

func replayMigrationSteps(base []Field, steps []MigrationStep) ([]Field, error) {
	fields := append([]Field(nil), base...)
	for _, step := range steps {
		if err := applyMigrationStep(&fields, step); err != nil {
			if mc, ok := err.(*MigrationConflictError); ok {
				return nil, mc
			}
			return nil, newMigrationConflict(err.Error(), step)
		}
	}
	return fields, nil
}

func applyMigrationStep(fields *[]Field, step MigrationStep) error {
	switch s := step.(type) {
	case AddFieldStep:
		for _, f := range *fields {
			if f.Name == s.Field.Name {
				return fmt.Errorf("field %s already exists", s.Field.Name)
			}
		}
		*fields = append(*fields, s.Field)
	case DropFieldStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("cannot drop unknown field %s", s.FieldName)
		}
		*fields = append((*fields)[:ix], (*fields)[ix+1:]...)
	case RenameFieldStep:
		ix := indexField(*fields, s.OldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.OldName)
		}
		for _, f := range *fields {
			if f.Name == s.NewName {
				return fmt.Errorf("target name %s already exists", s.NewName)
			}
		}
		cur := (*fields)[ix]
		(*fields)[ix] = Field{Name: s.NewName, Type: cur.Type, Required: cur.Required, Indexed: cur.Indexed, Unique: cur.Unique}
	case ChangeTypeStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.FieldName)
		}
		cur := (*fields)[ix]
		(*fields)[ix] = Field{Name: cur.Name, Type: s.NewType, Required: cur.Required, Indexed: cur.Indexed, Unique: cur.Unique}
	case AddIndexStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.FieldName)
		}
		cur := (*fields)[ix]
		(*fields)[ix] = Field{Name: cur.Name, Type: cur.Type, Required: cur.Required, Indexed: true, Unique: cur.Unique}
	case DropIndexStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.FieldName)
		}
		cur := (*fields)[ix]
		(*fields)[ix] = Field{Name: cur.Name, Type: cur.Type, Required: cur.Required, Indexed: false, Unique: false}
	case SetRequiredStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.FieldName)
		}
		cur := (*fields)[ix]
		(*fields)[ix] = Field{Name: cur.Name, Type: cur.Type, Required: s.Required, Indexed: cur.Indexed, Unique: cur.Unique}
	case SetUniqueStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.FieldName)
		}
		cur := (*fields)[ix]
		indexed := cur.Indexed
		if s.Unique {
			indexed = true
		}
		(*fields)[ix] = Field{Name: cur.Name, Type: cur.Type, Required: cur.Required, Indexed: indexed, Unique: s.Unique}
	case WidenEnumStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.FieldName)
		}
		cur := (*fields)[ix]
		et, ok := cur.Type.(EnumType)
		if !ok {
			return fmt.Errorf("%s is not an enum field", s.FieldName)
		}
		merged := copyEnumValues(et.Values)
		for v := range s.AddValues {
			merged[v] = struct{}{}
		}
		(*fields)[ix] = Field{Name: cur.Name, Type: EnumType{Values: merged}, Required: cur.Required, Indexed: cur.Indexed, Unique: cur.Unique}
	case NarrowEnumStep:
		ix := indexField(*fields, s.FieldName)
		if ix < 0 {
			return fmt.Errorf("unknown field %s", s.FieldName)
		}
		cur := (*fields)[ix]
		et, ok := cur.Type.(EnumType)
		if !ok {
			return fmt.Errorf("%s is not an enum field", s.FieldName)
		}
		for v := range s.RemoveValues {
			if _, ok := et.Values[v]; !ok {
				return fmt.Errorf("cannot remove enum symbols that are not declared on %s", s.FieldName)
			}
		}
		rest := copyEnumValues(et.Values)
		for v := range s.RemoveValues {
			delete(rest, v)
		}
		if len(rest) == 0 {
			return fmt.Errorf("enum cannot become empty on %s", s.FieldName)
		}
		(*fields)[ix] = Field{Name: cur.Name, Type: EnumType{Values: rest}, Required: cur.Required, Indexed: cur.Indexed, Unique: cur.Unique}
	}
	return nil
}

func indexField(fields []Field, name string) int {
	for i, f := range fields {
		if f.Name == name {
			return i
		}
	}
	return -1
}

func copyEnumValues(m map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
