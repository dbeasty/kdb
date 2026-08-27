package schema

// Diff is a human-readable schema delta.
type Diff struct {
	AddedFields    []Field
	RemovedFields  []Field
	ModifiedFields []FieldDiff
	FromVersion    int
	ToVersion      int
}

// IsEmpty reports whether the diff has no changes.
func (d Diff) IsEmpty() bool {
	return len(d.AddedFields) == 0 && len(d.RemovedFields) == 0 && len(d.ModifiedFields) == 0
}

// IsBreaking reports whether applying the diff would break existing data.
func (d Diff) IsBreaking() bool {
	if len(d.RemovedFields) > 0 {
		return true
	}
	for _, f := range d.AddedFields {
		if f.Required {
			return true
		}
	}
	for _, fd := range d.ModifiedFields {
		for _, ch := range fd.Changes {
			switch c := ch.(type) {
			case TypeChanged:
				return true
			case RequiredChanged:
				if c.To {
					return true
				}
			case UniqueChanged:
				if c.To {
					return true
				}
			case EnumValuesChanged:
				if len(c.Removed) > 0 {
					return true
				}
			}
		}
	}
	return false
}

// FieldDiff lists changes for one field name.
type FieldDiff struct {
	FieldName string
	Changes   []FieldChange
}

// FieldChange describes one aspect of a field modification.
type FieldChange interface {
	isFieldChange()
}

type (
	TypeChanged       struct{ From, To FieldType }
	RequiredChanged   struct{ From, To bool }
	IndexedChanged    struct{ From, To bool }
	UniqueChanged     struct{ From, To bool }
	EnumValuesChanged struct {
		Added, Removed map[string]struct{}
	}
)

func (TypeChanged) isFieldChange()       {}
func (RequiredChanged) isFieldChange()   {}
func (IndexedChanged) isFieldChange()    {}
func (UniqueChanged) isFieldChange()     {}
func (EnumValuesChanged) isFieldChange() {}
