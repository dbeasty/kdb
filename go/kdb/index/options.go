package index

import (
	"fmt"
	"strconv"
	"strings"
)

// Option keys carried in Descriptor.Options.
const (
	OptionIndexName      = "index_name"
	OptionWeights        = "weights"
	OptionDimensions     = "dimensions"
	OptionMetric         = "metric"
	OptionM              = "m"
	OptionEfConstruction = "ef_construction"
	OptionEfSearch       = "ef_search"
	OptionFieldType      = "field_type"
)

// IndexName returns Options["index_name"], or "" when the index is unnamed.
func (d Descriptor) IndexName() string {
	if d.Options == nil {
		return ""
	}
	return d.Options[OptionIndexName]
}

// FieldPaths returns the indexed JSON paths: Fields when set, else the single FieldName.
func (d Descriptor) FieldPaths() []string {
	if len(d.Fields) > 0 {
		return append([]string(nil), d.Fields...)
	}
	if d.FieldName == "" {
		return nil
	}
	return []string{d.FieldName}
}

// FirstField is the registry key: Fields[0] when Fields is set, else FieldName.
func (d Descriptor) FirstField() string {
	if len(d.Fields) > 0 {
		return d.Fields[0]
	}
	return d.FieldName
}

// FieldWeights returns one weight per FieldPaths entry, parsed from Options["weights"]
// ("title=3,description=1"). Fields not listed weigh 1. A malformed entry is an error.
func (d Descriptor) FieldWeights() ([]float64, error) {
	paths := d.FieldPaths()
	weights := make([]float64, len(paths))
	for i := range weights {
		weights[i] = 1
	}
	raw := ""
	if d.Options != nil {
		raw = strings.TrimSpace(d.Options[OptionWeights])
	}
	if raw == "" {
		return weights, nil
	}
	byPath := make(map[string]int, len(paths))
	for i, p := range paths {
		byPath[p] = i
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, val, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("index option weights: malformed entry %q", part)
		}
		w, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil || w < 0 {
			return nil, fmt.Errorf("index option weights: bad weight in %q", part)
		}
		i, known := byPath[strings.TrimSpace(name)]
		if !known {
			return nil, fmt.Errorf("index option weights: %q is not an indexed field", name)
		}
		weights[i] = w
	}
	return weights, nil
}

// IntOption returns an integer option, or def when absent. A present but unparsable value is
// an error.
func (d Descriptor) IntOption(key string, def int) (int, error) {
	if d.Options == nil {
		return def, nil
	}
	raw, ok := d.Options[key]
	if !ok || strings.TrimSpace(raw) == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("index option %s: %q is not an integer", key, raw)
	}
	return n, nil
}

// StringOption returns a string option, or def when absent.
func (d Descriptor) StringOption(key, def string) string {
	if d.Options == nil {
		return def
	}
	if v, ok := d.Options[key]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

// WithOption returns a copy of the descriptor with one option set.
func (d Descriptor) WithOption(key, value string) Descriptor {
	out := d
	out.Options = make(map[string]string, len(d.Options)+1)
	for k, v := range d.Options {
		out.Options[k] = v
	}
	out.Options[key] = value
	out.Fields = append([]string(nil), d.Fields...)
	return out
}
