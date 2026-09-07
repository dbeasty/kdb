package index

import (
	"fmt"
	"strings"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/json"
)

// ParseDocument parses a JSON document body for path evaluation.
func ParseDocument(jsonText string) (json.Value, error) {
	return json.ParseValue(jsonText)
}

// SplitFieldPath turns "title", "$.steps.text" or "steps.text" into its segments.
func SplitFieldPath(fieldPath string) []string {
	p := strings.TrimSpace(fieldPath)
	p = strings.TrimPrefix(p, "$.")
	p = strings.TrimPrefix(p, "$")
	p = strings.TrimPrefix(p, ".")
	if p == "" {
		return nil
	}
	return strings.Split(p, ".")
}

// PathValues evaluates a dotted field path with implicit array traversal (Layer 16 §2): when
// the value at a segment is an array the rest of the path is applied to every element, in
// document order, and when the final value is an array its elements are the candidates. It
// never panics: a segment applied to a non-object simply yields nothing.
func PathValues(root json.Value, fieldPath string) []json.Value {
	return walkPath(root, SplitFieldPath(fieldPath), true)
}

// PathValuesRaw is PathValues without flattening the final value: an array at the end of the
// path is returned as one candidate. Vector extraction needs the array itself.
func PathValuesRaw(root json.Value, fieldPath string) []json.Value {
	return walkPath(root, SplitFieldPath(fieldPath), false)
}

func walkPath(cur json.Value, segs []string, flattenFinal bool) []json.Value {
	if len(segs) == 0 {
		if arr, ok := cur.(json.ArrayValue); ok && flattenFinal {
			return append([]json.Value(nil), arr.Elements...)
		}
		return []json.Value{cur}
	}
	switch v := cur.(type) {
	case json.ObjectValue:
		next, ok := v.Fields[segs[0]]
		if !ok {
			return nil
		}
		return walkPath(next, segs[1:], flattenFinal)
	case json.ArrayValue:
		var out []json.Value
		for _, el := range v.Elements {
			out = append(out, walkPath(el, segs, flattenFinal)...)
		}
		return out
	default:
		return nil
	}
}

// FieldStrings returns every string candidate at fieldPath, in document order. Non-string
// candidates contribute nothing.
func FieldStrings(root json.Value, fieldPath string) []string {
	var out []string
	for _, v := range PathValues(root, fieldPath) {
		if s, ok := v.(json.StringValue); ok {
			out = append(out, s.V)
		}
	}
	return out
}

// FieldVector reads the first candidate at fieldPath as a vector. present is false when there
// is no candidate or the candidate is not an array; an array holding a non-number is an error.
func FieldVector(root json.Value, fieldPath string) (vec []float32, present bool, err error) {
	for _, v := range PathValuesRaw(root, fieldPath) {
		arr, ok := v.(json.ArrayValue)
		if !ok {
			continue
		}
		out := make([]float32, len(arr.Elements))
		for i, el := range arr.Elements {
			switch n := el.(type) {
			case json.NumberValue:
				out[i] = float32(n.V)
			case json.IntValue:
				out[i] = float32(n.V)
			default:
				return nil, true, fmt.Errorf("vector at %s: element %d is not a number", fieldPath, i)
			}
		}
		return out, true, nil
	}
	return nil, false, nil
}

// KeyFromJSONValue converts a scalar JSON value to an index key. fieldType is the schema
// field's codec type label ("" when unknown) and only refines the natural mapping: a
// TIMESTAMP string becomes a TimestampKey, an INT32 number an Int32Key, a UUID string a
// UUIDKey. Objects and arrays have no key.
func KeyFromJSONValue(v json.Value, fieldType string) (Key, bool) {
	switch t := v.(type) {
	case json.StringValue:
		switch strings.ToUpper(fieldType) {
		case "TIMESTAMP":
			if ts, err := codec.TimestampFromISO8601(t.V); err == nil {
				return TimestampKey{EpochMillis: ts.EpochMillis}, true
			}
		case "UUID":
			if id, err := codec.UUIDFromString(t.V); err == nil {
				return UUIDKey{ID: id}, true
			}
		}
		return StringKey{Value: t.V}, true
	case json.IntValue:
		switch strings.ToUpper(fieldType) {
		case "INT32":
			return Int32Key{Value: int32(t.V)}, true
		case "TIMESTAMP":
			return TimestampKey{EpochMillis: t.V}, true
		case "FLOAT64":
			return Float64Key{Value: float64(t.V)}, true
		}
		return Int64Key{Value: t.V}, true
	case json.NumberValue:
		switch strings.ToUpper(fieldType) {
		case "INT32":
			return Int32Key{Value: int32(t.V)}, true
		case "INT64":
			return Int64Key{Value: int64(t.V)}, true
		case "TIMESTAMP":
			return TimestampKey{EpochMillis: int64(t.V)}, true
		}
		return Float64Key{Value: t.V}, true
	case json.BoolValue:
		return BoolKey{Value: t.V}, true
	case json.NullValue:
		return NullKey{}, true
	default:
		return nil, false
	}
}
