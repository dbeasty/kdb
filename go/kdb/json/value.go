package json

import (
	"github.com/limidus/kdb/go/kdb/codec"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// Value is a typed JSON tree for JSON functions and codec bridging.
type Value interface {
	isJSONValue()
}

type StringValue struct{ V string }

func (StringValue) isJSONValue() {}

type NumberValue struct{ V float64 }

func (NumberValue) isJSONValue() {}

type IntValue struct{ V int64 }

func (IntValue) isJSONValue() {}

type BoolValue struct{ V bool }

func (BoolValue) isJSONValue() {}

type NullValue struct{}

func (NullValue) isJSONValue() {}

type ObjectValue struct {
	Keys   []string
	Fields map[string]Value
}

func newObject(fields map[string]Value, keys []string) ObjectValue {
	if fields == nil {
		fields = map[string]Value{}
	}
	if keys == nil {
		keys = make([]string, 0, len(fields))
		for k := range fields {
			keys = append(keys, k)
		}
	}
	return ObjectValue{Keys: keys, Fields: fields}
}

func (ObjectValue) isJSONValue() {}

type ArrayValue struct {
	Elements []Value
}

func (ArrayValue) isJSONValue() {}

// ToJSONString serialises the value to compact JSON text.
func ToJSONString(v Value) string {
	return writeValue(v)
}

// ParseValue parses a complete JSON document.
func ParseValue(json string) (Value, error) {
	return newParser(json).parseValue()
}

// FromKdbValue converts a codec value subtree to JSON.
func FromKdbValue(value codec.Value) (Value, error) {
	switch v := value.(type) {
	case codec.NullValue:
		return NullValue{}, nil
	case codec.BoolValue:
		return BoolValue{V: v.V}, nil
	case codec.Int64Value:
		return IntValue{V: v.V}, nil
	case codec.Float64Value:
		return NumberValue{V: v.V}, nil
	case codec.StringValue:
		return StringValue{V: v.V}, nil
	case codec.ArrayValue:
		els := make([]Value, len(v.Elements))
		for i, el := range v.Elements {
			jv, err := FromKdbValue(el)
			if err != nil {
				return nil, err
			}
			els[i] = jv
		}
		return ArrayValue{Elements: els}, nil
	case codec.MapValue:
		m := make(map[string]Value, len(v.Entries))
		keys := make([]string, 0, len(v.Entries))
		for _, e := range v.Entries {
			ks, ok := e.Key.(codec.StringValue)
			if !ok {
				return nil, kdberr.NewJsonPathError("Map key must be string", "$", nil)
			}
			jv, err := FromKdbValue(e.Val)
			if err != nil {
				return nil, err
			}
			m[ks.V] = jv
			keys = append(keys, ks.V)
		}
		return newObject(m, keys), nil
	default:
		return nil, kdberr.NewJsonPathError("unsupported KdbValue for JSON subtree", "$", nil)
	}
}

// ToKdbValue converts this JSON value to a codec value.
func ToKdbValue(v Value) (codec.Value, error) {
	switch t := v.(type) {
	case StringValue:
		return codec.StringValue{V: t.V}, nil
	case NumberValue:
		return codec.Float64Value{V: t.V}, nil
	case IntValue:
		return codec.Int64Value{V: t.V}, nil
	case BoolValue:
		return codec.BoolValue{V: t.V}, nil
	case NullValue:
		return codec.Null, nil
	case ObjectValue:
		entries := make([]codec.MapEntry, 0, len(t.Keys))
		for _, k := range t.Keys {
			val := t.Fields[k]
			kv := codec.StringValue{V: k}
			vv, err := ToKdbValue(val)
			if err != nil {
				return nil, err
			}
			entries = append(entries, codec.MapEntry{Key: kv, Val: vv})
		}
		return codec.MapValue{Entries: entries}, nil
	case ArrayValue:
		els := make([]codec.Value, len(t.Elements))
		for i, el := range t.Elements {
			vv, err := ToKdbValue(el)
			if err != nil {
				return nil, err
			}
			els[i] = vv
		}
		return codec.ArrayValue{Elements: els}, nil
	default:
		return nil, kdberr.NewJsonPathError("unknown json value", "$", nil)
	}
}
