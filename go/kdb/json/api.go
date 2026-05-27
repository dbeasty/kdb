package json

import (
	"reflect"

	kdberr "github.com/limidus/kdb/go/kdb/error"
)

// Get returns the value at path or nil if missing.
func Get(jsonText string, path *Path) (Value, error) {
	if path.HasWildcards() {
		return nil, kdberr.NewJsonPathError("wildcards not allowed in Get", path.Expression, nil)
	}
	root, err := ParseValue(jsonText)
	if err != nil {
		return nil, err
	}
	return navigateGet(root, path, 1), nil
}

// GetString is Get with path compiled from a string.
func GetString(jsonText, pathExpr string) (Value, error) {
	p, err := CompilePath(pathExpr)
	if err != nil {
		return nil, err
	}
	return Get(jsonText, p)
}

// GetAll collects all values matching a path (supports wildcards).
func GetAll(jsonText string, path *Path) ([]Value, error) {
	root, err := ParseValue(jsonText)
	if err != nil {
		return nil, err
	}
	if !path.HasWildcards() {
		if v := navigateGet(root, path, 1); v != nil {
			return []Value{v}, nil
		}
		return nil, nil
	}
	var out []Value
	collectAll(root, path.segments, 1, &out, path.Expression)
	return out, nil
}

// Set returns JSON with value set at path.
func Set(jsonText string, path *Path, value Value) (string, error) {
	if path.HasWildcards() {
		return "", kdberr.NewJsonPathError("wildcards not allowed in Set", path.Expression, nil)
	}
	root, err := ParseValue(jsonText)
	if err != nil {
		return "", err
	}
	obj, ok := root.(ObjectValue)
	if !ok {
		return "", kdberr.NewJsonPathError("root must be JSON object", path.Expression, nil)
	}
	if len(path.segments) < 2 {
		return "", kdberr.NewJsonPathError("cannot set root", path.Expression, nil)
	}
	updated, err := jsonSet(obj, path.segments, 1, value, path.Expression)
	if err != nil {
		return "", err
	}
	return ToJSONString(updated), nil
}

// Delete removes the value at path (no-op if missing).
func Delete(jsonText string, path *Path) (string, error) {
	if path.HasWildcards() {
		return "", kdberr.NewJsonPathError("wildcards not allowed in Delete", path.Expression, nil)
	}
	if len(path.segments) < 2 {
		return jsonText, nil
	}
	root, err := ParseValue(jsonText)
	if err != nil {
		return "", err
	}
	updated, err := jsonDelete(root, path.segments, 1, path.Expression)
	if err != nil {
		return "", err
	}
	return ToJSONString(updated), nil
}

// Merge shallow-merges two JSON objects at the root.
func Merge(jsonText, patchJSON string) (string, error) {
	base, err := ParseValue(jsonText)
	if err != nil {
		return "", err
	}
	patch, err := ParseValue(patchJSON)
	if err != nil {
		return "", err
	}
	bo, ok := base.(ObjectValue)
	if !ok {
		return "", kdberr.NewJsonPathError("left must be JSON object", "$", nil)
	}
	po, ok := patch.(ObjectValue)
	if !ok {
		return "", kdberr.NewJsonPathError("patch must be JSON object", "$", nil)
	}
	out := make(map[string]Value, len(bo.Fields)+len(po.Fields))
	keys := append([]string(nil), bo.Keys...)
	for k, v := range bo.Fields {
		out[k] = v
	}
	for k, v := range po.Fields {
		if _, exists := out[k]; exists {
			out[k] = v
			continue
		}
		out[k] = v
		keys = append(keys, k)
	}
	return ToJSONString(newObject(out, keys)), nil
}

// Contains reports whether an array at path contains value.
func Contains(jsonText string, path *Path, value Value) (bool, error) {
	if path.HasWildcards() {
		return false, kdberr.NewJsonPathError("wildcards not allowed in Contains", path.Expression, nil)
	}
	root, err := ParseValue(jsonText)
	if err != nil {
		return false, err
	}
	target := navigateGet(root, path, 1)
	if target == nil {
		return false, nil
	}
	if _, ok := target.(NullValue); ok {
		return false, nil
	}
	arr, ok := target.(ArrayValue)
	if !ok {
		return false, kdberr.NewJsonPathError("not array", path.Expression, nil)
	}
	for _, el := range arr.Elements {
		if deepEqual(el, value) {
			return true, nil
		}
	}
	return false, nil
}

// Keys returns object field names at path.
func Keys(jsonText string, path *Path) ([]string, error) {
	if path.HasWildcards() {
		return nil, kdberr.NewJsonPathError("wildcards not allowed in Keys", path.Expression, nil)
	}
	root, err := ParseValue(jsonText)
	if err != nil {
		return nil, err
	}
	var target Value
	if len(path.segments) == 1 {
		target = root
	} else {
		target = navigateGet(root, path, 1)
	}
	if target == nil {
		return nil, nil
	}
	o, ok := target.(ObjectValue)
	if !ok {
		return nil, kdberr.NewJsonPathError("not object", path.Expression, nil)
	}
	return append([]string(nil), o.Keys...), nil
}

// TypeName returns the JSON type name at path.
func TypeName(jsonText string, path *Path) (string, error) {
	if path.HasWildcards() {
		return "", kdberr.NewJsonPathError("wildcards not allowed in TypeName", path.Expression, nil)
	}
	root, err := ParseValue(jsonText)
	if err != nil {
		return "", err
	}
	var target Value
	if len(path.segments) == 1 {
		target = root
	} else {
		target = navigateGet(root, path, 1)
	}
	if target == nil {
		return "", nil
	}
	return jsonTypeName(target), nil
}

// ArrayLength returns array length at path.
func ArrayLength(jsonText string, path *Path) (*int, error) {
	if path.HasWildcards() {
		return nil, kdberr.NewJsonPathError("wildcards not allowed in ArrayLength", path.Expression, nil)
	}
	root, err := ParseValue(jsonText)
	if err != nil {
		return nil, err
	}
	var target Value
	if len(path.segments) == 1 {
		target = root
	} else {
		target = navigateGet(root, path, 1)
	}
	if target == nil {
		return nil, nil
	}
	if _, ok := target.(NullValue); ok {
		return nil, nil
	}
	a, ok := target.(ArrayValue)
	if !ok {
		return nil, kdberr.NewJsonPathError("not array", path.Expression, nil)
	}
	n := len(a.Elements)
	return &n, nil
}

func jsonTypeName(v Value) string {
	switch v.(type) {
	case StringValue:
		return "string"
	case NumberValue, IntValue:
		return "number"
	case BoolValue:
		return "boolean"
	case NullValue:
		return "null"
	case ObjectValue:
		return "object"
	case ArrayValue:
		return "array"
	default:
		return "unknown"
	}
}

func navigateGet(root Value, path *Path, startIdx int) Value {
	cur := root
	for si := startIdx; si < len(path.segments); si++ {
		switch seg := path.segments[si].(type) {
		case rootSeg:
			return nil
		case fieldSeg:
			o, ok := cur.(ObjectValue)
			if !ok {
				panic(kdberr.NewJsonPathError("not object", path.Expression, nil))
			}
			next, ok := o.Fields[seg.name]
			if !ok {
				return nil
			}
			cur = next
		case idxSeg:
			a, ok := cur.(ArrayValue)
			if !ok {
				panic(kdberr.NewJsonPathError("not array", path.Expression, nil))
			}
			ix := normalizeIndex(seg.index, len(a.Elements), path.Expression, true)
			if ix < 0 || ix >= len(a.Elements) {
				return nil
			}
			cur = a.Elements[ix]
		case wildcardElemSeg, wildcardFieldSeg:
			panic(kdberr.NewJsonPathError("wildcard in Get", path.Expression, nil))
		}
	}
	return cur
}

func normalizeIndex(index, length int, expr string, forGet bool) int {
	if index == -1 {
		if length == 0 {
			return -1
		}
		return length - 1
	}
	if index < -1 {
		panic(kdberr.NewJsonPathError("bad array index", expr, nil))
	}
	if !forGet && index < 0 {
		panic(kdberr.NewJsonPathError("bad array index", expr, nil))
	}
	return index
}

func collectAll(cur Value, segs []pathSeg, idx int, out *[]Value, expr string) {
	if idx == len(segs) {
		*out = append(*out, cur)
		return
	}
	switch seg := segs[idx].(type) {
	case rootSeg:
		collectAll(cur, segs, idx+1, out, expr)
	case fieldSeg:
		o, ok := cur.(ObjectValue)
		if !ok {
			panic(kdberr.NewJsonPathError("not object", expr, nil))
		}
		next, ok := o.Fields[seg.name]
		if !ok {
			return
		}
		collectAll(next, segs, idx+1, out, expr)
	case idxSeg:
		a, ok := cur.(ArrayValue)
		if !ok {
			panic(kdberr.NewJsonPathError("not array", expr, nil))
		}
		ix := normalizeIndex(seg.index, len(a.Elements), expr, true)
		if ix < 0 || ix >= len(a.Elements) {
			return
		}
		collectAll(a.Elements[ix], segs, idx+1, out, expr)
	case wildcardElemSeg:
		a, ok := cur.(ArrayValue)
		if !ok {
			panic(kdberr.NewJsonPathError("not array", expr, nil))
		}
		for _, e := range a.Elements {
			collectAll(e, segs, idx+1, out, expr)
		}
	case wildcardFieldSeg:
		o, ok := cur.(ObjectValue)
		if !ok {
			panic(kdberr.NewJsonPathError("not object", expr, nil))
		}
		for _, e := range o.Fields {
			collectAll(e, segs, idx+1, out, expr)
		}
	}
}

func jsonSet(cur Value, segs []pathSeg, idx int, newVal Value, expr string) (Value, error) {
	seg := segs[idx]
	last := idx == len(segs)-1
	switch s := seg.(type) {
	case fieldSeg:
		obj, ok := cur.(ObjectValue)
		if !ok {
			return nil, kdberr.NewJsonPathError("not object", expr, nil)
		}
		copy := make(map[string]Value, len(obj.Fields))
		keys := append([]string(nil), obj.Keys...)
		for k, v := range obj.Fields {
			copy[k] = v
		}
		if last {
			if _, ok := copy[s.name]; !ok {
				keys = append(keys, s.name)
			}
			copy[s.name] = newVal
		} else {
			old, ok := copy[s.name]
			if !ok {
				old = newObject(map[string]Value{}, nil)
				keys = append(keys, s.name)
			}
			updated, err := jsonSet(old, segs, idx+1, newVal, expr)
			if err != nil {
				return nil, err
			}
			copy[s.name] = updated
		}
		return newObject(copy, keys), nil
	case idxSeg:
		arr, ok := cur.(ArrayValue)
		if !ok {
			return nil, kdberr.NewJsonPathError("not array", expr, nil)
		}
		list := append([]Value(nil), arr.Elements...)
		ix := normalizeIndex(s.index, len(list), expr, false)
		for len(list) <= ix {
			list = append(list, NullValue{})
		}
		if last {
			list[ix] = newVal
		} else {
			old := list[ix]
			if _, ok := old.(NullValue); ok {
				old = newObject(map[string]Value{}, nil)
			}
			updated, err := jsonSet(old, segs, idx+1, newVal, expr)
			if err != nil {
				return nil, err
			}
			list[ix] = updated
		}
		return ArrayValue{Elements: list}, nil
	default:
		return nil, kdberr.NewJsonPathError("invalid segment for set", expr, nil)
	}
}

func jsonDelete(cur Value, segs []pathSeg, idx int, expr string) (Value, error) {
	seg := segs[idx]
	last := idx == len(segs)-1
	switch s := seg.(type) {
	case fieldSeg:
		obj, ok := cur.(ObjectValue)
		if !ok {
			return nil, kdberr.NewJsonPathError("not object", expr, nil)
		}
		if last {
			if _, ok := obj.Fields[s.name]; !ok {
				return cur, nil
			}
			copy := make(map[string]Value, len(obj.Fields)-1)
			keys := make([]string, 0, len(obj.Keys)-1)
			for _, k := range obj.Keys {
				if k == s.name {
					continue
				}
				keys = append(keys, k)
				copy[k] = obj.Fields[k]
			}
			return newObject(copy, keys), nil
		}
		child, ok := obj.Fields[s.name]
		if !ok {
			return cur, nil
		}
		newChild, err := jsonDelete(child, segs, idx+1, expr)
		if err != nil {
			return nil, err
		}
		copy := make(map[string]Value, len(obj.Fields))
		for k, v := range obj.Fields {
			copy[k] = v
		}
		copy[s.name] = newChild
		return newObject(copy, append([]string(nil), obj.Keys...)), nil
	case idxSeg:
		arr, ok := cur.(ArrayValue)
		if !ok {
			return nil, kdberr.NewJsonPathError("not array", expr, nil)
		}
		if last {
			ix := normalizeIndex(s.index, len(arr.Elements), expr, true)
			if ix < 0 || ix >= len(arr.Elements) {
				return cur, nil
			}
			list := append([]Value(nil), arr.Elements...)
			list = append(list[:ix], list[ix+1:]...)
			return ArrayValue{Elements: list}, nil
		}
		ix := normalizeIndex(s.index, len(arr.Elements), expr, true)
		if ix < 0 || ix >= len(arr.Elements) {
			return cur, nil
		}
		list := append([]Value(nil), arr.Elements...)
		updated, err := jsonDelete(list[ix], segs, idx+1, expr)
		if err != nil {
			return nil, err
		}
		list[ix] = updated
		return ArrayValue{Elements: list}, nil
	default:
		return nil, kdberr.NewJsonPathError("invalid segment for delete", expr, nil)
	}
}

func deepEqual(a, b Value) bool {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return numericEquals(a, b)
	}
	switch av := a.(type) {
	case StringValue:
		return av.V == b.(StringValue).V
	case BoolValue:
		return av.V == b.(BoolValue).V
	case NullValue:
		_, ok := b.(NullValue)
		return ok
	case IntValue:
		switch bv := b.(type) {
		case IntValue:
			return av.V == bv.V
		case NumberValue:
			return float64(av.V) == bv.V
		default:
			return false
		}
	case NumberValue:
		switch bv := b.(type) {
		case NumberValue:
			return av.V == bv.V
		case IntValue:
			return av.V == float64(bv.V)
		default:
			return false
		}
	case ArrayValue:
		bv := b.(ArrayValue)
		if len(av.Elements) != len(bv.Elements) {
			return false
		}
		for i := range av.Elements {
			if !deepEqual(av.Elements[i], bv.Elements[i]) {
				return false
			}
		}
		return true
	case ObjectValue:
		bv := b.(ObjectValue)
		if len(av.Keys) != len(bv.Keys) {
			return false
		}
		for i, k := range av.Keys {
			if k != bv.Keys[i] {
				return false
			}
			if !deepEqual(av.Fields[k], bv.Fields[k]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func numericEquals(a, b Value) bool {
	switch av := a.(type) {
	case IntValue:
		if bv, ok := b.(NumberValue); ok {
			return float64(av.V) == bv.V
		}
	case NumberValue:
		if bv, ok := b.(IntValue); ok {
			return av.V == float64(bv.V)
		}
	}
	return false
}
