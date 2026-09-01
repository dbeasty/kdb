package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

func jsonGet(docJSON, path string) (any, error) {
	if path == "$" {
		var v any
		if err := json.Unmarshal([]byte(docJSON), &v); err != nil {
			return nil, err
		}
		return v, nil
	}
	if !strings.HasPrefix(path, "$.") {
		return nil, fmt.Errorf("unsupported json path: %s", path)
	}
	var root any
	if err := json.Unmarshal([]byte(docJSON), &root); err != nil {
		return nil, err
	}
	cur := root
	for _, part := range strings.Split(path[2:], ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, nil
		}
		cur, ok = m[part]
		if !ok {
			return nil, nil
		}
	}
	return cur, nil
}

func isJSONNull(v any) bool {
	if v == nil {
		return true
	}
	return false
}

// FieldValue extracts one top-level field's decoded JSON value from a document body. It is
// jsonGet's exported, single-segment form, for callers outside this package that need a field's
// value rather than a full validation pass (go/kdb/transaction's unique-constraint enforcement).
// A missing field and an explicit JSON null are indistinguishable here - both return (nil, nil) -
// which is exactly the distinction unique enforcement does not want to draw either (see
// UniqueKeysFor).
func FieldValue(docJSON, fieldName string) (any, error) {
	return jsonGet(docJSON, "$."+fieldName)
}
