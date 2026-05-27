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
