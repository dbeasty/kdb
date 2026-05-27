package document

import (
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
)

// EnsureIDInJSON returns jsonText unchanged if the root object already has an "id" key;
// otherwise returns the object with "id" set to id's canonical string form.
func EnsureIDInJSON(jsonText string, id codec.UUID) (string, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonText), &root); err != nil {
		return "", NewDecodeError("invalid json", nil, err)
	}
	if root == nil {
		return "", NewDecodeError("root must be a JSON object", nil, nil)
	}
	if _, ok := root["id"]; ok {
		return jsonText, nil
	}
	idBytes, err := json.Marshal(id.String())
	if err != nil {
		return "", err
	}
	out := make(map[string]json.RawMessage, len(root)+1)
	out["id"] = idBytes
	for k, v := range root {
		out[k] = v
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", fmt.Errorf("marshal json with id: %w", err)
	}
	return string(b), nil
}
