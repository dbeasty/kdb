package document

import (
	"encoding/json"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
)

// ResolveID reads a document body's top-level "id" and maps it to the document's identity per
// kdb-spec-layer16 §9.4: a UUID string is the identity itself; any other non-empty string s is
// the derived id codec.DerivedUUID(s). supplied is false when the body has no "id" at all - the
// caller mints a random UUID and reports it, and the body is stored untouched either way.
//
// An "id" that is not a JSON string, or is the empty string, is an error rather than a silent
// substitution: a caller who wrote one meant something by it, and writing under a fresh random
// id they never asked for and cannot learn about is the failure mode 1-G3 already rejected.
func ResolveID(jsonText string) (id codec.UUID, supplied bool, err error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonText), &root); err != nil {
		return codec.UUID{}, false, NewDecodeError("invalid json", nil, err)
	}
	if root == nil {
		return codec.UUID{}, false, NewDecodeError("root must be a JSON object", nil, nil)
	}
	idRaw, ok := root["id"]
	if !ok {
		return codec.UUID{}, false, nil
	}
	var idStr string
	if err := json.Unmarshal(idRaw, &idStr); err != nil {
		return codec.UUID{}, true, fmt.Errorf("kdb: \"id\" field must be a string, got %s", idRaw)
	}
	if idStr == "" {
		return codec.UUID{}, true, fmt.Errorf("kdb: \"id\" field must not be empty")
	}
	if parsed, err := codec.ParseUUID(idStr); err == nil {
		return parsed, true, nil
	}
	return codec.DerivedUUID(idStr), true, nil
}
