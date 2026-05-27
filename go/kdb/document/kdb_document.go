package document

import (
	"encoding/json"

	"github.com/limidus/kdb/go/kdb/codec"
	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// Document is a KDB document: stable identity + canonical JSON object text.
type Document struct {
	ID   codec.UUID
	JSON string
}

func (d Document) ToDocumentBodyValue() codec.Value {
	return codec.RecordValue{Fields: map[int]codec.Value{
		1: uuidVal(d.ID),
		2: codec.StringValue{V: d.JSON},
	}}
}

// ContentHash is SHA-256 of canonical DocumentBody bytes.
func (d Document) ContentHash() (codec.Hash, error) {
	return ComputeContentHash(d)
}

// Merge applies a shallow root-level JSON object patch.
func (d Document) Merge(patchJSON string) (Document, error) {
	if _, err := kdbjson.ParseValue(d.JSON); err != nil {
		return Document{}, NewDecodeError("invalid document json", &d.ID, err)
	}
	if _, err := kdbjson.ParseValue(patchJSON); err != nil {
		return Document{}, NewDecodeError("invalid patch json", &d.ID, err)
	}
	out, err := kdbjson.Merge(d.JSON, patchJSON)
	if err != nil {
		return Document{}, NewDecodeError("merge failed", &d.ID, err)
	}
	return Document{ID: d.ID, JSON: out}, nil
}

// WithJSON returns a copy with new JSON after validation.
func (d Document) WithJSON(newJSON string) (Document, error) {
	if err := validateObjectJSON(newJSON); err != nil {
		return Document{}, err
	}
	return Document{ID: d.ID, JSON: newJSON}, nil
}

// FromJSON creates a document with a random id.
func FromJSON(jsonText string) (Document, error) {
	if err := validateObjectJSON(jsonText); err != nil {
		return Document{}, err
	}
	id, err := codec.RandomUUID()
	if err != nil {
		return Document{}, err
	}
	return Document{ID: id, JSON: jsonText}, nil
}

// FromJSONWithID creates a document with the given id.
func FromJSONWithID(id codec.UUID, jsonText string) (Document, error) {
	if err := validateObjectJSON(jsonText); err != nil {
		return Document{}, err
	}
	return Document{ID: id, JSON: jsonText}, nil
}

// FromDocumentBodyValue decodes a DocumentBody record.
func FromDocumentBodyValue(value codec.Value) (Document, error) {
	rec, ok := value.(codec.RecordValue)
	if !ok {
		return Document{}, NewDecodeError("expected DocumentBody record", nil, nil)
	}
	idVal, ok := rec.Fields[1].(codec.UUIDValue)
	if !ok {
		return Document{}, NewDecodeError("DocumentBody missing id", nil, nil)
	}
	id := codec.UUID{MSB: idVal.MSB, LSB: idVal.LSB}
	js, ok := rec.Fields[2].(codec.StringValue)
	if !ok {
		return Document{}, NewDecodeError("DocumentBody missing json", nil, nil)
	}
	if err := validateObjectJSON(js.V); err != nil {
		if de, ok := err.(*DecodeError); ok {
			return Document{}, NewDecodeError(de.msg, &id, de.cause)
		}
		return Document{}, NewDecodeError(err.Error(), &id, err)
	}
	return Document{ID: id, JSON: js.V}, nil
}

func validateObjectJSON(jsonText string) error {
	var v any
	if err := json.Unmarshal([]byte(jsonText), &v); err != nil {
		return NewDecodeError("invalid json", nil, err)
	}
	if _, ok := v.(map[string]any); !ok {
		return NewDecodeError("root must be a JSON object", nil, nil)
	}
	return nil
}

// ComputeContentHash returns SHA-256 of canonical DocumentBody bytes.
func ComputeContentHash(doc Document) (codec.Hash, error) {
	reg := WireRegistry()
	bytes, err := codec.EncodeBytes(doc.ToDocumentBodyValue(), DocumentBodyType, reg)
	if err != nil {
		return codec.Hash{}, err
	}
	return codec.HashFromBytes(SHA256Digest(bytes))
}
