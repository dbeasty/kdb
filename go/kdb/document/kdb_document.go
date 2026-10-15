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

// RebasedOnto carries the root-level change from before to d over onto onto: keys d added or
// changed take d's value, keys d dropped are removed, and every other key keeps onto's value.
// onto's key order is kept and new keys follow in d's order (as Merge orders them). A nil before
// is a creation, so every key counts as changed; a nil onto is an empty document. Values are
// compared by their JSON text, as Kotlin compares them, so both engines agree on what changed.
// Kotlin: KdbDocument.rebasedOnto.
//
// A logged write is the whole stored document, so this is how a merge re-applies one branch's
// write without overwriting the other branch's concurrent changes to other fields.
func (d Document) RebasedOnto(before, onto *Document) (Document, error) {
	after, err := rootObject(d.JSON, &d.ID)
	if err != nil {
		return Document{}, err
	}
	prev, target := kdbjson.ObjectValue{}, kdbjson.ObjectValue{}
	if before != nil {
		if prev, err = rootObject(before.JSON, &d.ID); err != nil {
			return Document{}, err
		}
	}
	if onto != nil {
		if target, err = rootObject(onto.JSON, &d.ID); err != nil {
			return Document{}, err
		}
	}
	changed := func(k string) bool {
		a, inAfter := after.Fields[k]
		b, inPrev := prev.Fields[k]
		if !inAfter || !inPrev {
			return inAfter != inPrev
		}
		return kdbjson.ToJSONString(a) != kdbjson.ToJSONString(b)
	}
	out := kdbjson.ObjectValue{Fields: map[string]kdbjson.Value{}}
	put := func(k string, v kdbjson.Value) {
		out.Keys = append(out.Keys, k)
		out.Fields[k] = v
	}
	for _, k := range target.Keys {
		_, inAfter := after.Fields[k]
		_, inPrev := prev.Fields[k]
		switch {
		case !inAfter:
			if !inPrev {
				put(k, target.Fields[k])
			}
		case changed(k):
			put(k, after.Fields[k])
		default:
			put(k, target.Fields[k])
		}
	}
	for _, k := range after.Keys {
		if _, inTarget := target.Fields[k]; !inTarget && changed(k) {
			put(k, after.Fields[k])
		}
	}
	return Document{ID: d.ID, JSON: kdbjson.ToJSONString(out)}, nil
}

func rootObject(jsonText string, id *codec.UUID) (kdbjson.ObjectValue, error) {
	v, err := kdbjson.ParseValue(jsonText)
	if err != nil {
		return kdbjson.ObjectValue{}, NewDecodeError("invalid document json", id, err)
	}
	o, ok := v.(kdbjson.ObjectValue)
	if !ok {
		return kdbjson.ObjectValue{}, NewDecodeError("document root must be a JSON object", id, nil)
	}
	return o, nil
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
