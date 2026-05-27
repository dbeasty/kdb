package json

import (
	"errors"
	"testing"

	kdberr "github.com/limidus/kdb/go/kdb/error"
)

func TestGetTopLevelField(t *testing.T) {
	v, err := GetString(`{"a":1}`, "$.a")
	if err != nil {
		t.Fatal(err)
	}
	iv, ok := v.(IntValue)
	if !ok || iv.V != 1 {
		t.Fatal("expected int 1")
	}
}

func TestGetNestedField(t *testing.T) {
	v, err := GetString(`{"a":{"b":"hello"}}`, "$.a.b")
	if err != nil {
		t.Fatal(err)
	}
	sv, ok := v.(StringValue)
	if !ok || sv.V != "hello" {
		t.Fatal("expected hello")
	}
}

func TestGetArrayElement(t *testing.T) {
	v, err := GetString(`{"tags":["x","y"]}`, "$.tags[0]")
	if err != nil {
		t.Fatal(err)
	}
	sv, ok := v.(StringValue)
	if !ok || sv.V != "x" {
		t.Fatal("expected x")
	}
}

func TestGetMissingPathReturnsNil(t *testing.T) {
	v, err := GetString(`{"a":1}`, "$.z")
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatal("expected nil")
	}
}

func TestGetJSONNull(t *testing.T) {
	v, err := GetString(`{"a":null}`, "$.a")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := v.(NullValue); !ok {
		t.Fatal("expected null")
	}
}

func TestSetNewField(t *testing.T) {
	out, err := SetString(`{"a":1}`, "$.b", StringValue{V: "v"})
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"a":1,"b":"v"}` {
		t.Fatalf("got %q", out)
	}
}

func TestSetOverwriteField(t *testing.T) {
	out, err := SetString(`{"a":1}`, "$.a", IntValue{V: 99})
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"a":99}` {
		t.Fatalf("got %q", out)
	}
}

func TestSetCreatesIntermediateObject(t *testing.T) {
	out, err := SetString(`{}`, "$.a.b.c", BoolValue{V: true})
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"a":{"b":{"c":true}}}` {
		t.Fatalf("got %q", out)
	}
}

func TestDeleteExistingField(t *testing.T) {
	out, err := DeleteString(`{"a":1,"b":2}`, "$.a")
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"b":2}` {
		t.Fatalf("got %q", out)
	}
}

func TestMergeRootKeys(t *testing.T) {
	out, err := Merge(`{"a":1,"b":2}`, `{"b":99,"c":3}`)
	if err != nil {
		t.Fatal(err)
	}
	if out != `{"a":1,"b":99,"c":3}` {
		t.Fatalf("got %q", out)
	}
}

func TestGetAllWildcard(t *testing.T) {
	p, err := CompilePath("$.*")
	if err != nil {
		t.Fatal(err)
	}
	all, err := GetAll(`{"a":1,"b":2}`, p)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("len %d", len(all))
	}
}

func TestInvalidPathThrows(t *testing.T) {
	_, err := CompilePath("not-a-path")
	var jpe *kdberr.JsonPathError
	if !errors.As(err, &jpe) {
		t.Fatal("expected JsonPathError")
	}
}

func TestJSONValueRoundTrip(t *testing.T) {
	v := newObject(map[string]Value{
		"a": ArrayValue{Elements: []Value{IntValue{V: 1}, StringValue{V: "z"}}},
		"b": NullValue{},
	}, []string{"a", "b"})
	back, err := ParseValue(ToJSONString(v))
	if err != nil {
		t.Fatal(err)
	}
	if !deepEqual(v, back) {
		t.Fatal("round trip mismatch")
	}
}

func TestJSONValueKdbRoundTrip(t *testing.T) {
	v := newObject(map[string]Value{
		"n": IntValue{V: 42},
		"f": NumberValue{V: 1.5},
	}, []string{"n", "f"})
	kv, err := ToKdbValue(v)
	if err != nil {
		t.Fatal(err)
	}
	back, err := FromKdbValue(kv)
	if err != nil {
		t.Fatal(err)
	}
	if !deepEqual(v, back) {
		t.Fatal("kdb round trip mismatch")
	}
}

func SetString(jsonText, pathExpr string, value Value) (string, error) {
	p, err := CompilePath(pathExpr)
	if err != nil {
		return "", err
	}
	return Set(jsonText, p, value)
}

func DeleteString(jsonText, pathExpr string) (string, error) {
	p, err := CompilePath(pathExpr)
	if err != nil {
		return "", err
	}
	return Delete(jsonText, p)
}
