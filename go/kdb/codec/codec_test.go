package codec

import (
	"bytes"
	"math"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec/schema"
	kdberr "github.com/limidus/kdb/go/kdb/error"
)

var (
	pInt32   = schema.Prim(schema.PhysicalInt32)
	pInt64   = schema.Prim(schema.PhysicalInt64)
	pFloat64 = schema.Prim(schema.PhysicalFloat64)
	pBool    = schema.Prim(schema.PhysicalBool)
	pStr     = schema.Prim(schema.PhysicalString)
)

func roundtrip(t *testing.T, reg *schema.Registry, v Value, typ schema.Type) Value {
	t.Helper()
	out, err := Roundtrip(v, typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestRoundtripNull(t *testing.T) {
	reg := schema.BuiltinRegistry()
	got := roundtrip(t, reg, Null, schema.Prim(schema.PhysicalNull))
	if _, ok := got.(NullValue); !ok {
		t.Fatal("expected null")
	}
}

func TestRoundtripCorePrimitives(t *testing.T) {
	reg := schema.BuiltinRegistry()
	if v := roundtrip(t, reg, Int32Value{V: math.MinInt32}, pInt32); v.(Int32Value).V != math.MinInt32 {
		t.Fatal("int32")
	}
	if v := roundtrip(t, reg, Int64Value{V: math.MaxInt64}, pInt64); v.(Int64Value).V != math.MaxInt64 {
		t.Fatal("int64")
	}
	if v := roundtrip(t, reg, Float64Value{V: -1.5}, pFloat64); v.(Float64Value).V != -1.5 {
		t.Fatal("float64")
	}
	if v := roundtrip(t, reg, BoolValue{V: true}, pBool); !v.(BoolValue).V {
		t.Fatal("bool")
	}
	if v := roundtrip(t, reg, StringValue{V: "〰 KDB"}, pStr); v.(StringValue).V != "〰 KDB" {
		t.Fatal("string")
	}
}

func TestNestedRecordArrayRoundtrip(t *testing.T) {
	inner := &schema.RecordSchema{
		Name: "Inner", Namespace: "t",
		Fields: []schema.FieldSchema{{ID: 1, Name: "x", Type: pInt32}},
	}
	root := &schema.RecordSchema{
		Name: "Root", Namespace: "t",
		Fields: []schema.FieldSchema{{ID: 2, Name: "a", Type: schema.Array{Element: schema.Ref{"t.Inner"}}}},
	}
	reg := schema.NewRegistry()
	reg.RegisterRecord(inner)
	reg.RegisterRecord(root)
	reg.Freeze()

	innerVal := RecordValue{Fields: map[int]Value{1: Int32Value{V: 7}}}
	outer := RecordValue{Fields: map[int]Value{2: ArrayValue{Elements: []Value{innerVal}}}}
	got := roundtrip(t, reg, outer, schema.Ref{"t.Root"})
	ogr := got.(RecordValue)
	if len(ogr.Fields[2].(ArrayValue).Elements) != 1 {
		t.Fatal("array len")
	}
}

func TestEncodedSizeEqualsBytes(t *testing.T) {
	reg := schema.NewRegistry()
	fields := make([]schema.FieldSchema, 50)
	for i := 0; i < 50; i++ {
		fields[i] = schema.FieldSchema{ID: i + 1, Name: "f", Type: pStr}
	}
	reg.RegisterRecord(&schema.RecordSchema{Name: "Wide", Namespace: "t", Fields: fields})
	reg.Freeze()

	fv := make(map[int]Value, 50)
	for i := 0; i < 50; i++ {
		fv[i+1] = StringValue{V: "payload"}
	}
	rv := RecordValue{Fields: fv}
	blob, err := EncodeBytes(rv, schema.Ref{"t.Wide"}, reg)
	if err != nil {
		t.Fatal(err)
	}
	n, err := EncodedSize(rv, schema.Ref{"t.Wide"}, reg)
	if err != nil || n != len(blob) {
		t.Fatalf("size %d blob %d", n, len(blob))
	}
}

func TestTruncatedPayloadThrows(t *testing.T) {
	reg := mkDocReg(t, []fieldPair{{"n", pInt32}})
	typ := schema.Ref{"demo.Doc"}
	blob, err := EncodeBytes(RecordValue{Fields: map[int]Value{1: Int32Value{V: -3}}}, typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	bad := blob[:len(blob)-1]
	_, err = DecodeBytes(bad, typ, reg)
	if err == nil {
		t.Fatal("expected decode error")
	}
	if kdberr.CodeOf(err) != kdberr.KdbDecodeError {
		t.Fatalf("code %v", kdberr.CodeOf(err))
	}
}

func TestSourceReadsFirstValueBoundary(t *testing.T) {
	reg := mkDocReg(t, []fieldPair{{"n", pInt32}})
	typ := schema.Ref{"demo.Doc"}
	mk := func(n int32) []byte {
		b, err := EncodeBytes(RecordValue{Fields: map[int]Value{1: Int32Value{V: n}}}, typ, reg)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	a, b := mk(-3), mk(999)
	combined := append(append([]byte{}, a...), b...)
	first, err := DecodeFrom(bytes.NewReader(combined), typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	if first.(RecordValue).Fields[1].(Int32Value).V != -3 {
		t.Fatal("first value")
	}
	tail := combined[len(a):]
	second, err := DecodeBytes(tail, typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	if second.(RecordValue).Fields[1].(Int32Value).V != 999 {
		t.Fatal("second value")
	}
}

type fieldPair struct {
	name string
	typ  schema.Type
}

func mkDocReg(t *testing.T, props []fieldPair) *schema.Registry {
	t.Helper()
	reg := schema.NewRegistry()
	fields := make([]schema.FieldSchema, len(props))
	for i, p := range props {
		fields[i] = schema.FieldSchema{ID: i + 1, Name: p.name, Type: p.typ}
	}
	reg.RegisterRecord(&schema.RecordSchema{Name: "Doc", Namespace: "demo", Fields: fields})
	reg.Freeze()
	return reg
}
