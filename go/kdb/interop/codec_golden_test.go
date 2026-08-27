package interop

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/codec/schema"
)

// Golden fixtures are produced by Kotlin KdbLayer0CodecTest (see export script in README).
// Go must decode Kotlin-encoded bytes and re-encode to the same bytes.

func TestKotlinGoldenDocInt32(t *testing.T) {
	reg := mkDemoDoc(t)
	typ := schema.Ref{FullyQualifiedName: "demo.Doc"}
	raw := readGolden(t, "codec/doc_n_minus3.hex")
	v, err := codec.DecodeBytes(raw, typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	rv := v.(codec.RecordValue)
	if rv.Fields[1].(codec.Int32Value).V != -3 {
		t.Fatalf("n=%d", rv.Fields[1].(codec.Int32Value).V)
	}
	re, err := codec.EncodeBytes(v, typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesEqual(raw, re) {
		t.Fatalf("re-encode mismatch\nkotlin %s\ngo     %s", hex.EncodeToString(raw), hex.EncodeToString(re))
	}
}

func TestKotlinGoldenPrimitives(t *testing.T) {
	reg := schema.BuiltinRegistry()
	cases := []struct {
		file  string
		typ   schema.Type
		check func(codec.Value) bool
	}{
		{"codec/int32_min.hex", schema.Prim(schema.PhysicalInt32), func(v codec.Value) bool {
			return v.(codec.Int32Value).V == -2147483648
		}},
		{"codec/string_utf8.hex", schema.Prim(schema.PhysicalString), func(v codec.Value) bool {
			return v.(codec.StringValue).V == "〰 KDB"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			raw := readGolden(t, tc.file)
			v, err := codec.DecodeBytes(raw, tc.typ, reg)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.check(v) {
				t.Fatal("value check failed")
			}
			re, err := codec.EncodeBytes(v, tc.typ, reg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytesEqual(raw, re) {
				t.Fatal("round-trip bytes differ from kotlin golden")
			}
		})
	}
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "golden", name)
	b, err := os.ReadFile(path)
	if err != nil {
		// also try from module root when running from go/
		path = filepath.Join("testdata", "golden", name)
		b, err = os.ReadFile(path)
	}
	if err != nil {
		t.Skipf("golden %s missing (run ExportGolden from Kotlin): %v", name, err)
	}
	raw, err := hex.DecodeString(string(trimSpace(b)))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == '\n' || b[0] == ' ') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == ' ') {
		b = b[:len(b)-1]
	}
	return b
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestGoCodecGoldenBytesStable(t *testing.T) {
	reg := mkDemoDoc(t)
	typ := schema.Ref{FullyQualifiedName: "demo.Doc"}
	v := codec.RecordValue{Fields: map[int]codec.Value{1: codec.Int32Value{V: -3}}}
	b, err := codec.EncodeBytes(v, typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	raw := readGolden(t, "codec/doc_n_minus3.hex")
	if !bytesEqual(b, raw) {
		t.Fatalf("golden mismatch: have %x want %x", b, raw)
	}
}

func mkDemoDoc(t *testing.T) *schema.Registry {
	t.Helper()
	reg := schema.NewRegistry()
	reg.RegisterRecord(&schema.RecordSchema{
		Name: "Doc", Namespace: "demo",
		Fields: []schema.FieldSchema{{ID: 1, Name: "n", Type: schema.Prim(schema.PhysicalInt32)}},
	})
	reg.Freeze()
	return reg
}
