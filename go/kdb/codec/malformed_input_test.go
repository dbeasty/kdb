package codec

import (
	"runtime"
	"testing"

	"github.com/limidus/kdb/go/kdb/codec/schema"
)

// The value decoder is fed bytes that came off the network (a peer's commit payload) and off
// disk (a delta page, an SSTable block). Both are outside this process's control, so a
// malformed length field has to end as an error - not as an allocation sized by whatever the
// input asked for, and not as a panic, since nothing on the frame-handling path recovers.

// hugeLeb is a nine-byte LEB128 varint for a count in the 2^60 range: small enough to encode in
// a handful of bytes, large enough that using it as a slice capacity is fatal.
var hugeLeb = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}

func TestDecodeRejectsArrayLengthLargerThanInput(t *testing.T) {
	typ := schema.Array{Element: schema.Prim(schema.PhysicalInt32)}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	if _, err := DecodeBytes(hugeLeb, typ, schema.BuiltinRegistry()); err == nil {
		t.Fatal("expected an error for an array length the input cannot back")
	}

	runtime.ReadMemStats(&after)
	const limit = 8 << 20
	if grew := after.TotalAlloc - before.TotalAlloc; grew > limit {
		t.Fatalf("decoding a %d-byte payload allocated %d bytes", len(hugeLeb), grew)
	}
}

func TestDecodeRejectsMapLengthLargerThanInput(t *testing.T) {
	typ := schema.Map{
		Key:   schema.Prim(schema.PhysicalString),
		Value: schema.Prim(schema.PhysicalInt32),
	}

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	if _, err := DecodeBytes(hugeLeb, typ, schema.BuiltinRegistry()); err == nil {
		t.Fatal("expected an error for a map length the input cannot back")
	}

	runtime.ReadMemStats(&after)
	const limit = 8 << 20
	if grew := after.TotalAlloc - before.TotalAlloc; grew > limit {
		t.Fatalf("decoding a %d-byte payload allocated %d bytes", len(hugeLeb), grew)
	}
}

// The bound is "no more elements than there are bytes left", so a count just past the remaining
// input is rejected while a plausible one is still allowed through to fail (or succeed) on its
// actual contents.
func TestDecodeArrayLengthBoundIsRemainingInput(t *testing.T) {
	typ := schema.Array{Element: schema.Prim(schema.PhysicalInt32)}
	reg := schema.BuiltinRegistry()

	// One length byte, then three bytes of body: a count of 4 is already more than the three
	// bytes left can hold.
	if _, err := DecodeBytes([]byte{0x04, 0x00, 0x00, 0x00}, typ, reg); err == nil {
		t.Fatal("expected an error for a count above the remaining byte count")
	}
	// A count within the remaining bytes gets as far as decoding elements, and fails there on
	// running out of input rather than being rejected up front - either way an error, but the
	// bound must not be the thing rejecting a plausible count.
	if _, err := DecodeBytes([]byte{0x02, 0x00, 0x00, 0x00}, typ, reg); err == nil {
		t.Fatal("expected an error for a truncated element body")
	}
}

// A real, well-formed array must still decode - the bound is only allowed to reject input that
// could not possibly be valid.
func TestDecodeAcceptsWellFormedArrayAndMap(t *testing.T) {
	reg := schema.BuiltinRegistry()

	arrType := schema.Array{Element: schema.Prim(schema.PhysicalInt32)}
	arr := ArrayValue{Elements: []Value{
		Int32Value{V: 1}, Int32Value{V: 2}, Int32Value{V: 3},
	}}
	encoded, err := EncodeBytes(arr, arrType, reg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBytes(encoded, arrType, reg)
	if err != nil {
		t.Fatalf("well-formed array was rejected: %v", err)
	}
	if got := len(decoded.(ArrayValue).Elements); got != 3 {
		t.Fatalf("array decoded %d elements, want 3", got)
	}

	mapType := schema.Map{
		Key:   schema.Prim(schema.PhysicalString),
		Value: schema.Prim(schema.PhysicalInt32),
	}
	m := MapValue{Entries: []MapEntry{
		{Key: StringValue{V: "a"}, Val: Int32Value{V: 1}},
		{Key: StringValue{V: "b"}, Val: Int32Value{V: 2}},
	}}
	encodedMap, err := EncodeBytes(m, mapType, reg)
	if err != nil {
		t.Fatal(err)
	}
	decodedMap, err := DecodeBytes(encodedMap, mapType, reg)
	if err != nil {
		t.Fatalf("well-formed map was rejected: %v", err)
	}
	if got := len(decodedMap.(MapValue).Entries); got != 2 {
		t.Fatalf("map decoded %d entries, want 2", got)
	}
}

// An array whose length is legitimately large but whose elements are each several bytes must
// still decode; a bound of "count <= remaining bytes" is conservative, never tight, so this is
// the case that would break if someone tightened it to bytes-per-element.
func TestDecodeAcceptsLargeWellFormedArray(t *testing.T) {
	reg := schema.BuiltinRegistry()
	typ := schema.Array{Element: schema.Prim(schema.PhysicalInt64)}
	elements := make([]Value, 2000)
	for i := range elements {
		elements[i] = Int64Value{V: int64(i)}
	}
	encoded, err := EncodeBytes(ArrayValue{Elements: elements}, typ, reg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeBytes(encoded, typ, reg)
	if err != nil {
		t.Fatalf("2000-element array was rejected: %v", err)
	}
	if got := len(decoded.(ArrayValue).Elements); got != 2000 {
		t.Fatalf("decoded %d elements, want 2000", got)
	}
}

func TestDecodeRejectsTruncatedVarint(t *testing.T) {
	typ := schema.Array{Element: schema.Prim(schema.PhysicalInt32)}
	// Continuation bit set on every byte, then the input ends.
	if _, err := DecodeBytes([]byte{0xff, 0xff, 0xff}, typ, schema.BuiltinRegistry()); err == nil {
		t.Fatal("expected an error for a varint that runs off the end of the input")
	}
}

func TestDecodeRejectsVarintOverflow(t *testing.T) {
	typ := schema.Array{Element: schema.Prim(schema.PhysicalInt32)}
	// Twelve continuation bytes: past the 64-bit shift limit the reader gives up on.
	overflow := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01}
	if _, err := DecodeBytes(overflow, typ, schema.BuiltinRegistry()); err == nil {
		t.Fatal("expected an error for a varint wider than 64 bits")
	}
}

func TestDecodeRejectsEmptyInput(t *testing.T) {
	for _, typ := range []schema.Type{
		schema.Array{Element: schema.Prim(schema.PhysicalInt32)},
		schema.Map{Key: schema.Prim(schema.PhysicalString), Value: schema.Prim(schema.PhysicalInt32)},
		schema.Prim(schema.PhysicalInt64),
	} {
		if _, err := DecodeBytes(nil, typ, schema.BuiltinRegistry()); err == nil {
			t.Errorf("%T accepted empty input", typ)
		}
	}
}
