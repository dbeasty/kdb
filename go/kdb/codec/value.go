package codec

import (
	"io"

	"github.com/limidus/kdb/go/kdb/codec/schema"
)

// Value is the sum type for all in-memory typed values.
type Value interface {
	isValue()
}

type NullValue struct{}
func (NullValue) isValue() {}

type BoolValue struct{ V bool }
func (BoolValue) isValue() {}

type Int32Value struct{ V int32 }
func (Int32Value) isValue() {}

type Int64Value struct{ V int64 }
func (Int64Value) isValue() {}

type Float64Value struct{ V float64 }
func (Float64Value) isValue() {}

type StringValue struct{ V string }
func (StringValue) isValue() {}

type BytesValue struct{ V []byte }
func (BytesValue) isValue() {}

type ArrayValue struct{ Elements []Value }
func (ArrayValue) isValue() {}

type MapValue struct{ Entries []MapEntry }
func (MapValue) isValue() {}

type MapEntry struct{ Key, Val Value }

type RecordValue struct{ Fields map[int]Value }
func (RecordValue) isValue() {}

type EnumValue struct{ Ordinal int; Symbol string }
func (EnumValue) isValue() {}

type UnionValue struct{ Branch int; Inner Value }
func (UnionValue) isValue() {}

type FixedValue struct{ V []byte }
func (FixedValue) isValue() {}

type DateValue struct{ DaysSinceEpoch int32 }
func (DateValue) isValue() {}

type TimestampValue struct{ EpochMicros int64; TZ *string }
func (TimestampValue) isValue() {}

type UUIDValue struct{ MSB, LSB int64 }
func (UUIDValue) isValue() {}

var Null = NullValue{}

func EncodeBytes(v Value, typ schema.Type, reg *schema.Registry) ([]byte, error) {
	return wireEncode(v, typ, reg)
}

func DecodeBytes(b []byte, typ schema.Type, reg *schema.Registry) (Value, error) {
	return wireDecode(b, typ, reg)
}

func DecodeFrom(r io.Reader, typ schema.Type, reg *schema.Registry) (Value, error) {
	all, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	v, _, err := wireDecodeFirst(all, typ, reg)
	return v, err
}

func EncodedSize(v Value, typ schema.Type, reg *schema.Registry) (int, error) {
	b, err := EncodeBytes(v, typ, reg)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func Roundtrip(v Value, typ schema.Type, reg *schema.Registry) (Value, error) {
	b, err := EncodeBytes(v, typ, reg)
	if err != nil {
		return nil, err
	}
	return DecodeBytes(b, typ, reg)
}
