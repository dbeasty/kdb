package index

import (
	"github.com/limidus/kdb/go/kdb/codec"
)

// Key is a typed index key (sealed union).
type Key interface {
	isKey()
}

type NullKey struct{}

func (NullKey) isKey() {}

type BoolKey struct{ Value bool }

func (BoolKey) isKey() {}

type Int32Key struct{ Value int32 }

func (Int32Key) isKey() {}

type Int64Key struct{ Value int64 }

func (Int64Key) isKey() {}

type Float64Key struct{ Value float64 }

func (Float64Key) isKey() {}

type TimestampKey struct{ EpochMillis int64 }

func (TimestampKey) isKey() {}

type StringKey struct{ Value string }

func (StringKey) isKey() {}

type UUIDKey struct{ ID codec.UUID }

func (UUIDKey) isKey() {}

type VectorKey struct {
	embedding []float32
}

func NewVectorKey(v []float32) VectorKey {
	cp := make([]float32, len(v))
	copy(cp, v)
	return VectorKey{embedding: cp}
}

func (v VectorKey) AsFloat32() []float32 {
	cp := make([]float32, len(v.embedding))
	copy(cp, v.embedding)
	return cp
}

func (VectorKey) isKey() {}

type CompositeKey struct{ Parts []Key }

func (CompositeKey) isKey() {}

// CompareKeys provides lexicographic ordering for in-memory btree replay.
func CompareKeys(a, b Key) int {
	ta, tb := keyTag(a), keyTag(b)
	if ta != tb {
		return ta - tb
	}
	switch ka := a.(type) {
	case NullKey:
		return 0
	case BoolKey:
		kb := b.(BoolKey)
		if ka.Value == kb.Value {
			return 0
		}
		if ka.Value {
			return 1
		}
		return -1
	case Int32Key:
		kb := b.(Int32Key)
		if ka.Value < kb.Value {
			return -1
		}
		if ka.Value > kb.Value {
			return 1
		}
		return 0
	case Int64Key:
		kb := b.(Int64Key)
		if ka.Value < kb.Value {
			return -1
		}
		if ka.Value > kb.Value {
			return 1
		}
		return 0
	case Float64Key:
		kb := b.(Float64Key)
		if ka.Value < kb.Value {
			return -1
		}
		if ka.Value > kb.Value {
			return 1
		}
		return 0
	case TimestampKey:
		kb := b.(TimestampKey)
		if ka.EpochMillis < kb.EpochMillis {
			return -1
		}
		if ka.EpochMillis > kb.EpochMillis {
			return 1
		}
		return 0
	case StringKey:
		kb := b.(StringKey)
		if ka.Value < kb.Value {
			return -1
		}
		if ka.Value > kb.Value {
			return 1
		}
		return 0
	case UUIDKey:
		kb := b.(UUIDKey)
		as := ka.ID.String()
		bs := kb.ID.String()
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	case CompositeKey:
		kb := b.(CompositeKey)
		max := len(ka.Parts)
		if len(kb.Parts) < max {
			max = len(kb.Parts)
		}
		for i := 0; i < max; i++ {
			if c := CompareKeys(ka.Parts[i], kb.Parts[i]); c != 0 {
				return c
			}
		}
		if len(ka.Parts) < len(kb.Parts) {
			return -1
		}
		if len(ka.Parts) > len(kb.Parts) {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func keyTag(k Key) int {
	switch k.(type) {
	case NullKey:
		return 0
	case BoolKey:
		return 1
	case Int32Key:
		return 2
	case Int64Key:
		return 3
	case Float64Key:
		return 4
	case TimestampKey:
		return 5
	case StringKey:
		return 6
	case UUIDKey:
		return 7
	case VectorKey:
		return 8
	case CompositeKey:
		return 9
	default:
		return -1
	}
}
