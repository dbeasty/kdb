package index

import (
	"bytes"
	"errors"
	"slices"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Composite key groups: a composite BTREE read like a Cassandra partition - every key that starts
// with an equality prefix, in the order of the field after it, forwards or backwards - so an
// `ORDER BY <last field> LIMIT k` over `WHERE <prefix fields> = ...` reads about k documents. Same
// exactness rule as KeyGroups: served only from a state with nothing to undo.

// compositePrefix is the encoding every key under prefix starts with: the composite tag and each
// prefix part, then the separator of the next part.
func compositePrefix(prefix []Key) []byte {
	enc := appendKeyEnc(nil, CompositeKey{Parts: prefix})
	enc = enc[:len(enc)-1] // drop the composite's terminator
	return append(enc, 0x01)
}

func (s *MemoryStore) requireComposite(prefixParts int) error {
	if err := s.requireKeyedType(); err != nil {
		return err
	}
	if s.descriptor.Type != IndexTypeBTree || len(s.descriptor.FieldPaths()) != prefixParts+1 {
		return errors.New("index: not a composite BTREE with that many prefix fields")
	}
	return nil
}

// CompositeGroups returns the keys under prefix (every field but the last), in the last field's
// order - ascending, or descending - that come strictly after after in that direction (from the
// first when after is nil), each with the last field's value as its Key and its documents in UUID
// order. Whole keys until at least maxIDs ids are listed; an empty result means none remain.
func (s *MemoryStore) CompositeGroups(prefix []Key, after Key, ascending bool, atCommit *codec.Hash, maxIDs int) ([]KeyBucket, error) {
	if err := s.requireComposite(len(prefix)); err != nil {
		return nil, err
	}
	if maxIDs <= 0 {
		maxIDs = 1
	}
	pre := compositePrefix(prefix)
	l := s.log
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.exactViewLocked(atCommit)
	if err != nil {
		return nil, err
	}
	order := l.keys.order()
	var out []KeyBucket
	listed := 0
	emit := func(ord uint32) bool {
		if !bytes.HasPrefix(l.keys.bytes(ord), pre) {
			return false
		}
		if ids := v.st.ids(ord); len(ids) > 0 {
			if k, ok := l.keys.key(ord).(CompositeKey); ok && len(k.Parts) == len(prefix)+1 {
				out = append(out, KeyBucket{Key: k.Parts[len(prefix)], DocIDs: slices.Clone(ids)})
				listed += len(ids)
			}
		}
		return true
	}
	if ascending {
		start := l.keys.lowerBound(order, pre)
		if after != nil {
			start = l.keys.upperBound(order, compositeFull(prefix, after))
		}
		for i := start; i < len(order) && listed < maxIDs; i++ {
			if !emit(order[i]) {
				break
			}
		}
		return out, nil
	}
	// Descending: from the last key under prefix, or from just below after.
	end := l.keys.lowerBound(order, prefixEnd(pre))
	if after != nil {
		end = l.keys.lowerBound(order, compositeFull(prefix, after))
	}
	for i := end - 1; i >= 0 && listed < maxIDs; i-- {
		if !emit(order[i]) {
			break
		}
	}
	return out, nil
}

// compositeFull is the encoding of the composite key prefix + last.
func compositeFull(prefix []Key, last Key) []byte {
	return appendKeyEnc(nil, CompositeKey{Parts: append(slices.Clone(prefix), last)})
}

// prefixEnd is the smallest encoding greater than every one that starts with pre.
func prefixEnd(pre []byte) []byte {
	end := slices.Clone(pre)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// CompositeSortTags reports which key types the last field takes under prefix, by encoding tag:
// a walk compares keys by their bytes, which agrees with SQL's order only while at most one
// numeric type (bool, int, float) is present.
func (s *MemoryStore) CompositeSortTags(prefix []Key, atCommit *codec.Hash) (map[byte]bool, error) {
	if err := s.requireComposite(len(prefix)); err != nil {
		return nil, err
	}
	pre := compositePrefix(prefix)
	l := s.log
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.exactViewLocked(atCommit)
	if err != nil {
		return nil, err
	}
	order := l.keys.order()
	tags := map[byte]bool{}
	for tag := byte(0); tag <= encTagComposite; tag++ {
		probe := append(slices.Clone(pre), tag)
		for i := l.keys.lowerBound(order, probe); i < len(order); i++ {
			if !bytes.HasPrefix(l.keys.bytes(order[i]), probe) {
				break
			}
			if len(v.st.ids(order[i])) > 0 {
				tags[tag] = true
				break
			}
		}
	}
	return tags, nil
}

// CompositeIrregular returns the documents filed under IrregularCompositeKey - those the index
// could not place by value - which a walk must read whatever their prefix.
func (s *MemoryStore) CompositeIrregular(atCommit *codec.Hash) ([]codec.UUID, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	l := s.log
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.exactViewLocked(atCommit)
	if err != nil {
		return nil, err
	}
	ord, ok := l.keys.find(appendKeyEnc(nil, IrregularCompositeKey))
	if !ok {
		return nil, nil
	}
	return slices.Clone(v.st.ids(ord)), nil
}
