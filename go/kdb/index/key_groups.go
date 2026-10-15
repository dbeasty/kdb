package index

import (
	"errors"
	"slices"

	"github.com/limidus/kdb/go/kdb/codec"
)

// Ordered key groups: what lets a query read a BTREE in key order with each key in hand, so an
// ORDER BY ... LIMIT can stop after the first page of documents (the SQL executor's ordered
// top-k). Range returns bare ids, which is enough for a predicate but not for an ORDER BY: the
// executor has to know where one key ends to decide that nothing after it can sort earlier.
//
// These reads are served only from a state that is exact without undo: the head state at the
// head cutoff, or a full replay. A read that would need the history window's undo (a commit
// behind the head) reports ErrKeyGroupsUnsupported, and the caller reads the documents instead.

// ErrKeyGroupsUnsupported reports a key-group read this index cannot answer at that commit.
var ErrKeyGroupsUnsupported = errors.New("index: ordered key groups are not served at that commit")

// KeyGroups returns, in ascending key order, the keys of kind's type (kind's value is ignored)
// that sort strictly after after (from the first key of that type when after is nil), each with
// its documents in UUID order. It returns whole keys until at least maxIDs ids are listed (at
// least one key when any remain); an empty result means none remain.
func (s *MemoryStore) KeyGroups(kind, after Key, atCommit *codec.Hash, maxIDs int) ([]KeyBucket, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	return s.log.keyGroups(kind, after, atCommit, maxIDs)
}

// Unfiled returns the ids, in the order given, that are filed under no key at all at atCommit:
// documents that do not carry the field (or carry no value the index keys).
func (s *MemoryStore) Unfiled(ids []codec.UUID, atCommit *codec.Hash) ([]codec.UUID, error) {
	if err := s.requireKeyedType(); err != nil {
		return nil, err
	}
	return s.log.unfiled(ids, atCommit)
}

// KeysOnlyOfTypes reports whether every key that has documents at atCommit is of one of the
// types of kinds (their values are ignored).
func (s *MemoryStore) KeysOnlyOfTypes(kinds []Key, atCommit *codec.Hash) (bool, error) {
	if err := s.requireKeyedType(); err != nil {
		return false, err
	}
	return s.log.keysOnlyOfTypes(kinds, atCommit)
}

// exactViewLocked is viewLocked for the key-group reads: only a view with nothing to undo.
func (l *eventLog) exactViewLocked(at *codec.Hash) (keyedView, error) {
	v, err := l.viewLocked(at)
	if err != nil {
		return keyedView{}, err
	}
	if len(v.undo) > 0 {
		return keyedView{}, ErrKeyGroupsUnsupported
	}
	return v, nil
}

func keyTypeTag(k Key) byte { return appendKeyEnc(nil, k)[0] }

func (l *eventLog) keyGroups(kind, after Key, at *codec.Hash, maxIDs int) ([]KeyBucket, error) {
	if maxIDs <= 0 {
		maxIDs = 1
	}
	tag := keyTypeTag(kind)
	if after != nil && keyTypeTag(after) != tag {
		return nil, errors.New("index: key group bound of another type")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.exactViewLocked(at)
	if err != nil {
		return nil, err
	}
	order := l.keys.order()
	var start int
	if after == nil {
		start = l.keys.lowerBound(order, []byte{tag})
	} else {
		start = l.keys.upperBound(order, appendKeyEnc(nil, after))
	}
	var out []KeyBucket
	listed := 0
	for i := start; i < len(order) && listed < maxIDs; i++ {
		ord := order[i]
		if b := l.keys.bytes(ord); len(b) == 0 || b[0] != tag {
			break
		}
		ids := v.st.ids(ord)
		if len(ids) == 0 {
			continue
		}
		out = append(out, KeyBucket{Key: l.keys.key(ord), DocIDs: slices.Clone(ids)})
		listed += len(ids)
	}
	return out, nil
}

func (l *eventLog) unfiled(ids []codec.UUID, at *codec.Hash) ([]codec.UUID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.exactViewLocked(at)
	if err != nil {
		return nil, err
	}
	var out []codec.UUID
	for _, id := range ids {
		if len(v.st.docKeys(id)) == 0 {
			out = append(out, id)
		}
	}
	return out, nil
}

func (l *eventLog) keysOnlyOfTypes(kinds []Key, at *codec.Hash) (bool, error) {
	allowed := map[byte]bool{}
	for _, k := range kinds {
		allowed[keyTypeTag(k)] = true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	v, err := l.exactViewLocked(at)
	if err != nil {
		return false, err
	}
	order := l.keys.order()
	// Keys of one type are contiguous in key order: check each type's run once, skipping the
	// allowed ones whole.
	for i := 0; i < len(order); {
		b := l.keys.bytes(order[i])
		if len(b) == 0 {
			return false, nil
		}
		tag := b[0]
		end := l.keys.lowerBound(order, []byte{tag + 1})
		if end <= i {
			end = i + 1
		}
		if !allowed[tag] {
			for j := i; j < end; j++ {
				if v.st.count(order[j]) > 0 {
					return false, nil
				}
			}
		}
		i = end
	}
	return true, nil
}
