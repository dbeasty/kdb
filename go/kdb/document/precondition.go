package document

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
)

// PreconditionKind is the assertion a write makes about a document's state at the instant the
// transaction lands.
type PreconditionKind int

const (
	// ExpectAny asserts nothing. The zero value, so an operation with no declared precondition
	// behaves exactly as it did before preconditions existed.
	ExpectAny PreconditionKind = iota
	// ExpectAbsent requires that no document exist at the id. This is insert-if-not-exists.
	ExpectAbsent
	// ExpectPresent requires that some document exist at the id, whatever its content.
	ExpectPresent
	// ExpectContentHash requires that the document exist with exactly this content hash. This is
	// compare-and-set.
	ExpectContentHash
)

func (k PreconditionKind) String() string {
	switch k {
	case ExpectAny:
		return "EXPECT_ANY"
	case ExpectAbsent:
		return "EXPECT_ABSENT"
	case ExpectPresent:
		return "EXPECT_PRESENT"
	case ExpectContentHash:
		return "EXPECT_CONTENT_HASH"
	default:
		return "UNKNOWN"
	}
}

// Precondition is one operation's assertion about the state it is writing over.
//
// Preconditions are carried on the Transaction envelope rather than inside the Op, deliberately:
// an Op is part of committed history and is hashed into the commit, but a precondition is a
// request-time assertion about a state that no longer exists once the commit lands. Storing one
// in the Op would change every op's canonical encoding - and therefore commit hashes - to record
// something that is not a fact about the data.
type Precondition struct {
	// OpIndex is the index into Transaction.Operations this precondition guards.
	OpIndex int
	Kind    PreconditionKind
	// ContentHash is the required document content hash; meaningful only for ExpectContentHash.
	ContentHash codec.Hash
}

func (p Precondition) String() string {
	if p.Kind == ExpectContentHash {
		return fmt.Sprintf("op[%d] %s(%s)", p.OpIndex, p.Kind, p.ContentHash.Hex())
	}
	return fmt.Sprintf("op[%d] %s", p.OpIndex, p.Kind)
}

// PreconditionsByOpIndex indexes preconditions for lookup during evaluation. A duplicate index
// is an error rather than a last-wins overwrite: two contradictory assertions about the same
// operation have no defensible resolution, and silently honoring one of them would make a
// client's compare-and-set succeed for a reason it never asked for.
func PreconditionsByOpIndex(preconditions []Precondition, opCount int) (map[int]Precondition, error) {
	if len(preconditions) == 0 {
		return nil, nil
	}
	out := make(map[int]Precondition, len(preconditions))
	for _, p := range preconditions {
		if p.OpIndex < 0 || p.OpIndex >= opCount {
			return nil, fmt.Errorf("kdb precondition: op index %d out of range for %d operations", p.OpIndex, opCount)
		}
		if _, dup := out[p.OpIndex]; dup {
			return nil, fmt.Errorf("kdb precondition: duplicate precondition for op index %d", p.OpIndex)
		}
		out[p.OpIndex] = p
	}
	return out, nil
}
