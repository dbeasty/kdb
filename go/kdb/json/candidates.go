package json

import "strings"

// Candidate evaluation - kdb-spec-layer16 §2, "path evaluation with implicit array traversal".
//
// The navigator in api.go (Get/GetAll) implements strict JSONPath: a field segment applied to an
// array is an error (and, historically, a panic). SQL column references over documents need the
// Mongo reading instead: walking `$.a.b`, an array met at any segment fans the rest of the path
// out over its elements, and the results are concatenated in document order. Nothing here
// panics - a segment that does not apply to the value it meets simply contributes no candidates,
// which is exactly the "absent path is NULL" rule the query layer wants.
//
// These functions are additive; the strict navigator's semantics are unchanged.

// SplitDotted splits a dotted column path ("a.b.c") into its segments. An empty path yields no
// segments (the root itself). Consecutive or trailing dots produce empty segments, which never
// match a field and so yield no candidates rather than an error.
func SplitDotted(dotted string) []string {
	if dotted == "" {
		return nil
	}
	return strings.Split(dotted, ".")
}

// Candidates parses jsonText and returns every value the dotted path reaches with implicit array
// traversal, expanding a terminal array into its elements (so "tags" over {"tags":["x","y"]}
// yields "x" and "y", making equality a membership test). A path that reaches nothing returns an
// empty list and a nil error; only malformed JSON is an error.
func Candidates(jsonText string, dotted string) ([]Value, error) {
	root, err := ParseValue(jsonText)
	if err != nil {
		return nil, err
	}
	return CandidatesOf(root, SplitDotted(dotted), true), nil
}

// PathValues is Candidates without terminal-array expansion: intermediate arrays are still
// traversed, but a value that is itself an array at the end of the path is returned whole. This
// is the reading projection and ORDER BY want ("SELECT tags" returns the array, not its first
// element) and the one ARRAY_LENGTH needs.
func PathValues(jsonText string, dotted string) ([]Value, error) {
	root, err := ParseValue(jsonText)
	if err != nil {
		return nil, err
	}
	return CandidatesOf(root, SplitDotted(dotted), false), nil
}

// CandidatesOf walks an already-parsed value. With expandFinal, a terminal array contributes its
// elements; without, it is one candidate. Never panics.
func CandidatesOf(root Value, segments []string, expandFinal bool) []Value {
	var out []Value
	collectCandidates(root, segments, expandFinal, &out)
	return out
}

func collectCandidates(cur Value, segments []string, expandFinal bool, out *[]Value) {
	if cur == nil {
		return
	}
	if len(segments) == 0 {
		if arr, ok := cur.(ArrayValue); ok && expandFinal {
			for _, el := range arr.Elements {
				if el != nil {
					*out = append(*out, el)
				}
			}
			return
		}
		*out = append(*out, cur)
		return
	}
	switch v := cur.(type) {
	case ObjectValue:
		next, ok := v.Fields[segments[0]]
		if !ok {
			return
		}
		collectCandidates(next, segments[1:], expandFinal, out)
	case ArrayValue:
		// Implicit traversal: the remaining path applies to every element, in order.
		for _, el := range v.Elements {
			collectCandidates(el, segments, expandFinal, out)
		}
	default:
		// A scalar met before the path is exhausted: nothing to descend into.
	}
}

// DeepEqual reports structural equality of two JSON values: strings and booleans by value,
// numbers numerically (1 == 1.0), arrays element-wise in order, objects key-set and value-wise
// in declaration order. This is the equality ARRAY_CONTAINS uses (kdb-spec-layer16 §4).
func DeepEqual(a, b Value) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return deepEqual(a, b)
}
