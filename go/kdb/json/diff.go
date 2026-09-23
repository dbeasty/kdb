package json

import (
	"sort"
	"strings"
)

// Three-way field diff, for telling a conflict resolver which fields actually conflict.
//
// A resolver that is handed two whole documents has to work out itself which fields the two
// sides disagree about, and every resolver would do it slightly differently. Doing it once here
// keeps that answer the same everywhere - and, because an in-merge resolver's result is covered
// by the merge commit's hash, the same on every node.
//
// The walk recurses into objects and compares arrays whole: an array's identity is its order,
// so "element 2 changed" is not a fact two nodes can be relied on to derive alike from two
// arrays of different lengths.

// Change kinds.
const (
	// ChangeLocal: only the local side moved away from the base.
	ChangeLocal = "local"
	// ChangeRemote: only the remote side moved away from the base.
	ChangeRemote = "remote"
	// ChangeBoth: both sides moved away from the base, to different values. This is the set a
	// resolver exists to settle.
	ChangeBoth = "both"
	// ChangeSame: both sides moved away from the base and landed on the same value. It is not a
	// conflict, but a resolver is told about it so that a rule like "keep the field the other
	// side also set" can see it.
	ChangeSame = "same"
)

// FieldChange is one field's three values. A field both sides left at the base is not reported.
//
// Absent and null are different: a field deleted on one side has Present false there, while a
// field set to null has Present true and a NullValue. A resolver that conflated them would
// resurrect deleted fields.
type FieldChange struct {
	// Path is an RFC 6901 JSON Pointer from the document root.
	Path                                     string
	Base, Local, Remote                      Value
	BasePresent, LocalPresent, RemotePresent bool
	Kind                                     string
}

// DiffConflict lists the fields on which local and remote differ, measured against base. A nil
// value means the whole document is absent on that side (a delete).
//
// The order is by path, compared as UTF-8 bytes, so every runtime that sorts by code unit or by
// byte - and Kotlin, whose String.compareTo is UTF-16 and so orders some astral characters
// differently - can produce this same list.
func DiffConflict(base, local, remote Value) []FieldChange {
	var out []FieldChange
	walkConflict("", base, local, remote, base != nil, local != nil, remote != nil, &out)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// walkConflict emits the changes at this path, recursing while both sides hold objects.
func walkConflict(path string, base, local, remote Value, bp, lp, rp bool, out *[]FieldChange) {
	if sameValue(local, lp, remote, rp) && sameValue(base, bp, local, lp) {
		return
	}
	lo, lok := local.(ObjectValue)
	ro, rok := remote.(ObjectValue)
	if lp && rp && lok && rok {
		bo, bok := base.(ObjectValue)
		if !bp || bok {
			for _, k := range unionKeys(bo, bok && bp, lo, ro) {
				bv, bkp := field(bo, bok && bp, k)
				lv, lkp := field(lo, true, k)
				rv, rkp := field(ro, true, k)
				walkConflict(path+"/"+escapePointer(k), bv, lv, rv, bkp, lkp, rkp, out)
			}
			return
		}
	}
	*out = append(*out, FieldChange{
		Path: path,
		Base: base, Local: local, Remote: remote,
		BasePresent: bp, LocalPresent: lp, RemotePresent: rp,
		Kind: classify(base, bp, local, lp, remote, rp),
	})
}

func classify(base Value, bp bool, local Value, lp bool, remote Value, rp bool) string {
	lc := !sameValue(base, bp, local, lp)
	rc := !sameValue(base, bp, remote, rp)
	switch {
	case lc && rc && sameValue(local, lp, remote, rp):
		return ChangeSame
	case lc && rc:
		return ChangeBoth
	case lc:
		return ChangeLocal
	default:
		return ChangeRemote
	}
}

// unionKeys lists every key any side has, in a fixed order: the local side's order, then the
// remote side's additions, then the base's. Sorting happens once at the end, over full paths.
func unionKeys(b ObjectValue, bp bool, l, r ObjectValue) []string {
	keys := make([]string, 0, len(l.Keys)+len(r.Keys))
	seen := map[string]bool{}
	add := func(ks []string) {
		for _, k := range ks {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	add(l.Keys)
	add(r.Keys)
	if bp {
		add(b.Keys)
	}
	return keys
}

func field(o ObjectValue, present bool, k string) (Value, bool) {
	if !present {
		return nil, false
	}
	v, ok := o.Fields[k]
	return v, ok
}

// sameValue compares two possibly-absent values. Two absent values are the same; an absent value
// never equals a present one, including a present null.
func sameValue(a Value, ap bool, b Value, bp bool) bool {
	if ap != bp {
		return false
	}
	if !ap {
		return true
	}
	return CanonicalText(a) == CanonicalText(b)
}

// CanonicalText renders a value with every object's keys sorted, for comparison: two documents
// that differ only in the order their fields were written are the same document.
func CanonicalText(v Value) string {
	var b strings.Builder
	appendCanonical(&b, v)
	return b.String()
}

func appendCanonical(b *strings.Builder, v Value) {
	switch t := v.(type) {
	case ObjectValue:
		keys := append([]string{}, t.Keys...)
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			appendString(b, k)
			b.WriteByte(':')
			appendCanonical(b, t.Fields[k])
		}
		b.WriteByte('}')
	case ArrayValue:
		b.WriteByte('[')
		for i, e := range t.Elements {
			if i > 0 {
				b.WriteByte(',')
			}
			appendCanonical(b, e)
		}
		b.WriteByte(']')
	default:
		appendValue(b, v)
	}
}

// escapePointer encodes one JSON Pointer reference token (RFC 6901 §3).
func escapePointer(k string) string {
	return strings.ReplaceAll(strings.ReplaceAll(k, "~", "~0"), "/", "~1")
}
