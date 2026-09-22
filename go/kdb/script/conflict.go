package script

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	kdbjson "github.com/limidus/kdb/go/kdb/json"
)

// The conflict-resolution calling convention, shared with Kotlin's kdb-script.
//
// A procedure is handed one conflicting document: both sides in full, the value they last agreed
// on, who wrote each side, and the list of fields that actually differ. It returns one of five
// decisions. Working out the field list here rather than in each procedure keeps the answer the
// same on every node, which an in-merge decision needs (see kdb/json/diff.go).
//
// "local" and "remote" name the two sides, but which is which depends on where the call happens.
// In a merge they are canonical sides - side 0 is the merge's first parent's, the lower head
// hash - so that both nodes merging the same heads hand the procedure the same two sides in the
// same order. A procedure that must not care can say side0/side1 instead, and one that wants to
// know who wrote a side reads origins.

// Origin describes the commit that produced one side of a conflict.
type Origin struct {
	NodeID          string `json:"nodeId"`
	Commit          string `json:"commit"`
	TimestampMicros int64  `json:"timestampMicros"`
}

// ConflictInput is one conflicting document as the caller knows it. A nil body means the side
// deleted the document (or, for Base, that it did not exist when the sides parted).
type ConflictInput struct {
	DocID        string
	Base         *string
	Local        *string
	Remote       *string
	LocalOrigin  Origin
	RemoteOrigin Origin
}

// DecisionKind is what a procedure decided.
type DecisionKind int

const (
	// DecideDefer: this procedure has no opinion; the caller moves on to whatever is next.
	DecideDefer DecisionKind = iota
	// DecideTake: keep one side whole, discarding the other.
	DecideTake
	// DecideDoc: store a document the procedure built.
	DecideDoc
	// DecideFork: keep one side at this document's id and write the other to a new document.
	DecideFork
	// DecideDelete: the document should not survive the merge.
	DecideDelete
)

// Decision is a parsed, validated procedure result.
type Decision struct {
	Kind DecisionKind
	// Side is the side kept, 0 or 1, for DecideTake and DecideFork.
	Side int
	// Doc is the document body for DecideDoc, canonical.
	Doc string
}

// jsField is one entry of the fields array handed to a procedure.
type jsField struct {
	Path   string          `json:"path"`
	Kind   string          `json:"kind"`
	Base   json.RawMessage `json:"base,omitempty"`
	Local  json.RawMessage `json:"local,omitempty"`
	Remote json.RawMessage `json:"remote,omitempty"`
	// Present says whether each side has the field at all, which "base": null cannot express.
	Present map[string]bool `json:"present"`
}

// jsConflict is the whole argument object.
type jsConflict struct {
	DocID   string          `json:"docId"`
	Base    json.RawMessage `json:"base"`
	Local   json.RawMessage `json:"local"`
	Remote  json.RawMessage `json:"remote"`
	Side0   json.RawMessage `json:"side0"`
	Side1   json.RawMessage `json:"side1"`
	Present map[string]bool `json:"present"`
	Origins struct {
		Local  Origin `json:"local"`
		Remote Origin `json:"remote"`
		Side0  Origin `json:"side0"`
		Side1  Origin `json:"side1"`
	} `json:"origins"`
	Fields []jsField `json:"fields"`
}

// ConflictArgs renders in as the JSON a procedure receives.
func ConflictArgs(in ConflictInput) (string, error) {
	parse := func(body *string, side string) (kdbjson.Value, error) {
		if body == nil {
			return nil, nil
		}
		v, err := kdbjson.ParseValue(*body)
		if err != nil {
			return nil, fmt.Errorf("%w: the %s side of %s is not valid JSON: %v", ErrOutput, side, in.DocID, err)
		}
		return v, nil
	}
	base, err := parse(in.Base, "base")
	if err != nil {
		return "", err
	}
	local, err := parse(in.Local, "local")
	if err != nil {
		return "", err
	}
	remote, err := parse(in.Remote, "remote")
	if err != nil {
		return "", err
	}
	raw := func(v kdbjson.Value) json.RawMessage {
		if v == nil {
			return json.RawMessage("null")
		}
		return json.RawMessage(kdbjson.ToJSONString(v))
	}
	c := jsConflict{
		DocID:  in.DocID,
		Base:   raw(base),
		Local:  raw(local),
		Remote: raw(remote),
		Side0:  raw(local),
		Side1:  raw(remote),
		Present: map[string]bool{
			"base": base != nil, "local": local != nil, "remote": remote != nil,
			"side0": local != nil, "side1": remote != nil,
		},
	}
	c.Origins.Local, c.Origins.Side0 = in.LocalOrigin, in.LocalOrigin
	c.Origins.Remote, c.Origins.Side1 = in.RemoteOrigin, in.RemoteOrigin
	for _, f := range kdbjson.DiffConflict(base, local, remote) {
		e := jsField{
			Path: f.Path, Kind: f.Kind,
			Present: map[string]bool{
				"base": f.BasePresent, "local": f.LocalPresent, "remote": f.RemotePresent,
				"side0": f.LocalPresent, "side1": f.RemotePresent,
			},
		}
		if f.BasePresent {
			e.Base = raw(f.Base)
		}
		if f.LocalPresent {
			e.Local = raw(f.Local)
		}
		if f.RemotePresent {
			e.Remote = raw(f.Remote)
		}
		c.Fields = append(c.Fields, e)
	}
	if c.Fields == nil {
		c.Fields = []jsField{}
	}
	out, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// rawDecision is a procedure's result before it is checked. Every member is optional so that
// "which one did it choose" can be answered by counting, rather than by guessing at a default.
type rawDecision struct {
	Take *string         `json:"take"`
	Doc  json.RawMessage `json:"doc"`
	Fork *struct {
		Keep string `json:"keep"`
	} `json:"fork"`
	Delete *bool `json:"delete"`
	Defer  *bool `json:"defer"`
}

// ParseDecision validates a procedure's returned JSON.
//
// It is strict on purpose: a result that could be read two ways is a result two nodes might read
// differently. Exactly one decision must be present, a side must be named, and a returned
// document must be a JSON object.
func ParseDecision(text string) (Decision, error) {
	var raw rawDecision
	dec := json.NewDecoder(strings.NewReader(text))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrOutput, err)
	}
	set := 0
	for _, present := range []bool{raw.Take != nil, raw.Doc != nil, raw.Fork != nil, raw.Delete != nil, raw.Defer != nil} {
		if present {
			set++
		}
	}
	if set != 1 {
		return Decision{}, fmt.Errorf("%w: name exactly one of take, doc, fork, delete or defer (found %d)", ErrOutput, set)
	}
	switch {
	case raw.Take != nil:
		side, err := parseSide(*raw.Take)
		if err != nil {
			return Decision{}, err
		}
		return Decision{Kind: DecideTake, Side: side}, nil
	case raw.Doc != nil:
		v, err := kdbjson.ParseValue(string(raw.Doc))
		if err != nil {
			return Decision{}, fmt.Errorf("%w: doc is not valid JSON: %v", ErrOutput, err)
		}
		if _, ok := v.(kdbjson.ObjectValue); !ok {
			return Decision{}, fmt.Errorf("%w: doc must be an object, not %T", ErrOutput, v)
		}
		return Decision{Kind: DecideDoc, Doc: kdbjson.ToJSONString(v)}, nil
	case raw.Fork != nil:
		side, err := parseSide(raw.Fork.Keep)
		if err != nil {
			return Decision{}, err
		}
		return Decision{Kind: DecideFork, Side: side}, nil
	case raw.Delete != nil:
		if !*raw.Delete {
			return Decision{}, fmt.Errorf("%w: delete must be true; to decide nothing, return {defer:true}", ErrOutput)
		}
		return Decision{Kind: DecideDelete}, nil
	default:
		if !*raw.Defer {
			return Decision{}, fmt.Errorf("%w: defer must be true; to decide nothing, return {defer:true}", ErrOutput)
		}
		return Decision{Kind: DecideDefer}, nil
	}
}

func parseSide(s string) (int, error) {
	switch s {
	case "local", "side0":
		return 0, nil
	case "remote", "side1":
		return 1, nil
	default:
		return 0, fmt.Errorf("%w: %q is not a side; use local/remote or side0/side1", ErrOutput, s)
	}
}

// ResolveConflict runs source over one conflict and returns its decision.
func (r *Runtime) ResolveConflict(ctx context.Context, source string, mode Mode, host Host, in ConflictInput) (Decision, error) {
	args, err := ConflictArgs(in)
	if err != nil {
		return Decision{}, err
	}
	res, err := r.Call(ctx, source, mode, host, args)
	if err != nil {
		return Decision{}, err
	}
	return ParseDecision(res.Value)
}
