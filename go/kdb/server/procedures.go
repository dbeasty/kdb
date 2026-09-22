package server

import (
	"sort"
	"sync/atomic"

	"github.com/limidus/kdb/go/kdb/script"
)

// Stored procedures a namespace can resolve conflicts with (Component 32, spec Layer 11).
//
// A procedure is a replicated definition, like a schema or an index: one document in the metadata
// namespace, so every node that serves the namespace has the same source. That matters more here
// than for other definitions. A procedure consulted inside a merge decides part of the merge's
// content, and the merge's hash covers its content, so two nodes running different revisions of
// the same procedure would build different merge commits. They do not: a chain that calls a
// procedure pins the source hash it was set against, and a node whose copy does not match stops
// merging that namespace until it does (see peersync.ResolutionChain).
//
// procRuntime is process-wide so that its compiled-program cache is shared: a merge with many
// conflicting documents calls the same procedure once per document, and every namespace this
// process serves draws on the same cache.
var procRuntime = script.New(script.Limits{})

// ProcedureRuntime is the runtime this process runs stored procedures on.
func ProcedureRuntime() *script.Runtime { return procRuntime }

// procedures is a namespace's procedure sources by name. Replaced whole rather than mutated, so
// a merge reading it never sees a half-applied definition change.
type procedures map[string]string

func (s *KdbServerRuntime) procedureTable() procedures {
	if p := s.procs.Load(); p != nil {
		return *p
	}
	return nil
}

// SetProcedure defines or replaces a procedure on this runtime, refusing one that does not
// compile. Called by the metadata store as the definition arrives or changes.
func (s *KdbServerRuntime) SetProcedure(name, source string) error {
	if err := procRuntime.Compile(source); err != nil {
		return err
	}
	s.storeProcedures(func(next procedures) { next[name] = source })
	return nil
}

// RemoveProcedure drops a procedure from this runtime.
func (s *KdbServerRuntime) RemoveProcedure(name string) {
	s.storeProcedures(func(next procedures) { delete(next, name) })
}

func (s *KdbServerRuntime) storeProcedures(edit func(procedures)) {
	for {
		cur := s.procs.Load()
		next := procedures{}
		if cur != nil {
			for k, v := range *cur {
				next[k] = v
			}
		}
		edit(next)
		if s.procs.CompareAndSwap(cur, &next) {
			return
		}
	}
}

// ProcedureSource returns a procedure's source.
func (s *KdbServerRuntime) ProcedureSource(name string) (string, bool) {
	src, ok := s.procedureTable()[name]
	return src, ok
}

// ProcedureHash is the source hash of the named procedure, or "" when this node does not hold it.
// It is what a resolution chain pins, and what peers compare before they merge.
func (s *KdbServerRuntime) ProcedureHash(name string) string {
	src, ok := s.ProcedureSource(name)
	if !ok {
		return ""
	}
	return script.SourceHash(src)
}

// ProcedureNames lists this namespace's procedures.
func (s *KdbServerRuntime) ProcedureNames() []string {
	t := s.procedureTable()
	names := make([]string, 0, len(t))
	for n := range t {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// procsField is the type of KdbServerRuntime.procs; see the struct.
type procsField = atomic.Pointer[procedures]
