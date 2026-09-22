package script

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// Mode is how much of the outside world a call may see.
type Mode int

const (
	// ModePure is for a procedure whose result becomes part of a merge commit: no clock, no
	// randomness, no database, nothing but its arguments. See sandbox.go.
	ModePure Mode = iota
	// ModeRead is for a procedure that runs after a merge on one node: it may read the database
	// through its Host, but still may not write. Writes are the caller's to make, as an ordinary
	// commit that replicates like any other.
	ModeRead
)

// Host is the database a ModeRead procedure may read. ModePure calls never touch it.
type Host interface {
	// Get returns a document's JSON, or "" when it does not exist.
	Get(docID string) (string, error)
	// Query runs a read-only SQL statement and returns its rows as a JSON array.
	Query(sql string, params []any) (string, error)
}

// Result is what a call produced.
type Result struct {
	// Value is the JSON text of main's return value.
	Value string
	// Logs is what the procedure passed to kdb.log, in order.
	Logs []string
}

// Runtime compiles and runs procedures. It is safe for concurrent use.
//
// Compiled programs are cached by source hash, because a merge with many conflicting documents
// calls the same procedure once per document. Each call still gets a brand-new goja.Runtime: a
// procedure that stashed state in a global would otherwise see the previous document's leftovers,
// and then its answer would depend on how many conflicts happened to precede it - which is
// exactly the kind of order dependence that makes two nodes build different merges.
type Runtime struct {
	limits   Limits
	mu       sync.Mutex
	programs map[string]*goja.Program
}

// New returns a Runtime applying limits, with DefaultLimits for anything left zero.
func New(limits Limits) *Runtime {
	return &Runtime{limits: limits.withDefaults(), programs: map[string]*goja.Program{}}
}

// SourceHash identifies a procedure's source. It is what a resolution chain pins, so that a node
// holding a different revision of a procedure can tell before it merges.
func SourceHash(source string) string {
	sum := sha256.Sum256([]byte(source))
	return hex.EncodeToString(sum[:])
}

// Compile checks that source is a procedure this runtime can run. It is called when a procedure
// is defined, so that a broken one is refused then rather than in the middle of a merge.
func (r *Runtime) Compile(source string) error {
	_, err := r.program(source)
	return err
}

func (r *Runtime) program(source string) (*goja.Program, error) {
	key := SourceHash(source)
	r.mu.Lock()
	p, ok := r.programs[key]
	r.mu.Unlock()
	if ok {
		return p, nil
	}
	p, err := goja.Compile("kdb:procedure/"+key[:12], source, true)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCompile, err)
	}
	r.mu.Lock()
	r.programs[key] = p
	r.mu.Unlock()
	return p, nil
}

// interruptToken marks an interrupt this package raised, so a timeout is reported as one rather
// than as a procedure error.
type interruptToken struct{ reason error }

// Call runs source's main(args) with argsJSON as its argument and returns the JSON of what it
// returned. host is consulted only in ModeRead, where it must not be nil.
func (r *Runtime) Call(ctx context.Context, source string, mode Mode, host Host, argsJSON string) (Result, error) {
	prog, err := r.program(source)
	if err != nil {
		return Result{}, err
	}
	vm := goja.New()
	logs := &logSink{max: r.limits.MaxLogBytes}
	if mode == ModePure {
		if _, err := vm.RunProgram(purePreludeProgram); err != nil {
			return Result{}, fmt.Errorf("%w: installing the deterministic sandbox: %v", ErrRuntime, err)
		}
	} else if err := installHost(vm, host, logs, r.limits.MaxHostCalls); err != nil {
		return Result{}, err
	}
	if _, err := vm.RunProgram(invokePreludeProgram); err != nil {
		return Result{}, fmt.Errorf("%w: installing the call bridge: %v", ErrRuntime, err)
	}
	// Captured before the procedure's own source runs, so redefining __kdb_call does nothing.
	call, ok := goja.AssertFunction(vm.Get("__kdb_call"))
	if !ok {
		return Result{}, fmt.Errorf("%w: the call bridge is missing", ErrRuntime)
	}

	done := make(chan struct{})
	defer close(done)
	timer := time.NewTimer(r.limits.WallClock)
	defer timer.Stop()
	go func() {
		select {
		case <-done:
		case <-timer.C:
			vm.Interrupt(interruptToken{reason: ErrTimeout})
		case <-ctx.Done():
			vm.Interrupt(interruptToken{reason: ctx.Err()})
		}
	}()

	if _, err := vm.RunProgram(prog); err != nil {
		return Result{}, callError(err, "running the procedure's source")
	}
	out, err := call(goja.Undefined(), vm.ToValue(argsJSON))
	if err != nil {
		return Result{}, callError(err, "calling main(args)")
	}
	text := out.String()
	if len(text) > r.limits.MaxOutputBytes {
		return Result{}, fmt.Errorf("%w: returned %d bytes, over the %d-byte limit", ErrOutput, len(text), r.limits.MaxOutputBytes)
	}
	return Result{Value: text, Logs: logs.lines}, nil
}

// callError turns a goja failure into one of this package's errors. An interrupt this package
// raised keeps its own reason; a procedure's own exception is ErrRuntime.
func callError(err error, during string) error {
	var ie *goja.InterruptedError
	if errors.As(err, &ie) {
		if tok, ok := ie.Value().(interruptToken); ok {
			return fmt.Errorf("%w: while %s", tok.reason, during)
		}
	}
	return fmt.Errorf("%w: while %s: %v", ErrRuntime, during, err)
}
