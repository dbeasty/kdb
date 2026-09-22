package script

import (
	"fmt"

	"github.com/dop251/goja"
)

// The kdb object a ModeRead procedure sees.
//
// It is deliberately smaller than Kotlin's, which offers get, put, delete, query, log and
// callProc. A conflict resolver decides; it does not write. The write is the caller's - an
// ordinary commit, made under the caller's own permissions, which replicates like any other and
// which every node can see and audit. A procedure that could write would also be able to make
// changes that no merge or conflict record explains.

// logSink collects kdb.log output up to a byte budget.
type logSink struct {
	lines []string
	used  int
	max   int
}

func (l *logSink) add(s string) {
	if l.used >= l.max {
		return
	}
	if len(s) > l.max-l.used {
		s = s[:l.max-l.used]
	}
	l.used += len(s)
	l.lines = append(l.lines, s)
}

// installHost puts the kdb object in the VM for ModeRead.
func installHost(vm *goja.Runtime, host Host, logs *logSink, maxCalls int) error {
	if host == nil {
		return fmt.Errorf("%w: ModeRead needs a Host", ErrDenied)
	}
	calls := 0
	budget := func() error {
		calls++
		if calls > maxCalls {
			return fmt.Errorf("%w: over the %d host-call limit", ErrDenied, maxCalls)
		}
		return nil
	}
	obj := vm.NewObject()
	set := func(name string, fn func(goja.FunctionCall) goja.Value) {
		if err := obj.Set(name, fn); err != nil {
			panic(err) // a fresh object always accepts a property
		}
	}
	set("get", func(c goja.FunctionCall) goja.Value {
		if err := budget(); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		body, err := host.Get(c.Argument(0).String())
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		if body == "" {
			return goja.Null()
		}
		return parseJSON(vm, body)
	})
	set("query", func(c goja.FunctionCall) goja.Value {
		if err := budget(); err != nil {
			panic(vm.ToValue(err.Error()))
		}
		var params []any
		if len(c.Arguments) > 1 && !goja.IsUndefined(c.Argument(1)) && !goja.IsNull(c.Argument(1)) {
			if arr, ok := c.Argument(1).Export().([]any); ok {
				params = arr
			}
		}
		rows, err := host.Query(c.Argument(0).String(), params)
		if err != nil {
			panic(vm.ToValue(err.Error()))
		}
		return parseJSON(vm, rows)
	})
	set("log", func(c goja.FunctionCall) goja.Value {
		logs.add(c.Argument(0).String())
		return goja.Undefined()
	})
	// A ModePure procedure has no kdb object at all; these exist so that a procedure written for
	// the authority and then used in a merge fails loudly instead of quietly behaving differently.
	for _, denied := range []string{"put", "delete", "callProc"} {
		name := denied
		set(name, func(goja.FunctionCall) goja.Value {
			panic(vm.ToValue(fmt.Sprintf("kdb: kdb.%s is not available to a conflict procedure", name)))
		})
	}
	return vm.Set("kdb", obj)
}

// parseJSON turns host JSON into a JavaScript value using the VM's own JSON.parse, so numbers and
// strings arrive exactly as a procedure's own JSON.parse would produce them.
func parseJSON(vm *goja.Runtime, text string) goja.Value {
	v, err := vm.RunString("(" + "function(t){return JSON.parse(t);})")
	if err != nil {
		panic(vm.ToValue(err.Error()))
	}
	fn, ok := goja.AssertFunction(v)
	if !ok {
		panic(vm.ToValue("kdb: JSON.parse is unavailable"))
	}
	out, err := fn(goja.Undefined(), vm.ToValue(text))
	if err != nil {
		panic(vm.ToValue(err.Error()))
	}
	return out
}
